/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	Resolver: CueBallResolver
};

const mod_dns = require('native-dns');
const mod_events = require('events');
const mod_net = require('net');
const mod_util = require('util');
const mod_mooremachine = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_utils = require('./utils');
const mod_vasync = require('vasync');
const mod_bunyan = require('bunyan');
const mod_bloom = require('bloomfilter');
const mod_ipaddr = require('ipaddr.js');
const mod_crypto = require('crypto');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;
const BloomFilter = mod_bloom.BloomFilter;

function CueBallResolver(options) {
	mod_assert.object(options);
	mod_assert.arrayOfString(options.resolvers, 'options.resolvers');
	mod_assert.string(options.domain, 'options.domain');
	mod_assert.optionalString(options.service, 'options.service');
	mod_assert.optionalNumber(options.maxDNSConcurrency,
	    'options.maxDNSConcurrency');
	mod_assert.optionalNumber(options.defaultPort, 'options.defaultPort');

	this.r_resolvers = options.resolvers;
	this.r_domain = options.domain;
	this.r_service = options.service || '_http._tcp';
	this.r_maxres = options.maxDNSConcurrency;
	this.r_defport = options.defaultPort || 80;

	/*
	 * Use a bloom filter to avoid always trying UDP first on names that
	 * are truncated and only queriable over TCP. This could just be a
	 * hash, but we don't want to spend a lot of memory on potentially
	 * remembering every name we ever look up.
	 */
	this.r_tcpNeeded = new BloomFilter(8 * 1024, 16);

	mod_assert.optionalObject(options.log, 'options.log');
	this.r_log = options.log || mod_bunyan.createLogger({
		name: 'CueBallResolver'
	});
	this.r_log = this.r_log.child({domain: this.r_domain});

	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	mod_assert.optionalNumber(options.delay, 'options.delay');

	this.r_srvRetry = {
		max: 3,
		count: 3,
		timeout: options.timeout || 1000,
		minDelay: options.delay || 100,
		delay: options.delay || 100,
		maxDelay: 1000
	};

	this.r_retry = {
		max: 3,
		count: 3,
		timeout: options.timeout || 1000,
		minDelay: options.delay || 100,
		delay: options.delay || 100,
		maxDelay: 1000
	};

	this.r_nextService = new Date();
	this.r_nextV6 = new Date();
	this.r_nextV4 = new Date();

	this.r_lastError = undefined;

	this.r_srvs = [];
	this.r_srvRem = [];
	this.r_srv = undefined;
	this.r_backends = {};

	this.r_stopping = false;

	FSM.call(this, 'init');
}
mod_util.inherits(CueBallResolver, FSM);

CueBallResolver.prototype.start = function () {
	this.emit('startAsserted');
};

CueBallResolver.prototype.stop = function (cb) {
	this.r_stopping = true;
	this.emit('stopAsserted');
};

CueBallResolver.prototype.state_init = function (on) {
	var self = this;
	this.r_stopping = false;
	on(this, 'startAsserted', function () {
		self.gotoState('srv');
	});
};

CueBallResolver.prototype.state_srv = function () {
	var r = this.r_srvRetry;
	r.delay = r.minDelay;
	r.count = r.max;
	this.gotoState('srv_try');
};

CueBallResolver.prototype.state_srv_try = function (on, once, timeout) {
	var self = this;

	var name = this.r_service + '.' + this.r_domain;
	var req = this.resolve(name, 'SRV');
	once(req, 'answers', function (ans, ttl) {
		var d = new Date();
		d.setTime(d.getTime() + 1000*ttl);
		self.r_nextService = d;

		self.r_srvs = ans;
		self.gotoState('aaaa');
	});
	once(req, 'error', function (err) {
		self.r_lastError = err;

		if (NoRecordsError.isInstance(err)) {
			/*
			 * If we didn't get an error, but found no records for
			 * SRV, then just proceed to do our AAAA/A lookups
			 * directly on the base domain.
			 */
			self.r_srvs = [{
				name: self.r_domain,
				port: self.r_defport
			}];

			/*
			 * Don't bother retrying SRV lookups for at least 60
			 * minutes -- there probably aren't any available.
			 */
			var d = new Date();
			d.setTime(d.getTime() + 1000*60*60);
			self.r_nextService = d;

			self.r_log.trace('no SRV records found for service ' +
			    '%s, treating as a plain name for next 60min',
			    self.r_service);

			self.gotoState('aaaa');
			
		} else {
			self.gotoState('srv_error');
		}
	});
	req.send();
};

CueBallResolver.prototype.state_srv_error = function (on, once, timeout) {
	var self = this;
	var r = self.r_srvRetry;
	if (--r.count > 0) {
		timeout(r.delay, function () {
			self.gotoState('srv_try');
		});

		r.delay *= 2;
		if (r.delay > r.maxDelay)
			r.delay = r.maxDelay;

	} else {
		self.r_log.trace(self.r_lastError,
		    'repeated error during SRV resolution for service %s, ' +
		    'will retry in 5min', self.r_service);

		self.r_srvs = [{
			name: self.r_domain,
			port: self.r_defport
		}];

		/*
		 * Retry in 5 mins, but proceed on through -- just in case
		 * our resolvers are giving us some error on SRV lookups
		 * (e.g. because they don't implement the record type).
		 */
		var d = new Date();
		d.setTime(d.getTime() + 1000*60*5);
		self.r_nextService = d;

		self.gotoState('aaaa');
	}
};

CueBallResolver.prototype.state_aaaa = function (on, once, timeout) {
	this.r_srvRem = this.r_srvs.slice();
	this.r_nextV6 = undefined;
	this.gotoState('aaaa_next');
};

CueBallResolver.prototype.state_aaaa_next = function () {
	var r = this.r_retry;
	r.delay = r.minDelay;
	r.count = r.max;

	var srv = this.r_srvRem.shift();
	if (srv) {
		this.r_srv = srv;
		this.gotoState('aaaa_try');
	} else {
		/* Lookups are all done, proceed on through. */
		this.gotoState('a');
	}
};

CueBallResolver.prototype.state_aaaa_try = function (on, once, timeout) {
	var self = this;
	var srv = this.r_srv;
	var req = this.resolve(srv.name, 'AAAA');
	once(req, 'answers', function (ans, ttl) {
		var d = new Date();
		d.setTime(d.getTime() + 1000*ttl);
		if (self.r_nextV6 === undefined || d <= self.r_nextV6)
			self.r_nextV6 = d;

		srv.addresses_v6 = ans.map(function (v) {
			return (v.address);
		});
		self.gotoState('aaaa_next');
	});
	once(req, 'error', function (err) {
		self.r_lastError = err;
		self.gotoState('aaaa_error');
	});
	req.send();
};

CueBallResolver.prototype.state_aaaa_error = function (on, once, timeout) {
	var self = this;
	var r = self.r_retry;
	if (--r.count > 0) {
		timeout(r.delay, function () {
			self.gotoState('aaaa_try');
		});

		r.delay *= 2;
		if (r.delay > r.maxDelay)
			r.delay = r.maxDelay;

	} else {
		self.r_log.trace(self.r_lastError,
		    'repeated error during AAAA resolution for name %s, ' +
		    'proceeding', self.r_srv.name);

		var d = new Date();
		d.setTime(d.getTime() + 1000*60*60);
		if (self.r_nextV6 === undefined || d <= self.r_nextV6)
			self.r_nextV6 = d;

		self.gotoState('aaaa_next');
	}
};

CueBallResolver.prototype.state_a = function (on, once, timeout) {
	this.r_srvRem = this.r_srvs.slice();
	this.r_nextV4 = undefined;
	this.gotoState('a_next');
};

CueBallResolver.prototype.state_a_next = function () {
	var r = this.r_retry;
	r.delay = r.minDelay;
	r.count = r.max;

	var srv = this.r_srvRem.shift();
	if (srv) {
		this.r_srv = srv;
		this.gotoState('a_try');
	} else {
		/* Lookups are all done, proceed on through. */
		this.gotoState('process');
	}
};

CueBallResolver.prototype.state_a_try = function (on, once, timeout) {
	var self = this;
	var srv = this.r_srv;
	var req = this.resolve(srv.name, 'A');
	once(req, 'answers', function (ans, ttl) {
		var d = new Date();
		d.setTime(d.getTime() + 1000*ttl);
		if (self.r_nextV4 === undefined || d <= self.r_nextV4)
			self.r_nextV4 = d;

		srv.addresses_v4 = ans.map(function (v) {
			return (v.address);
		});
		self.gotoState('a_next');
	});
	once(req, 'error', function (err) {
		self.r_lastError = err;
		self.gotoState('a_error');
	});
	req.send();
};

CueBallResolver.prototype.state_a_error = function (on, once, timeout) {
	var self = this;
	var r = self.r_retry;
	if (--r.count > 0) {
		timeout(r.delay, function () {
			self.gotoState('a_try');
		});

		r.delay *= 2;
		if (r.delay > r.maxDelay)
			r.delay = r.maxDelay;

	} else {
		self.r_log.debug(self.r_lastError,
		    'repeated error during A resolution for name %s, ' +
		    'proceeding', self.r_srv.name);

		var d = new Date();
		d.setTime(d.getTime() + 1000*60);
		if (self.r_nextV4 === undefined || d <= self.r_nextV4)
			self.r_nextV4 = d;

		self.gotoState('a_next');
	}
};

CueBallResolver.prototype.state_process = function () {
	var self = this;

	var oldBackends = this.r_backends;
	var newBackends = {};
	this.r_srvs.forEach(function (srv) {
		srv.addresses = [].
		    concat(srv.addresses_v6 || []).
		    concat(srv.addresses_v4 || []);
		srv.addresses.forEach(function (addr) {
			var finalSrv = {
				name: srv.name,
				port: srv.port,
				address: addr
			};
			newBackends[srvKey(finalSrv)] = finalSrv;
		});
	});

	var added = [];
	var removed = [];

	var oldKeys = Object.keys(oldBackends);
	var newKeys = Object.keys(newBackends);

	if (newKeys.length === 0) {
		this.r_log.warn(this.r_lastError, 'failed to find any ' +
		    'backend records for (%s.)%s (more details in TRACE)',
		    this.r_service, this.r_domain);
	}

	oldKeys.forEach(function (k) {
		if (newBackends[k] === undefined)
			removed.push(k);
	});
	newKeys.forEach(function (k) {
		if (oldBackends[k] === undefined)
			added.push(k);
	});

	this.r_backends = newBackends;

	removed.forEach(function (k) {
		self.r_log.trace('host removed: %s', k);
		self.emit('removed', k);
	});
	added.forEach(function (k) {
		self.r_log.trace('host added: %s', k);
		self.emit('added', k, newBackends[k]);
	});

	this.gotoState('sleep');
};

CueBallResolver.prototype.state_sleep = function (on, once, timeout) {
	var self = this;
	var now = new Date();
	var minDelay, state;

	if (this.r_stopping) {
		this.gotoState('init');
		return;
	}

	minDelay = this.r_nextService - now;
	state = 'srv';
	if (this.r_nextV6 - now < minDelay) {
		minDelay = this.r_nextV6 - now;
		state = 'aaaa';
	}
	if (this.r_nextV4 - now < minDelay) {
		minDelay = this.r_nextV4 - now;
		state = 'a';
	}

	if (minDelay < 0) {
		this.gotoState(state);
	} else {
		self.r_log.trace({state: state, delay: minDelay},
		    'sleeping until next TTL expiry');
		var t = timeout(minDelay, function () {
			self.gotoState(state);
		});
		t.unref();
		on(this, 'stopAsserted', function () {
			self.gotoState('init');
		});
	}
};

function srvKey(srv) {
	var hash = mod_crypto.createHash('sha1');
	hash.update(srv.name);
	hash.update('||');
	hash.update(String(srv.port));
	hash.update('||');
	var ip = mod_ipaddr.parse(srv.address);
	var addr;
	if (ip.toNormalizedString)
		addr = ip.toNormalizedString();
	else
		addr = ip.toString();
	hash.update(addr);
	return (hash.digest('base64'));
}

function NoRecordsError(name) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, NoRecordsError);
	this.name = 'NoRecordsError';
	this.message = 'No records returned for name ' + name;
	this.dnsName = name;
}
mod_util.inherits(NoRecordsError, Error);
NoRecordsError.isInstance = function (i) {
	return (i instanceof NoRecordsError);
};

function TimeoutError(name) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, TimeoutError);
	this.name = 'NoRecordsError';
	this.message = 'Timeout while contacting resolvers for name ' + name;
	this.dnsName = name;
}
mod_util.inherits(TimeoutError, Error);

CueBallResolver.prototype.resolve = function (domain, type) {
	var rs = mod_utils.shuffle(this.r_resolvers.slice()).
	    slice(0, this.r_reslimit);

	var gotAnswer = false;

	var errCount = 0;
	var self = this;

	var prot = 'udp';
	var key = domain + '|' + type;
	if (this.r_tcpNeeded.test(key))
		prot = 'tcp';

	var em = new EventEmitter();

	em.send = function () {
		em.reqs = rs.map(function (res) {
			return (self._dnsLookup(domain, res, type, prot,
			    function (err, ans, ttl) {
				if (!err && ans.length < 1)
					err = new NoRecordsError(domain);
				if (err) {
					if (++errCount >= rs.length)
						em.emit('error', err);
					return;
				}
				if (gotAnswer)
					return;
				gotAnswer = true;
				em.reqs.forEach(function (req) {
					req.cancel();
				});
				em.emit('answers', ans, ttl);
			}));
		});
	};

	return (em);
};

CueBallResolver.prototype._dnsLookup = function (dom, res, type, prot, cb) {
	var self = this;

	var q = mod_dns.Question({
		name: dom,
		type: type
	});
	var req = mod_dns.Request({
		question: q,
		server: { address: res, port: 53, type: prot },
		timeout: 1000,
		try_edns: true
	});
	this.r_log.trace({domain: dom, resolver: res, type: type, protocol: prot}, 'dnsLookup');

	req.on('timeout', function () {
		cb(new TimeoutError(dom));
		return;
	});

	req.on('message', function (err, answer) {
		if (answer.header.tc && prot === 'udp') {
			self.r_tcpNeeded.add(dom + '|' + type);

			var nreq = self._dnsLookup(dom, res, type, 'tcp', cb);
			req.cancel = function () {
				nreq.cancel();
			};
			return;
		}
		if (err) {
			cb(err);
			return;
		}

		var minTTL = undefined;
		if (type === 'A' || type === 'AAAA') {
			var ans = answer.answer.map(function (a) {
				if (minTTL === undefined ||
				    a.ttl < minTTL) {
					minTTL = a.ttl;
				}
				return ({
					name: a.name,
					address: a.address
				});
			});
			cb(null, ans, minTTL);

		} else if (type === 'SRV') {
			var cache = {};
			answer.additional.forEach(function (rr) {
				if (rr.address) {
					if (minTTL === undefined ||
					    a.ttl < minTTL) {
						minTTL = a.ttl;
					}
					cache[rr.name] = rr.address;
				}
			});
			var ans = answer.answer.map(function (a) {
				if (minTTL === undefined ||
				    a.ttl < minTTL) {
					minTTL = a.ttl;
				}
				var obj = { name: a.target, port: a.port };
				if (cache[a.target])
					obj.address = cache[a.target];
				return (obj);
			});
			cb(null, ans, minTTL);

		} else {
			throw (new Error('Invalid record type ' + type));
		}
	});

	req.send();

	return (req);
};
