/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	Resolver: CueBallResolver,
	StaticIpResolver: CueBallStaticResolver,
	resolverForIpOrDomain: resolverForIpOrDomain,

	/* exposed for testing only */
	configForIpOrDomain: configForIpOrDomain,
	parseIpOrDomain: parseIpOrDomain
};

const mod_nsc = require('named-client');
const mod_events = require('events');
const mod_net = require('net');
const mod_util = require('util');
const mod_mooremachine = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_utils = require('./utils');
const mod_vasync = require('vasync');
const mod_bunyan = require('bunyan');
const mod_ipaddr = require('ipaddr.js');
const mod_fs = require('fs');
const mod_crypto = require('crypto');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

/*
 * Cueball provides two types of resolvers: the primary interface is the
 * DNS-based resolver (called just "Resolver" for historical reasons) that
 * uses DNS servers to locate backends.  This is appropriate for most server
 * deployments.  The static resolver emits a pre-configured set of IP addresses
 * and is intended for development environments and debugging tools where the
 * user may want to target specific instances.
 *
 * Resolvers take a domain (plus service name and default port) and resolve it,
 * emitting events 'added' and 'removed' as new hosts behind this domain +
 * service are discovered (or discovered to be gone).
 *
 * The 'added' event receives both a key and an object. The key is a unique
 * string to identify this particular backend host (and will be provided again
 * on the 'removed' event if this backend later goes away). The object contains
 * the address and port to connect to to reach this backend.
 *
 * The factory method resolverForDomain can be used for programs that intend to
 * support both DNS-based resolution or static IP resolution, depending on
 * whether the user provides a DNS hostname or an IP address.
 */

/*
 * DNS-based Resolver
 *
 * The basic workflow for the DNS-based resolver is to query for SRV records,
 * then AAAA, then A, then work out which events to emit (if any). After this it
 * sleeps.  At each step it records the TTL for the information collected, and
 * when a TTL expires, we resume the workflow at the point where the expiring
 * information was gathered (e.g. if an SRV record expired we would re-query all
 * the AAAA and A records, but if an A record expired, only that stage would be
 * re-run).
 *
 * When SRV record lookup succeeds, the ports will be set based on the contents
 * of these records. If SRV records are not available, then the 'domain' will
 * be looked up as a plain A/AAAA name, and the defaultPort option will
 * determine what port number appears in the backend objects passed to 'added'.
 *
 * The Resolver takes as one of its options a list of 'resolvers' to use. If
 * not provided, it will use the system resolvers (obtained by node-native-dns
 * parsing /etc/resolv.conf for us).
 *
 * Instead of providing an array of IP addresses as 'resolvers', you can also
 * provide an array of a single DNS name as a string. In this case, the Resolver
 * will "bootstrap" by using the system resolvers to look up this name (in the
 * same fashion as a regular Resolver would, with the service name _dns._udp).
 * Then it will proceed to manage its list of 'resolvers' by looking up the
 * provided name. In this way, HA setups can remove and drain nameservers with
 * zero impact just like any other service, by removing them from DNS and
 * waiting until traffic subsides.
 */
function CueBallResolver(options) {
	mod_assert.object(options);
	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.string(options.domain, 'options.domain');
	mod_assert.optionalString(options.service, 'options.service');
	mod_assert.optionalNumber(options.maxDNSConcurrency,
	    'options.maxDNSConcurrency');
	mod_assert.optionalNumber(options.defaultPort, 'options.defaultPort');

	this.r_resolvers = options.resolvers || [];
	this.r_domain = options.domain;
	this.r_service = options.service || '_http._tcp';
	this.r_maxres = options.maxDNSConcurrency || 3;
	this.r_defport = options.defaultPort || 80;

	mod_assert.optionalObject(options.log, 'options.log');
	this.r_log = options.log || mod_bunyan.createLogger({
		name: 'CueBallResolver'
	});
	this.r_log = this.r_log.child({domain: this.r_domain});

	mod_assert.object(options.recovery, 'options.recovery');
	this.r_recovery = options.recovery;

	var dnsSrvRecov = options.recovery.default;
	var dnsRecov = options.recovery.default;
	if (options.recovery.dns) {
		dnsSrvRecov = options.recovery.dns;
		dnsRecov = options.recovery.dns;
	}
	if (options.recovery.dns_srv)
		dnsSrvRecov = options.recovery.dns_srv;
	mod_utils.assertRecovery(dnsSrvRecov, 'recovery.dns_srv');
	mod_utils.assertRecovery(dnsRecov, 'recovery.dns');

	this.r_srvRetry = {
		max: dnsSrvRecov.retries,
		count: dnsSrvRecov.retries,
		timeout: dnsSrvRecov.timeout,
		minDelay: dnsSrvRecov.delay,
		delay: dnsSrvRecov.delay,
		maxDelay: dnsSrvRecov.maxDelay || Infinity
	};

	this.r_retry = {
		max: dnsRecov.retries,
		count: dnsRecov.retries,
		timeout: dnsRecov.timeout,
		minDelay: dnsRecov.delay,
		delay: dnsRecov.delay,
		maxDelay: dnsRecov.maxDelay || Infinity
	};

	this.r_nextService = new Date();
	this.r_nextV6 = new Date();
	this.r_nextV4 = new Date();

	this.r_lastError = undefined;

	this.r_srvs = [];
	this.r_srvRem = [];
	this.r_srv = undefined;
	this.r_backends = {};
	this.r_bootstrap = undefined;
	this.r_bootstrapRes = {};

	this.r_nsclient = new mod_nsc.DnsClient({
		concurrency: this.r_maxres
	});

	this.r_stopping = false;

	FSM.call(this, 'init');
}
mod_util.inherits(CueBallResolver, FSM);

CueBallResolver.bootstrapResolvers = {};

CueBallResolver.prototype.start = function () {
	this.emit('startAsserted');
};

CueBallResolver.prototype.stop = function (cb) {
	this.r_stopping = true;
	this.emit('stopAsserted');
};

CueBallResolver.prototype.count = function () {
	return (Object.keys(this.r_backends).length);
};

CueBallResolver.prototype.list = function () {
	var self = this;
	var ret = {};
	Object.keys(this.r_backends).forEach(function (k) {
		ret[k] = self.r_backends[k];
	});
	return (ret);
};

CueBallResolver.prototype.state_init = function (on) {
	var self = this;
	this.r_stopping = false;
	on(this, 'startAsserted', function () {
		self.gotoState('check_ns');
	});
};

CueBallResolver.prototype.state_check_ns = function (on, once) {
	var self = this;
	if (this.r_resolvers.length > 0) {
		var notIp = this.r_resolvers.filter(function (r) {
			return (!mod_net.isIPv4(r) && !mod_net.isIPv6(r));
		});
		if (notIp.length === 0) {
			this.gotoState('srv');
			return;
		}
		mod_assert.equal(notIp.length, 1);
		this.r_resolvers = [];
		this.r_bootstrap = CueBallResolver.bootstrapResolvers[notIp[0]];
		if (this.r_bootstrap === undefined) {
			this.r_bootstrap = new CueBallResolver({
				domain: notIp[0],
				service: '_dns._udp',
				defaultPort: 53,
				log: this.r_log,
				recovery: this.r_recovery
			});
			CueBallResolver.bootstrapResolvers[notIp[0]] =
			    this.r_bootstrap;
		}
		this.gotoState('bootstrap_ns');
	} else {
		mod_fs.readFile('/etc/resolv.conf', 'ascii',
		    function (err, file) {
			if (err) {
				self.r_resolvers = ['8.8.8.8', '8.8.4.4'];
				self.gotoState('srv');
				return;
			}
			self.r_resolvers = [];
			file.split(/\n/).forEach(function (line) {
				var m = line.match(
				    /^\s*nameserver\s*([^\s]+)\s*$/);
				if (m && mod_net.isIP(m[1])) {
					self.r_resolvers.push(m[1]);
				}
			});
			self.gotoState('srv');
		});
	}
};

CueBallResolver.prototype.state_bootstrap_ns = function (on, once) {
	var self = this;
	this.r_bootstrap.on('added', function (k, srv) {
		self.r_bootstrapRes[k] = srv;
		self.r_resolvers.push(srv.address);
		if (self.r_bootstrap.r_resolvers.indexOf(srv.address) === -1)
			self.r_bootstrap.r_resolvers.push(srv.address);
	});
	this.r_bootstrap.on('removed', function (k) {
		var srv = self.r_bootstrapRes[k];
		delete (self.r_bootstrapRes[k]);

		var idx = self.r_resolvers.indexOf(srv.address);
		mod_assert.ok(idx !== -1);
		self.r_resolvers.splice(idx, 1);

		idx = self.r_bootstrap.r_resolvers.indexOf(srv.address);
		if (idx !== -1)
			self.r_bootstrap.r_resolvers.splice(idx, 1);
	});
	if (this.r_bootstrap.count() > 0) {
		var srvs = this.r_bootstrap.list();
		self.r_bootstrapRes = srvs;
		Object.keys(srvs).forEach(function (k) {
			self.r_resolvers.push(srvs[k].address);
		});
		self.gotoState('srv');
	} else {
		once(this.r_bootstrap, 'added', function () {
			self.gotoState('srv');
		});
		this.r_bootstrap.start();
	}
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
	var req = this.resolve(name, 'SRV', this.r_srvRetry.timeout);
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
			self.r_srvs = [ {
				name: self.r_domain,
				port: self.r_defport
			} ];

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

		self.r_srvs = [ {
			name: self.r_domain,
			port: self.r_defport
		} ];

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

	if (srv.additionals && srv.additionals.length > 0) {
		self.r_log.trace('skipping v6 lookup for %s, using ' +
		    'additionals from SRV', srv.name);
		srv.addresses_v6 = srv.additionals.filter(function (a) {
			return (mod_net.isIPv6(a));
		});
		self.gotoState('aaaa_next');
		return;
	}

	var req = this.resolve(srv.name, 'AAAA', this.r_retry.timeout);
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

	if (srv.additionals && srv.additionals.length > 0) {
		self.r_log.trace('skipping v4 lookup for %s, using ' +
		    'additionals from SRV', srv.name);
		srv.addresses_v4 = srv.additionals.filter(function (a) {
			return (mod_net.isIPv4(a));
		});
		self.gotoState('a_next');
		return;
	}

	var req = this.resolve(srv.name, 'A', this.r_retry.timeout);
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
		this.emit('updated');
		this.gotoState('sleep');
		return;
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

	this.emit('updated');
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

CueBallResolver.prototype.resolve = function (domain, type, timeout) {
	var opts = {};
	opts.domain = domain;
	opts.type = type;
	opts.timeout = timeout;
	opts.resolvers = this.r_resolvers;

	var self = this;
	var em = new EventEmitter();

	em.send = function () {
		self.r_nsclient.lookup(opts, onLookup);
	};

	return (em);

	function onLookup(err, msg) {
		if (err && err.code === 'NXDOMAIN')
			err = new NoRecordsError(domain);
		if (err) {
			em.emit('error', err);
			return;
		}
		var ans;
		var answers = msg.getAnswers();
		var minTTL = undefined;
		if (type === 'A' || type === 'AAAA') {
			ans = answers.map(function (a) {
				if (minTTL === undefined ||
				    a.ttl < minTTL) {
					minTTL = a.ttl;
				}
				return ({
					name: a.name,
					address: a.target
				});
			});

		} else if (type === 'SRV') {
			var cache = {};
			var addns = msg.getAdditionals();
			addns.forEach(function (rr) {
				if (rr.target) {
					if (minTTL === undefined ||
					    rr.ttl < minTTL) {
						minTTL = rr.ttl;
					}
					if (cache[rr.name] === undefined)
						cache[rr.name] = [];
					cache[rr.name].push(rr.target);
				}
			});
			ans = answers.map(function (a) {
				if (minTTL === undefined ||
				    a.ttl < minTTL) {
					minTTL = a.ttl;
				}
				var obj = { name: a.target, port: a.port };
				if (cache[a.target])
					obj.additionals = cache[a.target];
				return (obj);
			});

		} else {
			throw (new Error('Invalid record type ' + type));
		}
		em.emit('answers', ans, minTTL);
	}
};


/*
 * Static IP Resolver
 *
 * This Resolver implementation emits a fixed list of IP addresses.  This is
 * useful in development environments and debugging tools, where users may want
 * to target specific service instances.
 */
function CueBallStaticResolver(options) {
	mod_assert.object(options, 'options');
	mod_assert.arrayOfObject(options.backends, 'options.backends');

	this.sr_backends = options.backends.map(function (backend, i) {
		mod_assert.string(backend.address,
		    'options.backends[' + i + '].address');
		mod_assert.ok(mod_net.isIP(backend.address),
		    'options.backends[' + i +
		    '].address must be an IP address');
		mod_assert.number(backend.port,
		    'options.backends[' + i + '].port');

		return ({
		    'name': backend.address + ':' + backend.port,
		    'address': backend.address,
		    'port': backend.port
		});
	});
	this.sr_state = 'idle';

	EventEmitter.call(this);
}

mod_util.inherits(CueBallStaticResolver, EventEmitter);

CueBallStaticResolver.prototype.start = function ()
{
	var self = this;

	mod_assert.equal(this.sr_state, 'idle',
	    'cannot call start() again without calling stop()');
	this.sr_state = 'started';

	setImmediate(function () {
		self.sr_backends.forEach(function (be) {
			self.emit('added', srvKey(be), be);
		});

		self.emit('updated');
	});
};

CueBallStaticResolver.prototype.stop = function ()
{
	mod_assert.equal(this.sr_state, 'started',
	    'cannot call stop() again without calling start()');
	this.sr_state = 'idle';
};

CueBallStaticResolver.prototype.count = function ()
{
	return (this.sr_backends.length);
};

CueBallStaticResolver.prototype.list = function ()
{
	var ret = {};

	this.sr_backends.forEach(function (be) {
		ret[srvKey(be)] = be;
	});

	return (ret);
};


/*
 * resolverForIpOrDomain(args): given an input string of the form:
 *
 *     HOSTNAME[:PORT]
 *
 * where HOSTNAME may be either a DNS domain or IP address and PORT is an
 * integer representing a TCP port, return an appropriate resolver instance that
 * either uses the static IP resolver (if HOSTNAME is determined to be an IP
 * address) or the DNS-based Resolver class (otherwise).
 *
 * Named arguments include:
 *
 *    input           the input string (described above)
 *
 *    resolverConfig  configuration properties passed to the resolver's
 *                    constructor
 *
 * This is the appropriate interface for constructing a resolver from
 * user-specified configuration because it allows users to specify IP addresses
 * or DNS names interchangeably, which is what most users expect.
 *
 * If the input is well-formed but invalid (e.g., has the correct JavaScript
 * types, but the port number is out of range, or the HOSTNAME portion cannot be
 * interpreted as either an IP address or a DNS domain), then an Error object is
 * returned.
 */
function resolverForIpOrDomain(args)
{
	var speccfg, cons, rcfg;

	speccfg = configForIpOrDomain(args);
	if (speccfg instanceof Error) {
		return (speccfg);
	}

	cons = speccfg.cons;
	rcfg = speccfg.mergedConfig;
	return (new cons(rcfg));
}

/*
 * Implements the guts of resolverForIpOrDomain().
 */
function configForIpOrDomain(args)
{
	var rcfg, speccfg, k;

	mod_assert.object(args, 'args');
	mod_assert.string(args.input, 'args.input');
	mod_assert.optionalObject(args.resolverConfig, 'args.resolverConfig');

	rcfg = {};
	if (args.resolverConfig) {
		for (k in args.resolverConfig) {
			rcfg[k] = args.resolverConfig[k];
		}
	}

	speccfg = parseIpOrDomain(args.input);
	if (speccfg instanceof Error) {
		return (speccfg);
	}

	for (k in speccfg.config) {
		rcfg[k] = speccfg.config[k];
	}

	speccfg.mergedConfig = rcfg;
	return (speccfg);
}

/*
 * Implements the parsing part of resolverForIpOrDomain().
 */
function parseIpOrDomain(str)
{
	var colon, first, port, ret;

	colon = str.lastIndexOf(':');
	if (colon == -1) {
		first = str;
		port = undefined;
	} else {
		first = str.substr(0, colon);
		port = parseInt(str.substr(colon + 1), 10);
		if (isNaN(port) || port < 0 || port > 65535) {
			return (new Error('unsupported port in input: ' + str));
		}
	}

	ret = {};
	if (mod_net.isIP(first) === 0) {
		ret['kind'] = 'dns';
		ret['cons'] = CueBallResolver;
		/* XXX validate DNS domain? */
		ret['config'] = {
		    'domain': first
		};

		if (port !== undefined) {
			ret['config']['defaultPort'] = port;
		}
	} else {
		ret['kind'] = 'static';
		ret['cons'] = CueBallStaticResolver;
		ret['config'] = {
		    'backends': [ {
			'address': first,
			'port': port
		    } ]
		};
	}

	return (ret);
}
