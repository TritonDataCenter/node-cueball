/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2017, Joyent, Inc.
 */

module.exports = {
	Agent: CueBallAgent,
	HttpAgent: CueBallHttpAgent,
	HttpsAgent: CueBallHttpsAgent
};

const EventEmitter = require('events').EventEmitter;
const mod_net = require('net');
const mod_tls = require('tls');
const mod_util = require('util');
const mod_pool = require('./pool');
const mod_assert = require('assert-plus');
const mod_http = require('http');
const mod_https = require('https');
const mod_utils = require('./utils');
const mod_vasync = require('vasync');
const mod_bunyan = require('bunyan');
const mod_resolver = require('./resolver');

const Pool = mod_pool.ConnectionPool;

function CueBallAgent(options) {
	mod_assert.object(options, 'options');
	mod_assert.number(options.defaultPort, 'options.defaultPort');
	mod_assert.string(options.protocol, 'options.protocol');
	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.optionalArrayOfString(options.initialDomains,
	    'options.initialDomains');
	mod_assert.optionalNumber(options.tcpKeepAliveInitialDelay,
	    'options.tcpKeepAliveInitialDelay');
	mod_assert.optionalObject(options.log, 'options.log');

	EventEmitter.call(this);

	/* These are used as defaults in the ClientRequest constructor. */
	this.defaultPort = options.defaultPort;
	this.protocol = options.protocol + ':';
	this.service = '_' + options.protocol + '._tcp';

	/* Make the ClientRequest set Connection: keep-alive. */
	this.keepAlive = true;
	this.maxSockets = Infinity;

	this.tcpKAID = options.tcpKeepAliveInitialDelay;

	this.pools = {};
	this.poolResolvers = {};
	this.poolExternalResolvers = {};
	this.resolvers = options.resolvers;
	this.log = options.log || mod_bunyan.createLogger({
		name: 'cueball'
	});
	this.log = this.log.child({
		component: 'CueBallAgent'
	});
	this.cba_stopped = false;

	mod_assert.number(options.spares, 'options.spares');
	mod_assert.number(options.maximum, 'options.maximum');
	this.maximum = options.maximum;
	this.spares = options.spares;

	mod_assert.optionalString(options.ping, 'options.ping');
	this.cba_ping = options.ping;
	mod_assert.optionalNumber(options.pingInterval, 'options.pingInterval');
	this.cba_pingInterval = options.pingInterval;

	mod_assert.object(options.recovery, 'options.recovery');
	this.cba_recovery = options.recovery;
	mod_utils.assertRecovery(options.recovery.default, 'recovery.default');

	mod_assert.optionalBool(options.errorOnEmpty, 'options.errorOnEmpty');
	this.cba_errOnEmpty = options.errorOnEmpty;

	if (options.initialDomains !== undefined &&
	    options.initialDomains !== null) {
		var self = this;
		options.initialDomains.forEach(function (host) {
			self._addPool(host, {});
		});
	}
}
mod_util.inherits(CueBallAgent, EventEmitter);

const PASS_FIELDS = ['pfx', 'key', 'passphrase', 'cert', 'ca', 'ciphers',
    'servername', 'rejectUnauthorized'];

var USE_SECURECONNECT = false;
if (/^v0\.[0-9]\./.test(process.version) ||
    /^v0\.10\./.test(process.version)) {
	USE_SECURECONNECT = true;
}

CueBallAgent.prototype._addPool = function (host, options) {
	var self = this;
	if (this.cba_stopped) {
		throw (new Error('Cannot add a pool to a stopped agent'));
	}
	mod_assert.string(host, 'hostname');
	var defPort = this.defaultPort;
	if (typeof (options.port) === 'number')
		defPort = options.port;
	if (typeof (options.port) === 'string')
		defPort = parseInt(options.port, 10);
	mod_assert.optionalNumber(defPort, 'default port');

	var res;
	var useExternalResolver = (options.resolver !== undefined &&
	    options.resolver !== null);

	if (useExternalResolver) {
		res = options.resolver;
	} else {
		res = mod_resolver.resolverForIpOrDomain({
			input: host,
			resolverConfig: {
				resolvers: this.resolvers,
				service: this.service,
				defaultPort: defPort,
				recovery: this.cba_recovery,
				log: this.log
			}
		});
	}
	var poolOpts = {
		resolver: res,
		domain: host,
		constructor: constructSocket,
		maximum: this.maximum,
		spares: this.spares,
		log: this.log,
		recovery: this.cba_recovery
	};
	function constructSocket(backend) {
		var opts = {
			host: backend.address || backend.name,
			port: backend.port || defPort,
			servername: backend.name || host
		};
		PASS_FIELDS.forEach(function (k) {
			if (options[k] !== null &&
			    options[k] !== undefined)
				opts[k] = options[k];
		});
		var nsock;
		if (self.protocol === 'https:') {
			nsock = mod_tls.connect(opts);
			/*
			 * In older versions of node, TLS sockets don't
			 * quite obey the socket interface -- they emit
			 * the event 'secureConnect' instead of
			 * 'connect' and they don't support ref/unref.
			 *
			 * We polyfill these here.
			 */
			if (USE_SECURECONNECT) {
				nsock.on('secureConnect',
				    nsock.emit.bind(nsock, 'connect'));
			}
			if (nsock.unref === undefined) {
				nsock.unref = function () {
					nsock.socket.unref();
				};
				nsock.ref = function () {
					nsock.socket.ref();
				};
			}
		} else {
			nsock = mod_net.createConnection(opts);
		}
		if (self.tcpKAID !== undefined &&
		    self.tcpKAID !== null) {
			nsock.on('connect', function () {
				if (USE_SECURECONNECT &&
				    self.protocol === 'https:') {
					nsock.socket.setKeepAlive(true,
					    self.tcpKAID);
				} else {
					nsock.setKeepAlive(true,
					    self.tcpKAID);
				}
			});
		}
		return (nsock);
	}
	if (this.cba_ping !== undefined &&
	    this.cba_ping !== null) {
		poolOpts.checkTimeout = this.cba_pingInterval || 30000;
		poolOpts.checker = this._checkSocket.bind(this, host);
	}
	this.log.debug({ host: host }, 'CueBallAgent creating new pool');
	this.pools[host] = new Pool(poolOpts);
	if (useExternalResolver) {
		this.poolExternalResolvers[host] = res;
	} else {
		res.start();
		this.poolResolvers[host] = res;
	}
};

CueBallAgent.prototype.stop = function (cb) {
	var self = this;
	if (this.cba_stopped) {
		throw (new Error('Cannot stop a CueBallAgent that has ' +
		    'already stopped'));
	}
	this.cba_stopped = true;
	this.log.debug('CueBallAgent stopping all pools');
	mod_vasync.forEachParallel({
		inputs: Object.keys(this.pools),
		func: stopPool
	}, function (err) {
		self.log.info('CueBallAgent has stopped all pools');
		if (cb) {
			setImmediate(function () {
				cb(err);
			});
		}
	});
	function stopPool(host, pcb) {
		var pool = self.pools[host];
		var res = self.poolResolvers[host];
		var hasExternalResolver = false;

		delete (self.pools[host]);

		if (res === undefined) {
			res = self.poolExternalResolvers[host];
			mod_assert.ok(res, 'found pool with no resolver');
			hasExternalResolver = true;
			delete (self.poolExternalResolvers[host]);
		} else {
			delete (self.poolResolvers[host]);
		}

		if (pool.isInState('stopped')) {
			pcb();
			if (!hasExternalResolver && !res.isInState('stopped'))
				res.stop();
		} else {
			pool.on('stateChanged', function (st) {
				if (st === 'stopped') {
					if (!hasExternalResolver &&
					    !res.isInState('stopped')) {
						res.stop();
					}
					pcb();
				}
			});
			pool.stop();
		}
	}
};

/*
 * Sets up a duplex stream to be used for the given HTTP request.
 * Calls req.onSocket(sock) with said stream once it is ready.
 *
 * When the request has finished with the socket, it forces the socket to
 * emit either "free" or "agentRemove", depending on whether the request
 * believes the socket to be reuseable or not.
 */
CueBallAgent.prototype.addRequest = function (req, optionsOrHost, port) {
	if (this.cba_stopped) {
		throw (new Error('CueBallAgent is stopped and cannot handle ' +
		    'new requests'));
	}
	var options;
	mod_assert.object(req, 'req');
	if (typeof (optionsOrHost) === 'string') {
		options = {};
		options.host = optionsOrHost;
		options.port = port;
	} else {
		options = optionsOrHost;
		mod_assert.object(options, 'options');
	}
	var host = options.host || options.hostname;
	mod_assert.string(host, 'hostname');
	if (this.pools[host] === undefined) {
		this._addPool(host, options);
	}
	var pool = this.pools[host];
	var waiter, sock, conn;

	req.once('abort', onAbort);

	var claimOpts = {
		errorOnEmpty: this.cba_errOnEmpty
	};
	waiter = pool.claim(claimOpts, function (err, connh, socket) {
		waiter = undefined;
		if (err) {
			/*
			 * addRequest has no way to give an async error back
			 * to the rest of the http stack, except to create a
			 * fake socket here and make it emit 'error' in the
			 * next event loop :(
			 */
			var fakesock = new FakeSocket();
			req.onSocket(fakesock);
			setImmediate(function () {
				fakesock.emit('error', err);
			});
			return;
		}
		sock = socket;
		conn = connh;

		socket.once('free', onFree);
		socket.once('close', onClose);
		socket.once('agentRemove', onAgentRemove);
		req.onSocket(socket);
	});

	function onClose() {
		sock.removeListener('free', onFree);
		sock.removeListener('agentRemove', onAgentRemove);
		req.removeListener('abort', onAbort);
		/*
		 * The node http framework (especially in 0.10.x) can leave
		 * handlers on socket events when the socket has closed and the
		 * request completed normally (e.g. the remote server doesn't
		 * support keep-alive, or we exceeded its maximum time).
		 *
		 * To avoid a spurious warning here, we disable the handler
		 * leak check.
		 */
		conn.disableReleaseLeakCheck();
		/*
		 * Unfortunately, there is no way to distinguish between a
		 * 'close' event coming out of a socket where the request
		 * succeeded, and one coming out of a socket where it closed
		 * prematurely and the HTTP-level request failed. Node does not
		 * actually have any way to tell the difference between these
		 * (and actually doesn't know whether a request "ended early"
		 * at all, since it doesn't keep track of Content-Length
		 * properly, especially in 0.10.x).
		 *
		 * As a result, if we get a 'close' event before any other
		 * event happens, we give it the benefit of the doubt and
		 * don't induce a cueball-level backoff. We do this by always
		 * calling .release() instead of .close() on the handle here.
		 */
		conn.release();
		conn = undefined;
		sock = undefined;
	}

	function onAbort() {
		if (waiter !== undefined) {
			waiter.cancel();
			waiter = undefined;
		}
		if (conn !== undefined) {
			sock.removeListener('close', onClose);
			sock.removeListener('free', onFree);
			sock.removeListener('agentRemove', onAgentRemove);
			conn.close();
			conn = undefined;
			sock = undefined;
		}
	}
	function onFree() {
		sock.removeListener('close', onClose);
		sock.removeListener('agentRemove', onAgentRemove);
		req.removeListener('abort', onAbort);
		conn.release();
		conn = undefined;
		sock = undefined;
	}
	/*
	 * Called when the http framework wants to notify us that the request
	 * did an Upgrade or similar, and the socket is now being used for
	 * other purposes (and won't be available for more HTTP requests).
	 *
	 * We keep the lease open until 'close' is emitted, and take off all
	 * our other handlers.
	 */
	function onAgentRemove() {
		sock.removeListener('free', onFree);
		req.removeListener('abort', onAbort);
	}
};

CueBallAgent.prototype._checkSocket = function (host, handle, socket) {
	var t1 = new Date();
	var log = handle.ch_slot.makeChildLogger({
		component: 'CueBallAgent',
		domain: host,
		localPort: socket.localPort,
		path: this.cba_ping
	});
	var agent = new PingAgent({
		protocol: this.protocol,
		socket: socket,
		log: this.log
	});
	var mod = mod_http;
	if (this.protocol === 'https:')
		mod = mod_https;
	var opts = {
		method: 'GET',
		path: this.cba_ping,
		hostname: host,
		agent: agent
	};
	var req = mod.request(opts, function (res) {
		res.on('readable', function () {
			var r = true;
			while (r)
				r = res.read();
		});
		res.on('end', function () {
			if (res.statusCode >= 500 && res.statusCode < 600) {
				var t2 = new Date();
				log.warn({
					statusCode: res.statusCode,
					latency: (t2.getTime() - t1.getTime())
				}, 'got a 5xx code, closing');
				handle.close();
			} else {
				log.debug({
					statusCode: res.statusCode
				}, 'health check ok, releasing');
				handle.release();
			}
		});
	});
	req.once('error', function (e) {
		var t2 = new Date();
		log.warn({
			err: {
				name: e.name,
				stack: e.stack,
				message: e.message
			},
			latency: (t2.getTime() - t1.getTime())
		}, 'check failed');
		handle.close();
	});
	req.end();
};

/* Used by muskie to check for recent shark connections. */
CueBallAgent.prototype.getPool = function (host) {
	mod_assert.string(host, 'hostname');
	return (this.pools[host]);
};

/* Used by muskie to create a pool if one doesn't already exist. */
CueBallAgent.prototype.createPool = function (host, options) {
	mod_assert.string(host, 'hostname');
	mod_assert.optionalObject(options, 'options');
	if (options) {
		mod_assert.optionalNumber(options.port, 'options.port');
		mod_assert.optionalObject(options.resolver, 'options.resolver');
	}
	if (this.pools[host] === undefined) {
		if (options) {
			var opts = {
				port: options.port,
				resolver: options.resolver
			};
			PASS_FIELDS.forEach(function (k) {
				if (options[k] !== null &&
				    options[k] !== undefined)
					opts[k] = options[k];
			});
			this._addPool(host, opts);
		} else {
			this._addPool(host, {});
		}
	} else {
		throw (new Error('Attempting to create a pool for a hostname ' +
		    'that already has one.'));
	}
};

/* Only used by UNIX socket connections with "socketPath" set. */
CueBallAgent.prototype.createConnection = function (options, connectListener) {
	throw (new Error('UNIX domain sockets not supported'));
};

function CueBallHttpAgent(options) {
	options.protocol = 'http';
	if (options.defaultPort === undefined)
		options.defaultPort = 80;
	CueBallAgent.call(this, options);
}
mod_util.inherits(CueBallHttpAgent, CueBallAgent);

function CueBallHttpsAgent(options) {
	options.protocol = 'https';
	if (options.defaultPort === undefined)
		options.defaultPort = 443;
	CueBallAgent.call(this, options);
}
mod_util.inherits(CueBallHttpsAgent, CueBallAgent);


function FakeSocket() {
	EventEmitter.call(this);
}
mod_util.inherits(FakeSocket, EventEmitter);
FakeSocket.prototype.read = function () {
	return (null);
};
FakeSocket.prototype.destroy = function () {
	return;
};


function PingAgent(options) {
	mod_assert.object(options, 'options');
	mod_assert.string(options.protocol, 'options.protocol');
	mod_assert.object(options.socket, 'options.socket');

	EventEmitter.call(this);

	this.protocol = options.protocol;

	/* Make the ClientRequest set Connection: keep-alive. */
	this.keepAlive = true;
	this.maxSockets = Infinity;

	this.pa_socket = options.socket;

	mod_assert.optionalObject(options.log, 'options.log');
	this.log = options.log;
}
mod_util.inherits(PingAgent, EventEmitter);

PingAgent.prototype.addRequest = function (req, options) {
	var sock = this.pa_socket;
	sock.once('free', onFree);
	sock.once('agentRemove', onAgentRemove);
	req.once('abort', onAbort);
	req.onSocket(sock);

	function onAbort() {
		sock.removeListener('free', onFree);
		sock.removeListener('agentRemove', onAgentRemove);
	}
	function onFree() {
		sock.removeListener('agentRemove', onAgentRemove);
		req.removeListener('abort', onAbort);
	}
	function onAgentRemove() {
		sock.removeListener('free', onFree);
		req.removeListener('abort', onAbort);
	}
};
