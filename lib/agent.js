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
			self.addPool(host, {});
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

CueBallAgent.prototype.addPool = function (host, options) {
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
	var res = mod_resolver.resolverForIpOrDomain({
		input: host,
		resolverConfig: {
			resolvers: this.resolvers,
			service: this.service,
			defaultPort: defPort,
			recovery: this.cba_recovery,
			log: this.log
		}
	});
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
			if (options.hasOwnProperty(k))
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
		poolOpts.checker = this.checkSocket.bind(this, host);
	}
	this.log.debug({ host: host }, 'CueBallAgent creating new pool');
	this.pools[host] = new Pool(poolOpts);
	res.start();
	this.poolResolvers[host] = res;
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
		delete (self.pools[host]);
		delete (self.poolResolvers[host]);
		if (pool.isInState('stopped')) {
			pcb();
			if (!res.isInState('stopped'))
				res.stop();
		} else {
			pool.on('stateChanged', function (st) {
				if (st === 'stopped') {
					if (!res.isInState('stopped'))
						res.stop();
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
		this.addPool(host, options);
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
		socket.once('agentRemove', onAgentRemove);
		req.onSocket(socket);
	});

	function onAbort() {
		if (waiter !== undefined) {
			waiter.cancel();
			waiter = undefined;
		}
		if (conn !== undefined) {
			sock.removeListener('free', onFree);
			sock.removeListener('agentRemove', onAgentRemove);
			conn.close();
			conn = undefined;
			sock = undefined;
		}
	}
	function onFree() {
		sock.removeListener('agentRemove', onAgentRemove);
		req.removeListener('abort', onAbort);
		conn.release();
		conn = undefined;
		sock = undefined;
	}
	function onAgentRemove() {
		sock.removeListener('free', onFree);
		req.removeListener('abort', onAbort);
		conn.close();
		conn = undefined;
		sock = undefined;
	}
};

CueBallAgent.prototype.checkSocket = function (host, handle, socket) {
	var self = this;
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
				self.log.warn('got a 500, closing');
				handle.close();
			} else {
				self.log.info('health check ok, releasing');
				handle.release();
			}
		});
	});
	req.once('error', function (e) {
		self.log.warn(e, 'check failed');
		handle.close();
	});
	req.end();
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
