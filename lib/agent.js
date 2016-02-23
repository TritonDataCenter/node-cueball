/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
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

const Pool = mod_pool.ConnectionPool;

function CueBallAgent(options) {
	mod_assert.object(options, 'options');
	mod_assert.number(options.defaultPort, 'options.defaultPort');
	mod_assert.string(options.protocol, 'options.protocol');
	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.optionalObject(options.log, 'options.log');

	EventEmitter.call(this);

	/* These are used as defaults in the ClientRequest constructor. */
	this.defaultPort = options.defaultPort;
	this.protocol = options.protocol + ':';
	this.service = '_' + options.protocol + '._tcp';

	/* Make the ClientRequest set Connection: keep-alive. */
	this.keepAlive = true;
	this.maxSockets = Infinity;

	this.pools = {};
	this.resolvers = options.resolvers;
	this.log = options.log;

	mod_assert.optionalNumber(options.spares, 'options.spares');
	mod_assert.optionalNumber(options.maximum, 'options.maximum');
	this.maximum = options.maximum;
	this.spares = options.spares;

	mod_assert.optionalString(options.ping, 'options.ping');
	this.cba_ping = options.ping;
}
mod_util.inherits(CueBallAgent, EventEmitter);

const PASS_FIELDS = ['pfx', 'key', 'passphrase', 'cert', 'ca', 'ciphers',
    'servername', 'rejectUnauthorized'];

/*
 * Sets up a duplex stream to be used for the given HTTP request.
 * Calls req.onSocket(sock) with said stream once it is ready.
 *
 * When the request has finished with the socket, it forces the socket to
 * emit either "free" or "agentRemove", depending on whether the request
 * believes the socket to be reuseable or not.
 */
CueBallAgent.prototype.addRequest = function (req, options) {
	var self = this;
	var host = options.host || options.hostname;
	if (this.pools[host] === undefined) {
		var poolOpts = {
			defaultPort: this.defaultPort,
			resolvers: this.resolvers,
			service: this.service,
			domain: host,
			constructor: function (backend) {
				var opts = {
					host: backend.address || backend.name,
					port: backend.port || self.defaultPort,
					servername: backend.name || host
				};
				PASS_FIELDS.forEach(function (k) {
					if (options.hasOwnProperty(k))
						opts[k] = options[k];
				});
				if (self.protocol === 'https:') {
					return (mod_tls.connect(opts));
				} else {
					return (mod_net.createConnection(opts));
				}
			},
			maximum: this.maximum,
			spares: this.spares,
			log: this.log
		};
		if (this.cba_ping !== undefined) {
			poolOpts.checkTimeout = 30000;
			poolOpts.checker = this.checkSocket.bind(this, host);
		}
		this.pools[host] = new Pool(poolOpts);
	}
	var pool = this.pools[host];
	var waiter, sock, conn;

	req.once('abort', onAbort);

	waiter = pool.claim(function (err, connh, socket) {
		waiter = undefined;
		if (err) {
			req.emit('error', err);
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
	options.defaultPort = 80;
	CueBallAgent.call(this, options);
}
mod_util.inherits(CueBallHttpAgent, CueBallAgent);

function CueBallHttpsAgent(options) {
	options.protocol = 'https';
	options.defaultPort = 443;
	CueBallAgent.call(this, options);
}
mod_util.inherits(CueBallHttpsAgent, CueBallAgent);



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
