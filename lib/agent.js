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
	this.maximum = options.maximum || 16;
	this.spares = options.spares || 4;
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
		this.pools[host] = new Pool({
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
			arguments: [],
			log: this.log
		});
	}
	var pool = this.pools[host];
	pool.claim(function (err, conn, socket) {
		if (err) {
			req.emit('error', err);
			return;
		}
		function onFree() {
			socket.removeListener('agentRemove', onAgentRemove);
			conn.release();
		}
		function onAgentRemove() {
			socket.removeListener('free', onFree);
			conn.close();
		}
		socket.once('free', onFree);
		socket.once('agentRemove', onAgentRemove);
		req.onSocket(socket);
	});
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
