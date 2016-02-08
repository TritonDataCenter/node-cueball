/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	Agent: CueBallAgent
};

const EventEmitter = require('events').EventEmitter;
const mod_net = require('net');
const mod_util = require('util');
const mod_pool = require('./pool');
const mod_assert = require('assert-plus');

const Pool = mod_pool.ConnectionPool;

function CueBallAgent(options) {
	mod_assert.object(options, 'options');
	mod_assert.arrayOfString(options.resolvers, 'options.resolvers');
	mod_assert.optionalObject(options.log, 'options.log');

	EventEmitter.call(this);

	/* These are used as defaults in the ClientRequest constructor. */
	this.defaultPort = 80;
	this.protocol = 'http:';

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

/*
 * Sets up a duplex stream to be used for the given HTTP request.
 * Calls req.onSocket(sock) with said stream once it is ready.
 *
 * When the request has finished with the socket, it forces the socket to
 * emit either "free" or "agentRemove", depending on whether the request
 * believes the socket to be reuseable or not.
 */
CueBallAgent.prototype.addRequest = function (req, options) {
	var host = options.host || options.hostname;
	if (this.pools[host] === undefined) {
		this.pools[host] = new Pool({
			defaultPort: 80,
			resolvers: this.resolvers,
			domain: host,
			constructor: function (backend) {
				var opts = {
					host: backend.address || backend.name,
					port: backend.port || 80
				};
				return (mod_net.createConnection(opts));
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
