/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2017, Joyent, Inc.
 */

const mod_tape = require('tape');
const mod_sinon = require('sinon');
const mod_events = require('events');
const mod_util = require('util');
const mod_assert = require('assert-plus');
const mod_bunyan = require('bunyan');

const mod_cueball = require('..');
const mod_resolver = require('../lib/resolver');

const mod_kang = require('kang');
const mod_restify = require('restify');

var sandbox;
var connections = [];
var index, counts;
var resolver;
var log = mod_bunyan.createLogger({
	name: 'monitor-test',
	level: process.env.LOGLEVEL || 'debug'
});
var recovery = {
	default: {timeout: 500, retries: 1, delay: 0 }
};

function summarize() {
	index = {};
	counts = {};
	connections.forEach(function (c) {
		if (index[c.backend] === undefined) {
			index[c.backend] = [];
			counts[c.backend] = 0;
		}
		index[c.backend].push(c);
		++counts[c.backend];
	});
}

function DummyResolver() {
	resolver = this;
	this.state = 'stopped';
	this.backends = {};
	this.r_fsm = this;
	this.r_domain = 'resolverdomain';
	mod_events.EventEmitter.call(this);
	this.on('added', function (key, backend) {
		resolver.backends[key] = backend;
	});
	this.on('removed', function (key) {
		delete (resolver.backends[key]);
	});
	return (new mod_resolver.ResolverFSM(this, {}));
}
mod_util.inherits(DummyResolver, mod_events.EventEmitter);
DummyResolver.prototype.start = function () {
	this.emit('stateChanged', 'running');
	this.state = 'running';
};
DummyResolver.prototype.stop = function () {
	this.state = 'stopped';
	this.emit('stateChanged', 'stopped');
};
DummyResolver.prototype.count = function () {
	return (Object.keys(this.backends).length);
};
DummyResolver.prototype.list = function () {
	return (this.backends);
};
DummyResolver.prototype.isInState = function (st) {
	return (this.state === st);
};

function DummyConnection(backend) {
	connections.push(this);
	this.backend = backend.key;
	this.backendInfo = backend;
	this.refd = true;
	this.connected = false;
	this.dead = false;
	this.checked = false;
	mod_events.EventEmitter.call(this);
}
mod_util.inherits(DummyConnection, mod_events.EventEmitter);
DummyConnection.prototype.connect = function () {
	mod_assert.ok(this.dead === false);
	mod_assert.ok(this.connected === false);
	this.connected = true;
	this.emit('connect');
};
DummyConnection.prototype.unref = function () {
	this.refd = false;
};
DummyConnection.prototype.ref = function () {
	this.refd = true;
};
DummyConnection.prototype.destroy = function () {
	var idx = connections.indexOf(this);
	if (idx !== -1)
		connections.splice(idx, 1);
	this.connected = false;
	this.dead = true;
};

var pool, cset, client, kangServer;

mod_tape.test('setup sandbox', function (t) {
	sandbox = mod_sinon.sandbox.create();
	sandbox.stub(mod_resolver, 'Resolver', DummyResolver);
	t.end();
});

mod_tape.test('set up kang monitor', function (t) {
	var kangOpts = mod_cueball.poolMonitor.toKangOptions();
	kangOpts.log = log;
	kangOpts.port = 9090;

	kangServer = mod_restify.createServer({
		serverName: 'Kang',
		handleUncaughtExceptions: false
	});
	kangServer.get(new RegExp('.*'), mod_kang.knRestifyHandler(kangOpts));
	kangServer.listen(kangOpts.port, '127.0.0.1', function () {
		log.info('cueball kang monitor started on port %d',
		    kangOpts.port);
	});
	kangServer.on('after', mod_restify.auditLogger({
		log: log
	}));

	client = mod_restify.createJsonClient({
		url: 'http://localhost:' + kangOpts.port
	});

	t.end();
});

mod_tape.test('empty kang info', function (t) {
	client.get('/kang/snapshot', function (err, req, res, obj) {
		t.ifError(err);
		t.strictEqual(obj.service.name, 'cueball');
		t.deepEqual(obj.types, ['pool', 'set']);
		t.deepEqual(Object.keys(obj.pool), []);
		t.deepEqual(Object.keys(obj.set), []);
		t.end();
	});
});

mod_tape.test('set up a pool', function (t) {
	pool = new mod_cueball.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 2,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);
	resolver.start();

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		client.get('/kang/snapshot', function (err, req, res, obj) {
			t.ifError(err);
			t.strictEqual(obj.service.name, 'cueball');
			t.deepEqual(Object.keys(obj.pool), [pool.p_uuid]);
			t.deepEqual(Object.keys(obj.set), []);

			var pinf = obj.pool[pool.p_uuid];
			t.deepEqual(Object.keys(pinf.backends), ['b1']);
			t.deepEqual(pinf.connections, {
				'b1': { 'connecting': 2 }
			});
			t.strictEqual(pinf.state, 'starting');
			t.strictEqual(pinf.options.domain, 'resolverdomain');

			connectOne();
		});

		function connectOne() {
			connections[0].connect();
			setTimeout(checkForOneIdle, 100);
		}

		function checkForOneIdle() {
			client.get('/kang/snapshot',
			    function (err, req, res, obj) {
				t.ifError(err);
				t.strictEqual(obj.service.name, 'cueball');

				var pinf = obj.pool[pool.p_uuid];
				t.deepEqual(Object.keys(pinf.backends), ['b1']);
				t.deepEqual(pinf.connections, {
					'b1': {
						'idle': 1,
						'connecting': 1
					}
				});
				t.strictEqual(pinf.state, 'running');

				connectAnother();
			});
		}

		function connectAnother() {
			connections[1].connect();
			t.end();
		}
	});
});

mod_tape.test('set up a cset', function (t) {
	connections = [];

	cset = new mod_cueball.ConnectionSet({
		log: log,
		domain: 'foobar',
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 2,
		maximum: 4,
		resolver: resolver
	});

	cset.on('stateChanged', function (st) {
		if (st === 'running')
			setTimeout(afterRunning, 200);
	});

	cset.on('added', function (key, conn, hdl) {
		t.notStrictEqual(connections.indexOf(conn), -1);
		t.strictEqual(conn.refd, true);
	});

	cset.on('removed', function (key, conn, hdl) {
		if (!cset.isInState('stopping'))
			t.fail('removed ' + key);
		hdl.release();
	});

	setTimeout(function () {
		connections.forEach(function (c) {
			c.connect();
		});
	}, 100);

	function afterRunning() {
		client.get('/kang/snapshot', function (err, req, res, obj) {
			t.ifError(err);
			t.strictEqual(obj.service.name, 'cueball');
			t.deepEqual(Object.keys(obj.set), [cset.cs_uuid]);

			var sinf = obj.set[cset.cs_uuid];
			t.deepEqual(Object.keys(sinf.backends), ['b1']);
			t.deepEqual(sinf.fsms, {
				'b1': { 'busy': 1 }
			});
			t.strictEqual(sinf.state, 'running');
			t.strictEqual(sinf.options.domain, 'resolverdomain');

			t.end();
		});
	}
});

mod_tape.test('shut down and clean up', function (t) {
	kangServer.close();
	client.close();
	pool.stop();
	resolver.stop();
	cset.stop();
	sandbox.restore();
	t.end();
});
