/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_tape = require('tape');
const mod_sinon = require('sinon');
const mod_events = require('events');
const mod_util = require('util');
const mod_assert = require('assert-plus');
const mod_bunyan = require('bunyan');

const mod_cset = require('../lib/set');
const mod_resolver = require('../lib/resolver');

var connections = [];
var resolver;
var log = mod_bunyan.createLogger({
	name: 'pool-test',
	level: process.env.LOGLEVEL || 'debug'
});
var recovery = {
	default: {timeout: 1000, retries: 2, delay: 100 }
};

var index, counts;

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
	mod_events.EventEmitter.call(this);
	return (new mod_resolver.ResolverFSM(this, {}));
}
mod_util.inherits(DummyResolver, mod_events.EventEmitter);
DummyResolver.prototype.start = function () {
	this.state = 'running';
};
DummyResolver.prototype.stop = function () {
	this.state = 'stopped';
};

function DummyConnection(backend) {
	connections.push(this);
	this.backend = backend.key;
	this.backendInfo = backend;
	this.refd = true;
	this.connected = false;
	this.dead = false;
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
	if (this.dead)
		return;
	var idx = connections.indexOf(this);
	mod_assert.ok(idx !== -1);
	connections.splice(idx, 1);
	this.connected = false;
	this.dead = true;
	this.emit('close');
};

mod_tape.test('cset with one backend', function (t) {
	connections = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 2,
		maximum: 4,
		resolver: resolver
	});

	cset.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});

	cset.on('added', function (key, conn, hdl) {
		t.notStrictEqual(connections.indexOf(conn), -1);
		t.strictEqual(conn.refd, true);
		if (connections.length > 1) {
			t.fail('more than 2 connections');
		}
		if (connections.length === 1) {
			setImmediate(function () {
				cset.stop();
				resolver.stop();
			});
		}
	});

	cset.on('removed', function (key, conn, hdl) {
		if (!cset.isInState('stopping'))
			t.fail('removed ' + key);
		hdl.release();
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});

	setTimeout(function () {
		connections.forEach(function (c) { c.connect(); });
	}, 100);

	setTimeout(function () {}, 5000);
});

mod_tape.test('cset with two backends', function (t) {
	connections = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 2,
		maximum: 4,
		resolver: resolver
	});

	cset.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});

	cset.on('added', function (key, conn, hdl) {
		t.notStrictEqual(connections.indexOf(conn), -1);
		t.strictEqual(conn.refd, true);
		if (connections.length > 2) {
			t.fail('more than 2 connections');
		}
		if (connections.length === 2) {
			var backends = connections.map(function (c) {
				return (c.backend);
			}).sort();
			t.deepEqual(backends, ['b1', 'b2']);
			setImmediate(function () {
				cset.stop();
				resolver.stop();
			});
		}
	});

	cset.on('removed', function (key, conn, hdl) {
		if (!cset.isInState('stopping'))
			t.fail();
		hdl.release();
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});

	setImmediate(function () {
		connections.forEach(function (c) { c.connect(); });
	});
});

mod_tape.test('cset swapping', function (t) {
	connections = [];
	var inset = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 1,
		maximum: 1,
		resolver: resolver
	});

	cset.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});

	cset.on('added', function (key, conn) {
		inset.push(conn);
	});

	cset.on('removed', function (key, conn, hdl) {
		t.ok(!conn.dead);
		conn.seen = true;
		hdl.release();
		var idx = inset.indexOf(conn);
		if (idx !== -1)
			inset.splice(idx, 1);
	});

	resolver.emit('added', 'b1', {});

	setImmediate(function () {
		t.equal(connections.length, 1);
		summarize();
		t.deepEqual(counts, { 'b1': 1 });
		index.b1[0].connect();

		setTimeout(function () {
			t.equal(connections.length, 1);
			summarize();
			t.deepEqual(counts, { 'b1': 1 });
			t.equal(inset.length, 1);

			var conn = index.b1[0];

			resolver.emit('added', 'b0', {});
			cset.cs_keys.sort();
			t.strictEqual(cset.cs_keys[0], 'b0');

			t.ok(!conn.dead);
			t.ok(!conn.seen);

			setTimeout(function () {
				t.equal(connections.length, 2);
				t.equal(inset.length, 1);
				summarize();
				t.deepEqual(counts, { 'b1': 1, 'b0': 1 });
				t.ok(!conn.dead);
				t.ok(!conn.seen);
				index.b0[0].connect();

				setTimeout(function () {
					t.equal(connections.length, 1);
					t.equal(inset.length, 1);
					t.strictEqual(inset[0], index.b0[0]);
					summarize();
					t.deepEqual(counts, { 'b0': 1 });
					t.ok(conn.dead);
					cset.stop();
					resolver.stop();
				}, 1000);
			}, 500);
		}, 500);
	});
});

mod_tape.test('removing a backend', function (t) {
	connections = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 3,
		maximum: 5,
		resolver: resolver
	});

	var stopTimer;
	cset.on('stateChanged', function (st) {
		if (st === 'stopped') {
			if (stopTimer !== undefined)
				clearTimeout(stopTimer);
			t.end();
		}
	});

	cset.on('added', function (key, conn) {
	});

	cset.on('removed', function (key, conn, hdl) {
		conn.seen = true;
		hdl.release();
	});

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});
	resolver.emit('added', 'b3', {});

	setImmediate(function () {
		t.equal(connections.length, 3);
		summarize();
		t.deepEqual(counts, { 'b1': 1, 'b2': 1, 'b3': 1 });
		index.b1[0].connect();
		index.b2[0].connect();

		setTimeout(function () {
			t.equal(connections.length, 3);
			summarize();
			t.deepEqual(counts, { 'b1': 1, 'b2': 1, 'b3': 1 });

			var conn = index.b2[0];
			var conn2 = index.b3[0];

			resolver.emit('removed', 'b2');
			resolver.emit('removed', 'b3');

			setTimeout(function () {
				t.ok(conn.dead);
				t.ok(conn2.dead);
				t.ok(conn.seen);
				t.ok(!conn2.seen);
				t.equal(connections.length, 1);
				summarize();
				t.deepEqual(counts, { 'b1': 1 });
				cset.stop();
				resolver.stop();
				stopTimer = setTimeout(function () {}, 5000);
			}, 500);
		}, 500);
	});
});

mod_tape.test('removing an unused backend (cueball#47)', function (t) {
	connections = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 2,
		maximum: 5,
		resolver: resolver
	});

	var stopTimer;
	cset.on('stateChanged', function (st) {
		if (st === 'stopped') {
			if (stopTimer !== undefined)
				clearTimeout(stopTimer);
			t.end();
		}
	});

	cset.on('added', function (key, conn) {
	});

	cset.on('removed', function (key, conn, hdl) {
		conn.seen = true;
		hdl.release();
	});

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});
	resolver.emit('added', 'b3', {});
	var bkeys = ['b1', 'b2', 'b3'];

	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		var bs = bkeys.filter(function (k) {
			return (counts[k] > 0);
		});
		var nbs = bkeys.filter(function (k) {
			return (counts[k] === undefined || counts[k] === 0);
		});
		t.equal(bs.length, 2);
		index[bs[0]][0].connect();
		index[bs[1]][0].connect();

		resolver.emit('removed', nbs[0]);

		setTimeout(function () {
			t.equal(connections.length, 2);
			summarize();
			t.equal(counts[bs[0]], 1);
			t.equal(counts[bs[1]], 1);
			t.strictEqual(counts[nbs[0]], undefined);

			cset.stop();
			resolver.stop();

			stopTimer = setTimeout(function () { }, 5000);
		}, 500);
	});
});

mod_tape.test('cset with error', function (t) {
	connections = [];
	resolver = new DummyResolver();

	recovery.default.retries = 1;
	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		target: 2,
		maximum: 4,
		resolver: resolver
	});

	var stopTimer;
	cset.on('stateChanged', function (st) {
		if (st === 'stopped') {
			t.ok(errorKey === undefined);
			if (stopTimer !== undefined)
				clearTimeout(stopTimer);
			t.end();
		}
	});

	var errorKey;
	function silentErrHandler(err) {}
	function failErrHandler(err) {
		t.error(err);
	}
	cset.on('added', function (key, conn) {
		t.notStrictEqual(connections.indexOf(conn), -1);
		t.strictEqual(conn.refd, true);
		if (connections.length > 2) {
			t.fail('more than 2 connections');
		}
		if (connections.length === 2) {
			var backends = connections.map(function (c) {
				return (c.backend);
			}).sort();
			t.deepEqual(backends, ['b1', 'b2']);

			conn.on('error', silentErrHandler);
			errorKey = key;
			conn.emit('error', new Error());
		} else {
			conn.on('error', failErrHandler);
		}
	});

	cset.on('removed', function (key, conn, hdl) {
		conn.removeListener('error', silentErrHandler);
		conn.removeListener('error', failErrHandler);
		hdl.release();
		if (key === errorKey) {
			errorKey = undefined;
			t.ok(conn.dead);

			cset.stop();
			resolver.stop();
			stopTimer = setTimeout(function () {}, 5000);
			return;
		}
		if (!cset.isInState('stopping'))
			t.fail('removed connection ' + key + ' unexpectedly');
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});

	setImmediate(function () {
		connections.forEach(function (c) { c.connect(); });
	});
});

mod_tape.test('cset connect-reject (#92)', function (t) {
	connections = [];
	var inset = [];
	resolver = new DummyResolver();

	var cset = new mod_cset.ConnectionSet({
		log: log,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: {
			default: {
				timeout: 1000,
				retries: 0,
				delay: 0
			}
		},
		target: 2,
		maximum: 4,
		resolver: resolver
	});

	cset.on('stateChanged', function (st) {
		if (st === 'failed')
			cset.stop();
		if (st === 'stopped') {
			t.deepEqual(inset, []);
			t.end();
		}
	});

	cset.on('added', function (key, conn) {
		inset.push(key);
	});

	cset.on('removed', function (key, conn, hdl) {
		var idx = inset.indexOf(key);
		t.notStrictEqual(idx, -1);
		inset.splice(idx, 1);

		t.ok(conn);
		t.ok(hdl);
		t.ok(conn.dead);
		conn.seen = true;
		hdl.release();
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});

	setImmediate(function () {
		connections.forEach(function (c) {
			c.connect();
			setImmediate(function () {
				c.destroy();
			});
		});
	});
});
