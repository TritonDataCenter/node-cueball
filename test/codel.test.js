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
const mod_vasync = require('vasync');
const VError = require('verror');

const mod_pool = require('../lib/pool');
const mod_resolver = require('../lib/resolver');

var sandbox;
var connections = [];
var index, counts;
var resolver;
var log = mod_bunyan.createLogger({
	name: 'codel-test',
	level: process.env.LOGLEVEL || 'debug'
});
var recovery = {
	default: { timeout: 500, retries: 1, delay: 0 }
};

function isClaimTimeoutError(err) {
	if (!err) {
		return (false);
	}

	return (VError.hasCauseWithName(err, 'ClaimTimeoutError'));
}

function sumStep(acc, cur) {
	return (acc + cur);
}

function sum(arr) {
	return (arr.reduce(sumStep, 0));
}

function average(arr) {
	return (sum(arr) / arr.length);
}

function DummyResolver() {
	resolver = this;
	this.state = 'stopped';
	this.backends = {};
	mod_events.EventEmitter.call(this);
	this.on('added', function (key) {
		resolver.backends[key] = true;
	});
	this.on('removed', function (key) {
		delete (resolver.backends[key]);
	});
	return (new mod_resolver.ResolverFSM(this, {}));
}
mod_util.inherits(DummyResolver, mod_events.EventEmitter);
DummyResolver.prototype.start = function () {
	this.state = 'running';
};
DummyResolver.prototype.stop = function () {
	this.state = 'stopped';
};
DummyResolver.prototype.count = function () {
	return (Object.keys(this.backends).length);
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

mod_tape.test('setup sandbox', function (t) {
	sandbox = mod_sinon.sandbox.create();
	sandbox.stub(mod_resolver, 'Resolver', DummyResolver);
	t.end();
});

mod_tape.test('implicit high timeout', function (t) {
	connections = [];
	resolver = undefined;

	var delay = 100;
	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 2,
		targetClaimDelay: delay,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: {
			'default': {
				timeout: delay * 11,
				retries: 1,
				delay: 0
			}
		}
	});
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		/* The connections haven't emitted connect() yet. */
		pool.claim(function (err) {
			t.ok(err);
			t.ok(err.message.match(/timed out/i));

			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
				c.connect();
			});

			setImmediate(claimAgain);
		});

		/* Connections now exist, and pool is usable. */
		function claimAgain() {
			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
			});

			pool.claim(function (err, hdl, conn) {
				t.ifError(err);
				t.ok(hdl);
				t.ok(conn);
				hdl.release();
				setImmediate(done);
			});
		}

		function done() {
			pool.on('stateChanged', function (st) {
				if (st === 'stopped')
					t.end();
			});
			pool.stop();
		}
	});
});

/*
 * These tests generate the same load pattern multiple times to show that
 * as the target delays moves up, so does the average delay.
 */
function runCodelTest(delay, t) {
	connections = [];
	resolver = undefined;

	var delays = [];
	var successes = 0;
	var timeouts = 0;
	var failures = 0;
	var count = 0;
	var barrier = mod_vasync.barrier();
	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 2,
		targetClaimDelay: delay,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		connections.forEach(function (c) {
			t.strictEqual(c.refd, true);
			c.connect();
		});

		setImmediate(queueClaims);

		function enqueue() {
			var id = 'claim-' + count++;
			var start = Date.now();
			barrier.start(id);
			pool.claim(function (err, hdl, conn) {
				delays.push(Date.now() - start);

				if (isClaimTimeoutError(err)) {
					timeouts += 1;
				} else if (err) {
					t.ifError(err);
					failures += 1;
				} else {
					successes += 1;
					setTimeout(function () {
					    hdl.release();
					}, 50);
				}

				barrier.done(id);
			});
		}

		function queueClaims() {
			var intvl = setInterval(function () {
				for (var i = 0; i < 5; ++i) {
				    enqueue();
				}
			}, 10);

			setTimeout(function () {
				clearInterval(intvl);
				barrier.start('draining');
				barrier.on('drain', onDrain);
				barrier.done('draining');
			}, 5000);
		}

		function onDrain() {
			var delavg = average(delays);
			var pstats = pool.getStats();
			t.ok(delavg < delay + 175, 'avg(delay) < target + 175');
			t.ok(delavg > delay - 175, 'avg(delay) > target - 175');
			t.equal(successes + timeouts + failures, count,
			    'no pending claim callbacks');
			t.notEqual(successes, 0, 'successes != 0');
			t.notEqual(timeouts, 0, 'timeouts != 0');
			t.notEqual(timeouts, pstats.counters['claim-timeouts'],
			    'timeouts == counters["claim-timeouts"]');
			t.equal(failures, 0, 'failures == 0');
			setImmediate(done);
		}

		function done() {
			pool.on('stateChanged', function (st) {
				if (st === 'stopped')
					t.end();
			});
			pool.stop();
		}
	});
}

mod_tape.test('delay target: 300ms', runCodelTest.bind(null, 300));

mod_tape.test('delay target: 500ms', runCodelTest.bind(null, 500));

mod_tape.test('delay target: 1000ms', runCodelTest.bind(null, 1000));

mod_tape.test('delay target: 1500ms', runCodelTest.bind(null, 1500));

mod_tape.test('delay target: 2000ms', runCodelTest.bind(null, 2000));

mod_tape.test('delay target: 2500ms', runCodelTest.bind(null, 2500));

mod_tape.test('delay target: 5000ms', runCodelTest.bind(null, 5000));

mod_tape.test('cleanup sandbox', function (t) {
	sandbox.restore();
	t.end();
});
