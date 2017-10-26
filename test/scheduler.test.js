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
const mod_jsprim = require('jsprim');
const mod_vasync = require('vasync');
const VError = require('verror');

const mod_pool = require('../lib/pool');
const mod_resolver = require('../lib/resolver');

const currentMillis = require('../lib/utils').currentMillis;
const fmt = mod_util.format;

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

function repeat(val, length) {
	var arr = new Array(length);
	for (var i = 0; i < length; ++i) {
		arr[i] = mod_jsprim.deepCopy(val);
	}
	return (arr);
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

/*
 * Run a test against the scheduler. There are several different parameters to
 * control how the test is run:
 *
 * - "queues", a description of weighted queues, and how long the test should
 *   hold checked out handles for things from that queue.
 * - "bound", how tight of a bound to enforce in the percentage of overall
 *   checkout time.
 * - "duration", how long to sustain load before validating results.
 * - "load", how many items to add to each queue every 10ms.
 */
function runSchedTest(queues, bound, duration, load, t) {
	mod_assert.array(queues, 'queues');
	mod_assert.number(bound, 'bound');
	mod_assert.number(duration, 'duration');
	mod_assert.number(load, 'load');
	mod_assert.object(t, 't');

	connections = [];
	resolver = undefined;

	var finished = false;
	var delays = repeat([], queues.length);
	var holdtime = repeat(0, queues.length);
	var successes = repeat(0, queues.length);
	var timeouts = repeat(0, queues.length);
	var failures = repeat(0, queues.length);
	var count = 0;
	var qsum = queues.reduce(function (acc, queue) {
		return (acc + queue.weight);
	}, 0);
	var barrier = mod_vasync.barrier();
	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 4,
		maximum: 4,
		queues: queues,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	function verifyQueue(n) {
		var prefix = 'queues[' + n + ']: ';
		var expHold = queues[n].weight / qsum;
		var actHold = holdtime[n] / sum(holdtime);
		t.ok(actHold < expHold + bound,
		    fmt('%s%d < %d + %d', prefix, actHold, expHold, bound));
		t.ok(actHold > expHold - bound,
		    fmt('%s%d > %d - %d', prefix, actHold, expHold, bound));
		t.equal(failures[n], 0, prefix + 'failures == 0');

		var targ = queues[n].targetClaimDelay;
		if (targ !== undefined) {
			var delavg = average(delays[n]);
			t.notEqual(timeouts[n], 0, prefix + 'timeouts != 0');
			t.ok(delavg < targ + 175,
			    fmt('%s%d < %d + 175', prefix, delavg, targ));
			t.ok(delavg > targ - 175,
			    fmt('%s%d > %d - 175', prefix, delavg, targ));
		}
	}

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 4);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');
		t.strictEqual(connections[2].backend, 'b1');
		t.strictEqual(connections[3].backend, 'b1');

		connections.forEach(function (c) {
			t.strictEqual(c.refd, true);
			c.connect();
		});

		setImmediate(queueClaims);

		function enqueue(n) {
			var id = 'claim-' + count++;
			var start = currentMillis();

			/*
			 * Calculate a random time to sleep such that
			 * the average should be ~testSleepTime.
			 */
			var sleep = queues[n].testSleepTime - 20 +
			    Math.random() * 20;

			barrier.start(id);
			pool.claim({
				queue: n
			}, function (err, hdl, conn) {
				var hstart = currentMillis();
				delays[n].push(hstart - start);

				function afterHold() {
					var elapsed = currentMillis() - hstart;
					holdtime[n] += elapsed;
					hdl.release(n, elapsed);
				}

				if (isClaimTimeoutError(err)) {
					timeouts[n] += 1;
				} else if (err) {
					t.ifError(err);
					failures[n] += 1;
				} else {
					successes[n] += 1;
					setTimeout(afterHold,
					    finished ? 0 : sleep);
				}

				barrier.done(id);
			});
		}

		function queueClaims() {
			var intvl = setInterval(function () {
				for (var i = 0; i < queues.length * load; ++i) {
				    enqueue(i % queues.length);
				}
			}, 10);

			setTimeout(function () {
				finished = true;
				clearInterval(intvl);

				for (var i = 0; i < queues.length; ++i) {
					verifyQueue(i);
				}

				barrier.start('draining');
				barrier.on('drain', onDrain);
				barrier.done('draining');
			}, duration);
		}

		function onDrain() {
			var pstats = pool.getStats();
			t.equal(sum(successes) + sum(timeouts) + sum(failures),
			    count, 'no pending claim callbacks');
			t.notEqual(sum(successes), 0, 'successes != 0');
			t.notEqual(sum(timeouts), pstats.counters['claim-timeouts'],
			    'timeouts == counters["claim-timeouts"]');
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

mod_tape.test('many equally weighted queues',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 }
], 0.01, 4000, 1));

mod_tape.test('equally weighted queues w/ varied sleep times',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime:  50 },
	{ weight: 1, testSleepTime:  75 },
	{ weight: 1, testSleepTime: 100 },
	{ weight: 1, testSleepTime: 125 }
], 0.01, 5000, 1));

mod_tape.test('single codel queue',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 250 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 }
], 0.01, 5000, 1));

mod_tape.test('single codel queue and a higher weighted queue',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 250 },
	{ weight: 3, testSleepTime: 50 }
], 0.01, 4000, 1));

mod_tape.test('multiple codel queues',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 250  },
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 500  },
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 750  },
	{ weight: 1, testSleepTime: 50, targetClaimDelay: 1000 }
], 0.01, 4000, 1));

/*
 * Because these tests use higher sleep periods with infrequently run queues,
 * we need to run them for longer in order to get a more accurate and correct
 * average. We also need to use a higher load in some of them to make sure the
 * higher weight doesn't help us completely clear out the first queue.
 */

mod_tape.test('heavily weighted queue',
    runSchedTest.bind(null, [
	{ weight: 7, testSleepTime:  50 },
	{ weight: 1, testSleepTime:  50 }
], 0.01, 5000, 2));

mod_tape.test('heavily weighted queue w/ varied sleep times (asc)',
    runSchedTest.bind(null, [
	{ weight: 7, testSleepTime:  50 },
	{ weight: 1, testSleepTime:  75 },
	{ weight: 1, testSleepTime: 100 },
	{ weight: 1, testSleepTime: 125 }
], 0.02, 15000, 3));

mod_tape.test('heavily weighted queue w/ varied sleep times (desc)',
    runSchedTest.bind(null, [
	{ weight: 7, testSleepTime: 125 },
	{ weight: 1, testSleepTime: 100 },
	{ weight: 1, testSleepTime:  75 },
	{ weight: 1, testSleepTime:  50 }
], 0.02, 10000, 1));

mod_tape.test('several heavily weighted queues',
    runSchedTest.bind(null, [
	{ weight: 1, testSleepTime: 50 },
	{ weight: 1, testSleepTime: 50 },
	{ weight: 2, testSleepTime: 50 },
	{ weight: 3, testSleepTime: 50 },
	{ weight: 5, testSleepTime: 50 },
	{ weight: 8, testSleepTime: 50 }
], 0.01, 10000, 3));

mod_tape.test('cleanup sandbox', function (t) {
	sandbox.restore();
	t.end();
});
