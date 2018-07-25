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

const mod_pool = require('../lib/pool');
const mod_resolver = require('../lib/resolver');

var sandbox;
var connections = [];
var index, counts;
var resolver;
var log = mod_bunyan.createLogger({
	name: 'pool-test',
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

mod_tape.test('empty pool', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery,
		spares: 2,
		maximum: 4
	});
	t.ok(resolver);
	t.strictEqual(resolver.state, 'running');
	t.strictEqual(connections.length, 0);

	pool.claim({errorOnEmpty: true}, function (err) {
		t.ok(err);
		t.ok(err.message.match(/no backend/i));

		pool.claim({timeout: 100}, function (err2) {
			t.ok(err2);
			t.ok(err2.message.match(/timed out/i));
			t.end();
		});
	});
});

mod_tape.test('pool with one backend', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
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

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		/* The connections haven't emitted connect() yet. */
		pool.claim({timeout: 100}, function (err) {
			t.ok(err);
			t.ok(err.message.match(/timed out/i));

			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
				c.connect();
			});

			setImmediate(claimAgain);
		});

		function claimAgain() {
			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
			});

			pool.claim({timeout: 100}, function (err, hdl, conn) {
				t.error(err);
				t.ok(hdl);
				t.notStrictEqual(connections.indexOf(conn), -1);
				t.strictEqual(conn.refd, true);

				claimOnceMore();
			});
		}

		function claimOnceMore() {
			pool.claim({timeout: 100}, function (err, hdl, conn) {
				t.error(err);
				t.ok(hdl);
				t.notStrictEqual(connections.indexOf(conn), -1);
				claimEmpty();
			});
		}

		function claimEmpty() {
			pool.claim({timeout: 100}, function (err) {
				t.ok(err);
				t.ok(err.message.match(/timed out/i));
				t.end();
			});
		}
	});
});

mod_tape.test('async claim can expand up to max', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 0,
		maximum: 2,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});
	setImmediate(function () {
		t.equal(connections.length, 0);

		pool.claim(function (err, handle, conn) {
			t.error(err);
			var b1 = conn.backend;
			t.ok(['b1', 'b2'].indexOf(b1) !== -1);

			pool.claim(function (err2, handle2, conn2) {
				t.error(err2);
				var b2 = conn2.backend;
				t.ok(['b1', 'b2'].indexOf(b2) !== -1);
				t.notStrictEqual(b1, b2);

				pool.claim({timeout: 100}, function (err3) {
					t.ok(err3);
					t.end();
				});
			});

			setTimeout(function () {
				t.equal(connections.length, 2);
				connections[1].connect();
			}, 50);
		});

		setTimeout(function () {
			t.equal(connections.length, 1);
			connections[0].connect();
		}, 50);
	});
});

mod_tape.test('spares are evenly balanced', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 4,
		maximum: 4,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(pool);
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});
	setImmediate(function () {
		connections.forEach(function (c) { c.connect(); });

		t.equal(connections.length, 4);
		var bs = connections.map(function (c) { return (c.backend); });
		t.deepEqual(bs.sort(), ['b1', 'b1', 'b2', 'b2']);

		resolver.emit('added', 'b3', {});
		resolver.emit('added', 'b4', {});

		setTimeout(function () {
			connections.forEach(function (c) {
				if (!c.connected)
					c.connect();
			});

			t.equal(connections.length, 4);
			var bs2 = connections.map(
			    function (c) { return (c.backend); });
			t.deepEqual(bs2.sort(), ['b1', 'b2', 'b3', 'b4']);

			t.end();
		}, 50);
	});
});

mod_tape.test('error while claimed', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 1,
		maximum: 1,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 1);
		connections[0].connect();

		pool.claim(function errorWhileClaimed(err, handle, conn) {
			t.strictEqual(conn, connections[0]);
			conn.once('error', function () {
				/* do nothing */
			});
			conn.emit('error', new Error('testing'));
			handle.release();

			setTimeout(function () {
				t.ok(conn.dead);
				t.equal(connections.length, 1);
				connections[0].connect();

				t.end();
			}, 500);
		});
	});
});

mod_tape.test('close while idle', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 1,
		maximum: 1,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 1);
		var conn = connections[0];
		conn.connect();

		setTimeout(function () {
			conn.emit('close');

			setImmediate(function () {
				t.ok(conn.dead);
				t.equal(connections.length, 1);
				t.notStrictEqual(conn, connections[0]);
				t.ok(!connections[0].dead);
				connections[0].connect();

				t.strictEqual(conn.sm_fsm.fsm_history.
				    indexOf('backoff'), -1);
				pool.stop();
				t.end();
			});
		}, 100);
	});
});

mod_tape.test('removing a backend', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 3,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		recovery: recovery
	});
	t.ok(resolver);

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});

	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 1, 'b2': 1 });
		index.b1[0].connect();

		/* Get it to be declared dead. */
		index.b2[0].emit('error', new Error());

		setTimeout(function () {
			t.equal(Object.keys(pool.p_dead).length, 1);

			/*
			 * The backed-off monitor FSM should be there now, in
			 * addition to the extra one on b1.
			 */
			t.equal(connections.length, 3);
			summarize();
			t.deepEqual(counts, { 'b1': 2, 'b2': 1 });

			index.b1[1].connect();
			var conn = index.b2[0];

			resolver.emit('removed', 'b2');

			setTimeout(function () {
				summarize();
				if (counts.b2 > 0)
					index.b2[0].emit('error', new Error());
			}, 800);

			setTimeout(function () {
				t.ok(conn.dead);
				t.equal(connections.length, 2);
				summarize();
				t.deepEqual(counts, { 'b1': 2 });

				pool.stop();
			}, 1000);
		}, 400);
	});
});

mod_tape.test('pool failure', function (t) {
	connections = [];
	resolver = undefined;
	var timer;

	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			if (timer !== undefined)
				clearTimeout(timer);
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 2 });

		index.b1[0].connect();
		index.b1[0].emit('error', new Error());
		index.b1[1].connect();
		index.b1[1].emit('error', new Error());

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			index.b1[1].connect();
			index.b1[1].emit('error', new Error());
			index.b1[0].connect();

			setTimeout(function () {
				t.ok(pool.isInState('running'));

				t.equal(connections.length, 2);
				summarize();
				index.b1[0].emit('error', new Error('test'));
				index.b1[1].emit('error', new Error('test'));

				var sawErr = false;
				pool.claim(function (err) {
					t.ok(err);
					t.strictEqual(err.name,
					    'PoolFailedError');
					sawErr = true;
				});

				setTimeout(function () {
					t.ok(pool.isInState('failed'));
					t.ok(sawErr);
					t.notStrictEqual(pool.getLastError(),
					    undefined);
					t.strictEqual(
					    pool.getLastError().message,
					    'test');

					t.equal(connections.length, 1);
					summarize();
					t.deepEqual(counts, { 'b1': 1 });

					index.b1[0].connect();

					setTimeout(function () {
						t.ok(pool.isInState('running'));
						pool.stop();

						/* Stop tape from giving up. */
						timer = setTimeout(
						    function () {}, 5000);
					}, 100);
				}, 100);
			}, 100);
		}, 100);
	});
});

mod_tape.test('pool failure / retry race', function (t) {
	connections = [];
	resolver = undefined;

	var timer;
	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			if (timer !== undefined)
				clearTimeout(timer);
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 2 });

		index.b1[0].connect();
		index.b1[0].emit('error', new Error('test'));
		index.b1[1].connect();
		index.b1[1].emit('error', new Error('test'));

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			index.b1[1].connect();
			index.b1[1].emit('error', new Error('test'));
			index.b1[0].connect();
			index.b1[0].emit('error', new Error('test'));

			setTimeout(function () {
				t.ok(pool.isInState('running'));
				t.strictEqual(pool.getLastError(), undefined);

				t.equal(connections.length, 2);
				summarize();
				index.b1[1].emit('error', new Error('test2'));
				index.b1[0].connect();

				setTimeout(function () {
					t.ok(pool.isInState('running'));

					t.equal(connections.length, 2);
					summarize();
					t.deepEqual(counts, { 'b1': 2 });

					pool.stop();
					/* Stop tape from giving up. */
					timer = setTimeout(function () {},
					    5000);
				}, 100);
			}, 100);
		}, 100);
	});
});

mod_tape.test('pool ping checker', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 2,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		checkTimeout: 100,
		checker: doCheck,
		recovery: recovery
	});
	t.ok(resolver);

	pool.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});

	function doCheck(hdl, conn) {
		conn.checked = true;
		hdl.release();
	}

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		connections.forEach(function (c) {
			t.strictEqual(c.refd, true);
			c.connect();
		});

		pool.claim(function (err, hdl, conn) {
			t.error(err);
			t.strictEqual(conn.checked, false);

			setTimeout(function () {
				verifyCheck();
				hdl.release();
			}, 1000);
		});

		function verifyCheck() {
			var cs = connections.map(function (c) {
				return (c.checked);
			});
			cs.sort();
			t.deepEqual(cs, [false, true]);

			pool.stop();
		}
	});
});

mod_tape.test('pool ping checker no expand', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
		log: log,
		domain: 'foobar',
		spares: 2,
		maximum: 10,
		constructor: function (backend) {
			return (new DummyConnection(backend));
		},
		checkTimeout: 100,
		checker: doCheck,
		recovery: recovery
	});
	t.ok(resolver);

	pool.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});

	function doCheck(hdl, conn) {
		conn.checked = true;
		hdl.release();
	}

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		connections.forEach(function (c) {
			t.strictEqual(c.refd, true);
			c.connect();
		});

		setTimeout(verifyCheck, 300);

		function verifyCheck() {
			var cs = connections.map(function (c) {
				return (c.checked);
			});
			cs.sort();
			t.deepEqual(cs, [true, true]);

			pool.stop();
		}
	});
});

mod_tape.test('claim cancellation', function (t) {
	connections = [];
	resolver = undefined;

	var pool = new mod_pool.ConnectionPool({
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

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		t.strictEqual(connections[0].backend, 'b1');
		t.strictEqual(connections[1].backend, 'b1');

		/* The connections haven't emitted connect() yet. */
		pool.claim({timeout: 100}, function (err) {
			t.ok(err);
			t.ok(err.message.match(/timed out/i));

			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
				c.connect();
			});

			setImmediate(claimAgain);
		});

		function claimAgain() {
			connections.forEach(function (c) {
				t.strictEqual(c.refd, true);
			});

			var claim = pool.claim({timeout: 100},
			    function (err, hdl, conn) {
				t.fail();
			});
			claim.cancel();

			setTimeout(done, 150);
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

mod_tape.test('cueball#108', function (t) {
	connections = [];
	resolver = undefined;

	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 2 });

		index.b1[0].connect();
		index.b1[1].connect();

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			t.deepEqual(counts, { 'b1': 2 });

			pool.claim(function (err, hdl, conn) {
				t.ifError(err);

				setTimeout(doFail, 100);

				function doFail() {
					hdl.close();
					conn.emit('close');
					setTimeout(end, 100);
				}

				function end() {
					pool.stop();
				}
			});
		}, 100);
	});
});

mod_tape.test('cueball#111', function (t) {
	connections = [];
	resolver = undefined;

	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 2 });

		index.b1[0].connect();
		index.b1[1].connect();

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			t.deepEqual(counts, { 'b1': 2 });

			pool.claim(function (err, hdl, conn) {
				t.ifError(err);

				setTimeout(doFail, 100);

				function doFail() {
					hdl.close();
					conn.emit('error', new Error('Foo'));
					setTimeout(end, 100);
				}

				function end() {
					pool.stop();
				}
			});
		}, 100);
	});
});

mod_tape.test('cueball#132 getStats()', function (t) {
	var s = null;

	connections = [];
	resolver = undefined;

	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			t.end();
		}
	});

	s = pool.getStats();

	t.equal(typeof (s), 'object');
	t.equal(Object.keys(s).length, 5);
	t.equal(typeof (s['counters']), 'object');
	t.equal(s['totalConnections'], 0);
	t.equal(s['idleConnections'], 0);
	t.equal(s['pendingConnections'], 0);
	t.equal(s['waiterCount'], 0);

	resolver.emit('added', 'b1', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 2 });

		index.b1[0].connect();
		index.b1[1].connect();

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			t.deepEqual(counts, { 'b1': 2 });

			s = pool.getStats();

			t.equal(typeof (s), 'object');
			t.equal(Object.keys(s).length, 5);
			t.equal(typeof (s['counters']), 'object');
			t.equal(s['totalConnections'], 2);
			t.equal(s['idleConnections'], 2);
			t.equal(s['pendingConnections'], 0);
			t.equal(s['waiterCount'], 0);

			pool.stop();
		}, 100);
	});
});

mod_tape.test('backend failure/removal race (#144)', function (t) {
	connections = [];
	resolver = undefined;

	var timer;
	recovery.default.retries = 2;
	var pool = new mod_pool.ConnectionPool({
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

	pool.on('stateChanged', function (st) {
		if (st === 'stopped') {
			if (timer !== undefined)
				clearTimeout(timer);
			t.end();
		}
	});

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});
	setImmediate(function () {
		t.equal(connections.length, 2);
		summarize();
		t.deepEqual(counts, { 'b1': 1, 'b2': 1 });

		index.b1[0].connect();
		index.b2[0].connect();

		setTimeout(function () {
			t.ok(pool.isInState('running'));

			t.equal(connections.length, 2);
			summarize();
			index.b1[0].emit('error', new Error('test'));
			index.b2[0].emit('error', new Error('test'));

			setTimeout(function () {
				t.ok(pool.isInState('running'));
				t.strictEqual(pool.getLastError(), undefined);

				t.equal(connections.length, 2);
				summarize();

				resolver.emit('removed', 'b2');

				index.b1[0].emit('error', new Error('test2'));
				index.b2[0].emit('error', new Error('test2'));

				setTimeout(function () {
					t.ok(pool.isInState('failed'));

					t.deepEqual(pool.p_keys, ['b1']);
					t.deepEqual(pool.p_dead,
					    { 'b1': true });

					pool.stop();
					/* Stop tape from giving up. */
					timer = setTimeout(function () {},
					    5000);
				}, 100);
			}, 100);
		}, 100);
	});
});


mod_tape.test('cleanup sandbox', function (t) {
	sandbox.restore();
	t.end();
});
