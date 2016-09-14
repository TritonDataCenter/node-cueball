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
	default: {timeout: 1000, retries: 3, delay: 100 }
};

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
	var idx = connections.indexOf(this);
	mod_assert.ok(idx !== -1);
	connections.splice(idx, 1);
	this.connected = false;
	this.dead = true;
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

	cset.on('added', function (key, conn) {
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

	cset.on('removed', function (key, conn) {
		if (!cset.isInState('stopping'))
			t.fail();
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});

	setImmediate(function () {
		connections.forEach(function (c) { c.connect(); });
	});
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
			setImmediate(function () {
				cset.stop();
				resolver.stop();
			});
		}
	});

	cset.on('removed', function (key, conn) {
		if (!cset.isInState('stopping'))
			t.fail();
	});

	resolver.start();
	t.strictEqual(connections.length, 0);

	resolver.emit('added', 'b1', {});
	resolver.emit('added', 'b2', {});

	setImmediate(function () {
		connections.forEach(function (c) { c.connect(); });
	});
});
