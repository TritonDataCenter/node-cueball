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
const mod_restify = require('restify');
const mod_net = require('net');
const mod_sshpk = require('sshpk');

const mod_cueball = require('..');

var log = mod_bunyan.createLogger({
	name: 'agent-test',
	level: process.env.LOGLEVEL || 'debug'
});
var recovery = {
	default: {timeout: 100, retries: 2, delay: 50 }
};

var servers = {};

mod_tape.test('setup server 1', function (t) {
	var srv = mod_restify.createServer({
		log: log
	});
	servers[12380] = srv;
	srv.get('/test/:val', function (req, res, next) {
		res.send('test response');
		srv.lastVal = req.params.val;
		next();
	});
	srv.listen(12380, function () {
		t.end();
	});
});

mod_tape.test('basic agent usage, fixed ip', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1:12380',
		agent: agent,
		log: log,
		retry: false
	});

	client.get('/test/test1', function (err, req, res, data) {
		t.error(err);
		t.strictEqual(data, 'test response');
		t.strictEqual(servers[12380].lastVal, 'test1');

		var pool = agent.getPool('127.0.0.1');
		t.ok(pool);
		t.ok(pool.isInState('running'));

		t.strictEqual(agent.getPool('abcd'), undefined);

		t.throws(function () {
			agent.createPool('127.0.0.1', {});
		});

		t.notOk(agent.isStopped());

		agent.stop(function () {
			t.ok(pool.isInState('stopped'));
			t.ok(agent.isStopped());
			t.end();
		});
	});
});

mod_tape.test('agent initialDomains', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		defaultPort: 12380,
		initialDomains: ['127.0.0.1'],
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1',
		agent: agent,
		log: log,
		retry: false
	});

	var pool = agent.getPool('127.0.0.1');
	t.ok(pool);

	client.get('/test/test2', function (err, req, res, data) {
		t.error(err);
		t.strictEqual(data, 'test response');
		t.strictEqual(servers[12380].lastVal, 'test2');

		t.ok(pool.isInState('running'));

		agent.stop(function () {
			t.ok(pool.isInState('stopped'));
			t.throws(function () {
				agent.addPool('example.com');
			});
			t.end();
		});
	});
});

mod_tape.test('agent createpool args', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		log: log,
		defaultPort: 12380
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1:12380',
		agent: agent,
		log: log,
		retry: false
	});

	var pool = agent.getPool('127.0.0.1');
	t.strictEqual(pool, undefined);
	agent.createPool('127.0.0.1');
	pool = agent.getPool('127.0.0.1');
	t.ok(pool);
	t.strictEqual(pool, agent.getPool('127.0.0.1'));

	client.get('/test/test-createpool-args',
	    function (err, req, res, data) {
		t.error(err);
		t.strictEqual(data, 'test response');
		t.strictEqual(servers[12380].lastVal, 'test-createpool-args');

		t.ok(pool.isInState('running'));

		agent.stop(function () {
			t.ok(pool.isInState('stopped'));
			t.end();
		});
	});
});

mod_tape.test('agent pinger', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		ping: '/test/ping',
		pingInterval: 100,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1:12380',
		agent: agent,
		log: log,
		retry: false
	});

	var pool = agent.getPool('127.0.0.1');
	t.strictEqual(pool, undefined);
	agent.createPool('127.0.0.1', { port: 12380 });
	pool = agent.getPool('127.0.0.1');
	t.ok(pool);
	t.strictEqual(pool, agent.getPool('127.0.0.1'));

	t.notStrictEqual(servers[12380].lastVal, 'ping');
	setTimeout(function () {
		t.strictEqual(servers[12380].lastVal, 'ping');

		client.get('/test/test3', function (err) {
			t.error(err);
			t.strictEqual(servers[12380].lastVal, 'test3');
			agent.stop(function () {
				t.end();
			});
		});
	}, 200);
});

mod_tape.test('setup server 2', function (t) {
	var srv = mod_restify.createServer({
		log: log
	});
	servers[12381] = srv;
	srv.get('/test/:val', function (req, res, next) {
		res.send('test response');
		srv.lastVal = req.params.val;
		next();
	});
	srv.listen(12381, function () {
		t.end();
	});
});

mod_tape.test('agent with custom resolver', function (t) {
	var res = new mod_cueball.StaticIpResolver({
		backends: [
			{ address: '127.0.0.1', port: 12380 },
			{ address: '127.0.0.1', port: 12381 }
		]
	});

	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		ping: '/test/ping',
		pingTimeout: 150,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://foobar',
		agent: agent,
		log: log,
		retry: false
	});

	var srv = servers[12380];
	srv.server.timeout = 100;

	var pool = agent.getPool('foobar');
	t.strictEqual(pool, undefined);
	agent.createPool('foobar', { resolver: res });
	pool = agent.getPool('foobar');
	t.ok(pool);

	t.ok(res.isInState('stopped'));
	res.start();

	client.get('/test/test4', function (err, req, resp, data) {
		t.error(err);
		t.strictEqual(data, 'test response');
		t.ok(servers[12381].lastVal === 'test4' ||
		    servers[12380].lastVal === 'test4');

		t.ok(pool.isInState('running'));

		srv.on('close', function () {
			setTimeout(nextReq, 100);
		});

		function nextReq() {
			client.get('/test/test5',
			    function (err2, req2, resp2, data2) {
				t.error(err2);
				t.strictEqual(data2, 'test response');

				t.strictEqual(servers[12381].lastVal, 'test5');

				agent.stop(function () {
					t.ok(pool.isInState('stopped'));
					t.ok(!res.isInState('stopped'));
					res.stop();
					t.end();
				});
			});
		}

		srv.close();
		delete (servers[12380]);
	});
});

mod_tape.test('setup server 3', function (t) {
	var srv = mod_net.createServer();
	servers[12382] = srv;
	srv.on('connection', function (c) {
		setTimeout(function () {
			c.destroy();
		}, 50);
	});
	srv.listen(12382, function () {
		t.end();
	});
});

mod_tape.test('agent on conn refused server', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1:12389',
		agent: agent,
		log: log,
		retry: false
	});

	var t0 = process.hrtime();

	client.get('/test/test5', function (err, req, res, data) {
		t.ok(err);
		var delta = process.hrtime(t0);
		console.log(delta);
		t.ok(delta[0] < 1 && delta[1] < 1e9);

		var pool = agent.getPool('127.0.0.1');
		t.ok(pool.isInState('failed'));

		agent.stop(function () {
			t.ok(pool.isInState('stopped'));
			t.end();
		});
	});
});

mod_tape.test('agent on broken server', function (t) {
	var agent = new mod_cueball.HttpAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'http://127.0.0.1:12382',
		agent: agent,
		log: log,
		retry: false
	});

	client.get('/test/test5', function (err, req, res, data) {
		t.ok(err);
		t.strictEqual(err.code, 'ECONNRESET');

		var pool = agent.getPool('127.0.0.1');

		client.get('/test/test5', function (err2, req2, res2, data2) {
			t.ok(err2);
			t.strictEqual(err2.code, 'ECONNRESET');

			setTimeout(function () {
				t.ok(pool.isInState('running'));

				agent.stop(function () {
					t.ok(pool.isInState('stopped'));
					t.end();
				});
			}, 1000);
		});
	});
});

mod_tape.test('setup server 4 (tls)', function (t) {
	var privkey = mod_sshpk.generatePrivateKey('ecdsa');
	var id = mod_sshpk.identityFromDN('CN=127.0.0.1');
	var tlsCert = mod_sshpk.createSelfSignedCertificate(id, privkey);
	var srv = mod_restify.createServer({
		httpsServerOptions: {
			key: privkey.toBuffer('pem'),
			cert: tlsCert.toBuffer('pem')
		},
		log: log
	});
	servers[12383] = srv;
	srv.get('/test/:val', function (req, res, next) {
		res.send('test response');
		srv.lastVal = req.params.val;
		next();
	});
	srv.listen(12383, function () {
		t.end();
	});
});

mod_tape.test('agent https', function (t) {
	var agent = new mod_cueball.HttpsAgent({
		spares: 2,
		maximum: 4,
		recovery: recovery,
		log: log
	});

	var client = mod_restify.createStringClient({
		url: 'https://127.0.0.1:12383',
		agent: agent,
		log: log,
		retry: false
	});

	agent.createPool('127.0.0.1', {
		port: 12383,
		rejectUnauthorized: false
	});

	client.get('/test/test6', function (err, req, res, data) {
		t.error(err);
		t.strictEqual(data, 'test response');
		t.strictEqual(servers[12383].lastVal, 'test6');

		var pool = agent.getPool('127.0.0.1');
		t.ok(pool);
		t.ok(pool.isInState('running'));

		agent.stop(function () {
			t.ok(pool.isInState('stopped'));
			t.end();
		});
	});
});

mod_tape.test('teardown servers', function (t) {
	Object.keys(servers).forEach(function (k) {
		servers[k].close();
	});
	t.end();
});
