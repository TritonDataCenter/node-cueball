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

const mod_resolver = require('../lib/resolver');
const mod_nsc = require('mname-client');
const mod_mname = require('mname');
const mod_proto = mod_mname.Protocol;

var sandbox;
var nsclients = [];

var recovery = {
	default: {timeout: 1000, retries: 3, delay: 100 }
};

function emptyPacket() {
	return ({
		header: {
			id: 1234,
			flags: {},
			qdCount: 0,
			anCount: 0,
			nsCount: 0,
			arCount: 0
		},
		question: [],
		answer: [],
		authority: [],
		additional: []
	});
}

function DummyDnsClient(options) {
	this.history = [];
	nsclients.push(this);
}
DummyDnsClient.prototype.close = function () {
	var idx = nsclients.indexOf(this);
	mod_assert.notStrictEqual(idx, -1);
	nsclients.splice(idx, 1);
};
DummyDnsClient.prototype.lookup = function (options, cb) {
	mod_assert.object(options, 'options');
	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.string(options.domain, 'options.domain');
	mod_assert.string(options.type, 'options.type');
	mod_assert.number(options.timeout, 'options.timeout');

	this.history.push(options);

	var parts = options.domain.split('.').reverse();
	var err = null, reply;
	reply = emptyPacket();
	reply.header.flags.qr = true;
	reply.header.flags.rd = true;
	reply.header.flags.opcode = mod_proto.opCodes.QUERY;
	reply.header.flags.rcode = mod_proto.rCodes.NXDOMAIN;

	reply.question.push({
		name: options.domain,
		type: mod_proto.queryTypes[options.type],
		qclass: mod_proto.qClasses.IN
	});
	reply.header.qdCount++;

	switch (parts[0]) {
	case 'ok':
		if (parts[1] === 'srv' && parts[2] === '_tcp' &&
		    options.type === 'SRV') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.SRV,
				rclass: mod_proto.qClasses.IN,
				rttl: 3600,
				rdata: {
					priority: 0,
					weight: 10,
					port: 111,
					target: 'a.ok'
				}
			});
			reply.header.anCount++;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.SRV,
				rclass: mod_proto.qClasses.IN,
				rttl: 3600,
				rdata: {
					priority: 0,
					weight: 10,
					port: 111,
					target: 'aaaa.ok'
				}
			});
			reply.header.anCount++;

		} else if (parts[1] === 'a' && options.type === 'A') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.A,
				rclass: mod_proto.qClasses.IN,
				rttl: 3600,
				rdata: { target: '1.2.3.4' }
			});
			reply.header.anCount++;
		} else if (parts[1] === 'aaaa' && options.type === 'AAAA') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.AAAA,
				rclass: mod_proto.qClasses.IN,
				rttl: 3600,
				rdata: { target: '1234:abcd::1' }
			});
			reply.header.anCount++;
		} else if (parts[1] === 'a' || parts[1] === 'aaaa') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			/* send a NODATA response. */
		}
		break;
	case 'notfound':
		break;
	case 'notimp':
		if (parts[1] === 'srv' && parts[2] === '_tcp' &&
		    options.type === 'SRV') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.SRV,
				rclass: mod_proto.qClasses.IN,
				rttl: 3600,
				rdata: {
					priority: 0,
					weight: 10,
					port: 111,
					target: 'a.notimp'
				}
			});
			reply.header.anCount++;

		} else {
			reply.header.flags.rcode = mod_proto.rCodes.NOTIMP;
		}
		break;
	case 'short-ttl':
		if (parts[1] === 'a' && options.type === 'A') {
			reply.header.flags.rcode = mod_proto.rCodes.NOERROR;
			reply.answer.push({
				name: options.domain,
				rtype: mod_proto.queryTypes.A,
				rclass: mod_proto.qClasses.IN,
				rttl: 1,
				rdata: { target: '1.2.3.4' }
			});
			reply.header.anCount++;
		}
		break;
	case 'timeout':
		err = new Error('Timeout');
		setTimeout(cb.bind(this, err), options.timeout);
		return;
	default:
		throw (new Error('wat'));
	}

	reply = new mod_nsc.DnsMessage(reply);
	err = reply.toError();
	setImmediate(cb.bind(this, err, reply));
};

mod_tape.test('setup sandbox', function (t) {
	sandbox = mod_sinon.sandbox.create();
	sandbox.stub(mod_nsc, 'DnsClient', DummyDnsClient);
	t.end();
});

mod_tape.test('SRV lookup', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'srv.ok',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.fail();
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.equal(backends.length, 2);
			t.strictEqual(backends[0].address, '1.2.3.4');
			t.strictEqual(backends[0].port, 111);
			t.strictEqual(backends[1].address, '1234:abcd::1');
			t.strictEqual(backends[1].port, 111);

			t.equal(nsclients.length, 1);
			var history = nsclients[0].history.map(function (f) {
				return (f.domain + '/' + f.type);
			});
			t.deepEqual(history, [
				'_foo._tcp.srv.ok/SRV',
				'a.ok/AAAA', /* 1 try, got NODATA */
				'aaaa.ok/AAAA',
				'a.ok/A',
				'aaaa.ok/A'  /* 1 try, got NODATA */
			]);

			nsclients[0].history = [];

			res.stop();
			t.end();
		}
	});
	res.start();
});

mod_tape.test('plain A lookup', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'a.ok',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.fail();
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.equal(backends.length, 1);
			t.strictEqual(backends[0].address, '1.2.3.4');
			t.strictEqual(backends[0].port, 112);

			t.equal(nsclients.length, 1);
			var history = nsclients[0].history.map(function (f) {
				return (f.domain + '/' + f.type);
			});
			t.deepEqual(history, [
				'_foo._tcp.a.ok/SRV', /* no retries, SRV */
				'a.ok/AAAA', /* 1 try, got NODATA */
				'a.ok/A'
			]);

			nsclients[0].history = [];

			res.stop();
			t.end();
		}
	});
	res.start();
});

mod_tape.test('not found => failed', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'foo.notfound',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.ok(nsclients[0].history.length > 1);
			nsclients[0].history = [];
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.fail();
			res.stop();
			t.end();
		}
	});
	res.start();
});

mod_tape.test('notimp => failed', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'a.notimp',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.ok(nsclients[0].history.length > 1);
			nsclients[0].history = [];
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.fail();
			res.stop();
			t.end();
		}
	});
	res.start();
});

mod_tape.test('SRV ok, notimp on A => failed', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'srv.notimp',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.ok(nsclients[0].history.length > 1);
			nsclients[0].history = [];
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.fail();
			res.stop();
			t.end();
		}
	});
	res.start();
});

mod_tape.test('short TTL', function (t) {
	var res = new mod_resolver.DNSResolver({
		domain: 'a.short-ttl',
		service: '_foo._tcp',
		defaultPort: 112,
		resolvers: ['1.2.3.4'],
		recovery: recovery
	});
	var backends = [];
	res.on('added', function (key, backend) {
		backends.push(backend);
	});
	res.on('stateChanged', function (st) {
		if (st === 'failed') {
			t.fail();
			res.stop();
			t.end();
		} else if (st === 'running') {
			t.equal(backends.length, 1);
			t.strictEqual(backends[0].address, '1.2.3.4');
			t.strictEqual(backends[0].port, 112);

			t.equal(nsclients.length, 1);
			var history = nsclients[0].history.map(function (f) {
				return (f.domain + '/' + f.type);
			});
			t.deepEqual(history, [
				'_foo._tcp.a.short-ttl/SRV',
				'a.short-ttl/AAAA',
				'a.short-ttl/AAAA',
				'a.short-ttl/AAAA', /* 3 retries, not found */
				'a.short-ttl/A'
			]);
			nsclients[0].history = [];

			setTimeout(function () {
				t.equal(backends.length, 1);
				t.strictEqual(backends[0].address, '1.2.3.4');
				t.strictEqual(backends[0].port, 112);

				t.equal(nsclients.length, 1);
				var history2 = nsclients[0].history.map(
				    function (f) {
					return (f.domain + '/' + f.type);
				});
				t.deepEqual(history2, ['a.short-ttl/A']);
				nsclients[0].history = [];
				res.stop();
				t.end();
			}, 1500);
		}
	});
	res.start();
});

mod_tape.test('cleanup sandbox', function (t) {
	sandbox.restore();
	t.end();
});
