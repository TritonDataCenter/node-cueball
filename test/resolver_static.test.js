/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_jsprim = require('jsprim');
const mod_resolver = require('../lib/resolver');
const mod_tape = require('tape');

/*
 * Test the static resolver.
 */

mod_tape.test('static resolver: bad arguments', function (t) {
	/*
	 * javascriptlint (rightfully) doesn't like invoking a constructor and
	 * ignoring the result.  It also (rightfully) doesn't like assigning it
	 * to a variable that itself is never used.  This is an unusual case,
	 * since we're essentially testing the constructor's behavior.
	 */
	/* jsl:ignore */
	t.throws(function () {
		new mod_resolver.StaticIpResolver();
	}, /options/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({});
	}, /options.backends/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': null
		});
	}, /options.backends/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': [ null ]
		});
	}, /options.backends/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': [ {
		        'address': '127.0.0.1',
			'port': 1234
		    }, {} ]
		});
	}, /options.backends\[1\].address/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': [ {
		        'address': '127.0.0.1',
			'port': 1234
		    }, {
		        'address': 1234,
			'port': 'foobar'
		    } ]
		});
	}, /options.backends\[1\].address/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': [ {
		        'address': '127.0.0.1',
			'port': 1234
		    }, {
		        'address': '127.0.0.1'
		    } ]
		});
	}, /options.backends\[1\].port/);

	t.throws(function () {
		new mod_resolver.StaticIpResolver({
		    'backends': [ {
		        'address': '127.0.0.1',
			'port': 1234
		    }, {
		        'address': '127.0.0.1',
			'port': 'foobar'
		    } ]
		});
	}, /options.backends\[1\].port/);
	/* jsl:end */

	t.end();
});

mod_tape.test('static resolver: no backends', function (t) {
	var resolver, nadded;

	resolver = new mod_resolver.StaticIpResolver({ 'backends': [] });
	resolver.start();

	nadded = 0;
	resolver.on('added', function () { nadded++; });
	resolver.on('updated', function () {
		t.equal(nadded, 0);
		t.deepEqual(resolver.list(), {});
		t.equal(resolver.count(), 0);
		resolver.stop();
		t.end();
	});
});

mod_tape.test('static resolver: several backends', function (t) {
	var resolver, found;

	resolver = new mod_resolver.StaticIpResolver({
	    'backends': [ {
		'address': '10.0.0.3',
		'port': 2021
	    }, {
		'address': '10.0.0.3',
		'port': 2020
	    }, {
		'address': '10.0.0.7',
		'port': 2020
	    } ]
	});

	resolver.start();

	found = [];
	resolver.on('added', function (key, backend) { found.push(backend); });
	resolver.on('updated', function () {
		var expected;

		t.equal(resolver.count(), 3);
		t.deepEqual(found, [ {
		    'name': '10.0.0.3:2021',
		    'address': '10.0.0.3',
		    'port': 2021
		}, {
		    'name': '10.0.0.3:2020',
		    'address': '10.0.0.3',
		    'port': 2020
		}, {
		    'name': '10.0.0.7:2020',
		    'address': '10.0.0.7',
		    'port': 2020
		} ]);

		expected = {};
		found.forEach(function (be) { expected[be['name']] = true; });
		mod_jsprim.forEachKey(resolver.list(), function (k, reported) {
			t.ok(expected.hasOwnProperty(reported['name']));
			delete (expected[reported['name']]);
		});

		t.equal(Object.keys(expected).length, 0);
		resolver.stop();
		t.end();
	});
});
