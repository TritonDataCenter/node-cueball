/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_resolver = require('../lib/resolver');
const mod_tape = require('tape');

/*
 * Test the resolverForIpOrDomain() factory function and related interfaces.
 */

mod_tape.test('resolverForIpOrDomain: bad arguments', function (t) {
	t.throws(function () {
		mod_resolver.resolverForIpOrDomain({});
	}, /args\.input/);

	t.throws(function () {
		mod_resolver.resolverForIpOrDomain('foobar');
	}, /args/);

	t.throws(function () {
		mod_resolver.resolverForIpOrDomain({
		    'input': 1234
		});
	}, /args\.input/);

	t.throws(function () {
		mod_resolver.resolverForIpOrDomain({
		    'input': 'foobar',
		    'resolverConfig': 17
		});
	}, /args\.resolverConfig/);

	t.end();
});

/*
 * parseIpOrDomain(): this is the lowest-level function.  It's responsible for
 * parsing the input string and determining what kind of Resolver to create.
 */

mod_tape.test('parseIpOrDomain: ipv4', function (t) {
	var result;

	result = mod_resolver.parseIpOrDomain('127.0.0.1');
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'static');
	t.equal(result.cons.name, 'CueBallStaticResolver');
	t.deepEqual(result.config, {
	    'backends': [ { 'address': '127.0.0.1', 'port': undefined } ]
	});
	result = mod_resolver.parseIpOrDomain('127.0.0.1:1234');
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'static');
	t.equal(result.cons.name, 'CueBallStaticResolver');
	t.deepEqual(result.config, {
	    'backends': [ { 'address': '127.0.0.1', 'port': 1234 } ]
	});

	result = mod_resolver.parseIpOrDomain('127.0.0.1:-3');
	t.ok(result instanceof Error);
	t.ok(/unsupported port in input:/.test(result.message));

	result = mod_resolver.parseIpOrDomain('127.0.0.1:ab123');
	t.ok(result instanceof Error);
	t.ok(/unsupported port in input:/.test(result.message));

	t.end();
});

mod_tape.test('parseIpOrDomain: hostname', function (t) {
	var result;

	result = mod_resolver.parseIpOrDomain('1.moray.emy-10.joyent.us');
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'dns');
	t.equal(result.cons.name, 'CueBallDNSResolver');
	t.deepEqual(result.config, {
	    'domain': '1.moray.emy-10.joyent.us'
	});

	result = mod_resolver.parseIpOrDomain('myservice');
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'dns');
	t.equal(result.cons.name, 'CueBallDNSResolver');
	t.deepEqual(result.config, {
	    'domain': 'myservice'
	});

	result = mod_resolver.parseIpOrDomain('myservice:1234');
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'dns');
	t.equal(result.cons.name, 'CueBallDNSResolver');
	t.deepEqual(result.config, {
	    'domain': 'myservice',
	    'defaultPort': 1234
	});

	result = mod_resolver.parseIpOrDomain('myservice:-3');
	t.ok(result instanceof Error);
	t.ok(/unsupported port in input:/.test(result.message));

	t.end();
});

/*
 * configForIpOrDomain: takes the result of parseIpOrDomain() and merges the
 * user-provided configuration.
 */

mod_tape.test('configForIpOrDomain: static IP', function (t) {
	var result;

	/*
	 * For static resolvers, there's no additional configuration.
	 */
	result = mod_resolver.configForIpOrDomain({
	    'input': '127.0.0.1:2020'
	});
	t.ok(!(result instanceof Error));
	t.equal(result.kind, 'static');
	t.deepEqual(result.config, {
	    'backends': [ { 'address': '127.0.0.1', 'port': 2020 } ]
	});
	t.deepEqual(result.mergedConfig, result.config);

	/*
	 * With DNS-based resolvers, additional properties are typically
	 * specified, but the port provided in the input should override the
	 * default port.
	 */
	result = mod_resolver.configForIpOrDomain({
	    'input': '1.moray:4567',
	    'resolverConfig': {
		'defaultPort': 1234,
		'service': '_moray_.tcp',
		'resolvers': [ '192.168.0.1', '192.168.0.3' ]
	    }
	});
	t.equal(result.kind, 'dns');
	t.deepEqual(result.mergedConfig, {
	    'domain': '1.moray',
	    'defaultPort': 4567,
	    'service': '_moray_.tcp',
	    'resolvers': [ '192.168.0.1', '192.168.0.3' ]
	});

	/*
	 * If there's no port in the input, then we should use the default port.
	 */
	result = mod_resolver.configForIpOrDomain({
	    'input': '1.moray',
	    'resolverConfig': {
		'defaultPort': 1234,
		'service': '_moray_.tcp',
		'resolvers': [ '192.168.0.1', '192.168.0.3' ]
	    }
	});
	t.equal(result.kind, 'dns');
	t.deepEqual(result.mergedConfig, {
	    'domain': '1.moray',
	    'defaultPort': 1234,
	    'service': '_moray_.tcp',
	    'resolvers': [ '192.168.0.1', '192.168.0.3' ]
	});

	t.end();
});


/*
 * resolverForIpOrDomain: takes the result of configForIpOrDomain and simply
 * instantiates the resolver.  Given that we've tested everything above, there's
 * less that needs to be checked here.
 */

mod_tape.test('resolverForIpOrDomain: static IP', function (t) {
	var result, list;

	result = mod_resolver.resolverForIpOrDomain({
	    'input': '127.0.0.1:2020'
	});
	t.ok(!(result instanceof Error));
	t.ok(result instanceof mod_resolver.ResolverFSM);
	t.ok(result.r_fsm instanceof mod_resolver.StaticIpResolver);
	list = result.list();
	t.equal(1, Object.keys(list).length);
	t.deepEqual(list[Object.keys(list)[0]], {
	    'name': '127.0.0.1:2020',
	    'address': '127.0.0.1',
	    'port': 2020
	});

	result = mod_resolver.resolverForIpOrDomain({
	    'input': '127.0.0.1:70000'
	});
	t.ok(result instanceof Error);
	t.ok(/unsupported port in input:/.test(result.message));

	t.end();
});

mod_tape.test('resolverForIpOrDomain: hostname', function (t) {
	var result;

	result = mod_resolver.resolverForIpOrDomain({
	    'input': '1.moray.emy-10.joyent.us',
	    'resolverConfig': {
		'recovery': {
		    'default': {
			'retries': 1,
			'timeout': 1000,
			'delay': 1000,
			'maxDelay': 1000
		    }
		}
	    }
	});
	t.ok(!(result instanceof Error));
	t.ok(result instanceof mod_resolver.ResolverFSM);
	t.ok(result.r_fsm instanceof mod_resolver.DNSResolver);

	result = mod_resolver.resolverForIpOrDomain({
	    'input': '1.moray.emy-10.joyent.us:70000'
	});
	t.ok(result instanceof Error);
	t.ok(/unsupported port in input:/.test(result.message));

	t.end();
});
