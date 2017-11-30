/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	shuffle: shuffle,
	planRebalance: planRebalance,
	assertRecovery: assertRecovery,
	assertRecoverySet: assertRecoverySet,
	assertClaimDelay: assertClaimDelay,
	currentMillis: currentMillis,
	stackTracesEnabled: stackTracesEnabled,
	maybeCaptureStackTrace: maybeCaptureStackTrace
};

const mod_assert = require('assert-plus');

stackTracesEnabled.ENABLED = false;
var mod_dtrace, dtProvider, dtProbe;

/*
 * Returns true if cueball should collect stack traces at every claim() and
 * release() from a Pool.
 *
 * By default, stack traces are disabled for performance reasons. There are two
 * ways they can be enabled:
 *   * by calling mod_cueball.enableStackTraces(), which sets the
 *     stackTracesEnabled.ENABLED above to true
 *   * by enabling the dtrace probe "capture-stack", e.g.
 *     $ dtrace -n 'cueball$pid:::capture-stack { }' -p 12345
 */
function stackTracesEnabled() {
	if (mod_dtrace === null)
		return (stackTracesEnabled.ENABLED);

	if (mod_dtrace === undefined) {
		/*
		 * We might not have built dtrace-provider at all, so the
		 * require here might fail (e.g. we're on a platform without
		 * dtrace).
		 */
		try {
			mod_dtrace = require('dtrace-provider');
		} catch (e) {
			mod_dtrace = null;
			return (stackTracesEnabled.ENABLED);
		}
		/*
		 * We create one probe, named "capture-stack". If anybody
		 * enables it by hooking into it, we start returning true.
		 */
		dtProvider = mod_dtrace.createDTraceProvider('cueball');
		dtProbe = dtProvider.addProbe('capture-stack', 'int');
		dtProvider.enable();
	}

	/*
	 * dtrace-provider (apparently on a point of principle) does not have
	 * any kind of isEnabled() method to tell if a probe has an enabling
	 * attached to it or not.
	 *
	 * However, fire() will only call its callback in the case where the
	 * probe is enabled, so we can use this property and a closure
	 * to achieve our check.
	 */
	var en = stackTracesEnabled.ENABLED;
	dtProbe.fire(function () {
		en = true;
		return ([1]);
	});
	return (en);
}

/*
 * Returns an object with a .stack property, either the real stack (if stack
 * traces are enabled) or a fake one two frames long (since this is the
 * shortest length a real stack trace would be).
 */
function maybeCaptureStackTrace() {
	var e = {};
	if (stackTracesEnabled()) {
		Error.captureStackTrace(e);
	} else {
		e.stack = 'Error\n at unknown (stack traces disabled)\n' +
		    ' at unknown (stack traces disabled)\n';
	}
	return (e);
}

function assertRecoverySet(obj) {
	mod_assert.object(obj, 'recovery');
	var keys = Object.keys(obj);
	keys.forEach(function (k) {
		assertRecovery(obj[k], 'recovery.' + k);
	});
}

function assertRecovery(obj, name) {
	if (name === undefined || name === null)
		name = 'recovery';
	mod_assert.object(obj, name);
	var ks = {};
	Object.keys(obj).forEach(function (k) { ks[k] = true; });
	mod_assert.number(obj.retries, name + '.retries');
	mod_assert.ok(isFinite(obj.retries), name + '.retries must be finite');
	mod_assert.ok(obj.retries >= 0, name + '.retries must be >= 0');
	delete (ks.retries);
	mod_assert.number(obj.timeout, name + '.timeout');
	mod_assert.ok(isFinite(obj.timeout), name + '.timeout must be finite');
	mod_assert.ok(obj.timeout > 0, name + '.timeout must be > 0');
	delete (ks.timeout);
	mod_assert.optionalNumber(obj.maxTimeout, name + '.maxTimeout');
	mod_assert.ok(obj.maxTimeout === undefined ||
	    obj.maxTimeout === null ||
	    obj.timeout <= obj.maxTimeout,
	    name + '.maxTimeout must be >= timeout');
	delete (ks.maxTimeout);
	mod_assert.number(obj.delay, name + '.delay');
	mod_assert.ok(isFinite(obj.delay), name + '.delay must be finite');
	mod_assert.ok(obj.delay >= 0, name + '.delay must be >= 0');
	delete (ks.delay);
	mod_assert.optionalNumber(obj.maxDelay, name + '.maxDelay');
	mod_assert.ok(obj.maxDelay === undefined ||
	    obj.maxDelay === null ||
	    obj.delay <= obj.maxDelay,
	    name + '.maxDelay must be >= delay');
	delete (ks.maxDelay);
	mod_assert.deepEqual(Object.keys(ks), []);

	var mult;
	if (obj.maxDelay === undefined || obj.maxDelay === null) {
		mod_assert.ok(obj.retries < 32,
		    name + '.maxDelay is required when retries >= 32 ' +
		    '(exponential increase becomes unreasonably large)');
		mult = 1 << obj.retries;
		var maxDelay = obj.delay * mult;
		mod_assert.ok(maxDelay < 1000 * 3600 * 24,
		    name + '.maxDelay is required with given values of ' +
		    'retries and delay (effective unspecified maxDelay is ' +
		    ' > 1 day)');
	}
	if (obj.maxTimeout === undefined || obj.maxTimeout === null) {
		mod_assert.ok(obj.retries < 32,
		    name + '.maxTimeout is required when retries >= 32 ' +
		    '(exponential increase becomes unreasonably large)');
		mult = 1 << obj.retries;
		var maxTimeout = obj.timeout * mult;
		mod_assert.ok(maxTimeout < 1000 * 3600 * 24,
		    name + '.maxTimeout is required with given values of ' +
		    'retries and timeout (effective unspecified maxTimeout ' +
		    'is > 1 day)');
	}
}

function assertClaimDelay(delay) {
	mod_assert.optionalFinite(delay, 'options.targetClaimDelay');
	if (Number.isFinite(delay)) {
		mod_assert.ok(delay > 0, 'options.targetClaimDelay > 0');
		mod_assert.equal(delay, Math.floor(delay),
		    'options.targetClaimDelay');
	}
}

/* Get monotonic time in milliseconds */
function currentMillis() {
	var time = process.hrtime();
	var secs2ms = time[0] * 1000;
	var ns2ms = time[1] / 1000000;

	return (secs2ms + ns2ms);
}

/* A Fisher-Yates shuffle. */
function shuffle(array) {
	var i = array.length;
	while (i > 0) {
		var j = Math.floor(Math.random() * i);
		--i;
		var temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
	return (array);
}

/*
 * `planRebalance(connections, dead, target, max)`
 *
 * Takes an abstract representation of the state of a connection pool and
 * returns a 'plan' for what to do next to bring it to an ideal balanced state.
 *
 * Returns a 'plan': an Object with properties:
 * - `add` -- Array of String, backend keys that should be added
 * - `remove` -- Array of Object, connections to be closed
 *
 * Parameters:
 * - `connections` -- an Object, map of String (backend id) to Array of Object
 *                    (connections), list of currently open connections
 * - `dead` -- an Object, map of String (backend id) to Boolean, true when a
 *             a given backend is declared dead
 * - `target` -- a Number, target number of connections we want to have
 * - `max` -- a Number, maximum socket ceiling
 * - `singleton` -- optional Boolean (default false), create only a single
 *                  connection per distinct backend. used for Sets.
 */
function planRebalance(inSpares, dead, target, max, singleton) {
	var replacements = 0;
	var wantedSpares = {};

	mod_assert.object(inSpares, 'connections');
	mod_assert.number(target, 'target');
	mod_assert.number(max, 'max');

	mod_assert.ok(target >= 0, 'target must be >= 0');
	mod_assert.ok(max >= target, 'max must be >= target');

	var keys = Object.keys(inSpares);

	var plan = { add: [], remove: [] };

	/*
	 * Build up the number of FSMs we *want* to have for each backend in
	 * the wantedSpares map.
	 *
	 * First, we want to have the "target" number of connections, spread
	 * evenly across all the backends. If we find any dead backends along
	 * the way, make sure we have exactly 1 connection to each and we
	 * request a replacement for each time we wanted to use it.
	 */
	var done = 0;
	for (var i = 0; i < target; ++i) {
		var k = keys.shift();
		keys.push(k);
		if (wantedSpares[k] === undefined)
			wantedSpares[k] = 0;
		if (dead[k] !== true) {
			if (singleton) {
				if (wantedSpares[k] === 0) {
					wantedSpares[k] = 1;
					++done;
				}
			} else {
				++wantedSpares[k];
				++done;
			}
			continue;
		}
		if (wantedSpares[k] === 0) {
			wantedSpares[k] = 1;
			++done;
		}
		++replacements;
	}

	/* Apply the max cap. */
	if (done + replacements > max)
		replacements = max - done;

	/*
	 * Now try to allocate replacements. These proceed similarly to the
	 * first allocation, round-robin across all available backends.
	 */
	for (i = 0; i < replacements; ++i) {
		k = keys.shift();
		keys.push(k);
		if (wantedSpares[k] === undefined)
			wantedSpares[k] = 0;
		if (dead[k] !== true) {
			if (singleton) {
				if (wantedSpares[k] === 0) {
					wantedSpares[k] = 1;
					++done;
					continue;
				}
			} else {
				++wantedSpares[k];
				++done;
				continue;
			}
		}
		/*
		 * We can make replacements for a replacement (and so on) as
		 * long as we have room under our max socket cap and we haven't
		 * already tried every backend available.
		 *
		 * If this one is marked as dead, though, and we don't have room
		 * to add both it and a replacement, AND there are backends we
		 * haven't tried yet or that are alive, skip this one and use
		 * one of those.
		 *
		 * In this way we guarantee that even if our socket cap
		 * prevents us from making a double-replacement, we still try
		 * all the backends at least once.
		 */
		var count = done + replacements - i;
		var empties = keys.filter(function (kk) {
			if (singleton) {
				return (dead[kk] !== true &&
				    wantedSpares[kk] === undefined);
			} else {
				return (dead[kk] !== true ||
				    wantedSpares[kk] === undefined);
			}
		});

		/* We have room for both this and a replacement. */
		if (count + 1 <= max) {
			if (wantedSpares[k] === 0) {
				wantedSpares[k] = 1;
				++done;
			}
			if (empties.length > 0)
				++replacements;

		/*
		 * We only have room for one, but there are other candidates
		 * that might actually be up. Use one of them instead.
		 */
		} else if (count <= max && empties.length > 0) {
			++replacements;

		/* Only room for one, everything looks dead. Use us. */
		} else if (count <= max) {
			if (wantedSpares[k] === 0) {
				wantedSpares[k] = 1;
				++done;
			}

		/* Already met our max socket cap. Give up now. */
		} else {
			break;
		}
	}

	/*
	 * Now calculate the difference between what we want and what we have:
	 * this will be our plan to return for what to do next.
	 */
	keys = Object.keys(inSpares).reverse();
	keys.forEach(function (key) {
		var have = (inSpares[key] || []).length;
		var want = wantedSpares[key] || 0;
		var list = inSpares[key].slice();
		while (have > want) {
			plan.remove.push(list.shift());
			--have;
		}
	});
	keys.reverse();
	keys.forEach(function (key) {
		var have = (inSpares[key] || []).length;
		var want = wantedSpares[key] || 0;
		while (have < want) {
			plan.add.push(key);
			++have;
		}
	});

	return (plan);
}
