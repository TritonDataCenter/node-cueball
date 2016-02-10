/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	shuffle: shuffle,
	planRebalance: planRebalance
};

const mod_assert = require('assert-plus');

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

function planRebalance(inSpares, total, target) {
	var spares = {};
	var keyCount = 0;
	var buckets = {};

	mod_assert.object(inSpares, 'connections');
	mod_assert.number(total, 'count');
	mod_assert.number(target, 'target');

	mod_assert.ok(total >= 0, 'count must be >= 0');
	mod_assert.ok(target >= 0, 'target must be >= 0');

	Object.keys(inSpares).forEach(function (key) {
		mod_assert.array(inSpares[key], 'connections[' + key + ']');
		spares[key] = inSpares[key].slice();
		++keyCount;
		var count = spares[key].length;
		if (buckets[count] === undefined)
			buckets[count] = [];
		buckets[count].push(key);
	});

	var plan = { add: [], remove: [] };

	var max = Math.round(target / keyCount);
	if (max < 1)
		max = 1;

	function bucketsBySize() {
		return (Object.keys(buckets).map(function (cnt) {
			return (parseInt(cnt, 10));
		}).sort().reverse());
	}

	function findFatBuckets() {
		return (bucketsBySize().filter(function (cnt) {
			return (cnt > max);
		}));
	}

	/*
	 * First, cull any excess beyond the target size, working from the
	 * biggest buckets down to the smallest ones.
	 */
	var bkts = bucketsBySize();
	while (total > target) {
		var b = bkts[0];
		mod_assert.number(b, 'a backend key');
		mod_assert.ok(b > 0, 'backend key must not be zero');
		var ks = buckets[b];
		mod_assert.arrayOfString(ks, 'at least one key of order ' + b);
		while (total > target && ks.length > 0) {
			var k = ks.shift();
			--total;

			var fsm = spares[k].shift();
			mod_assert.ok(fsm, 'a spare ' + k + ' connection is ' +
			    'required');
			plan.remove.push(fsm);

			if (buckets[b - 1] === undefined)
				buckets[b - 1] = [];
			buckets[b - 1].push(k);
		}
		if (ks.length === 0)
			delete (buckets[b]);
		bkts = bucketsBySize();
	}

	/* Now, cull any buckets that are bigger than the average cap (max). */
	var fatbkts = findFatBuckets();
	while (fatbkts.length > 0) {
		b = fatbkts[0];
		mod_assert.number(b, 'a backend key');
		mod_assert.ok(b > 0, 'backend key must not be zero');
		ks = buckets[b];
		mod_assert.arrayOfString(ks, 'at least one key of order ' + b);
		while (ks.length > 0) {
			k = ks.shift();
			--total;

			fsm = spares[k].shift();
			mod_assert.ok(fsm, 'a spare ' + k + ' connection is ' +
			    'required');
			plan.remove.push(fsm);

			if (buckets[b - 1] === undefined)
				buckets[b - 1] = [];
			buckets[b - 1].push(k);
		}
		delete (buckets[b]);
		fatbkts = findFatBuckets();
	}

	/*
	 * Finally, if we're now under target, add evenly to the lowest buckets
	 * until we hit it.
	 */
	while (total < target) {
		var bs = Object.keys(buckets).map(function (cnt) {
			return (parseInt(cnt, 10));
		}).sort();
		b = bs[0];
		ks = buckets[b];
		var i = 0;
		while (total < target && i < ks.length) {
			var kk = ks[i];
			plan.add.push(kk);
			++total;
			if (buckets[b + 1] === undefined)
				buckets[b + 1] = [];
			buckets[b + 1].push(kk);
			++i;
		}
		delete (buckets[b]);
	}

	return (plan);
}
