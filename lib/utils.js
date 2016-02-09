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

function planRebalance(inSpares, total, want) {
	var spares = {};
	var keyCount = 0;
	var buckets = {};

	Object.keys(inSpares).forEach(function (key) {
		spares[key] = inSpares[key].slice();
		++keyCount;
		var count = spares[key].length;
		if (buckets[count] === undefined)
			buckets[count] = [];
		buckets[count].push(key);
	});

	var plan = { add: [], remove: [] };

	var max = Math.ceil(want / keyCount);

	function findFatBuckets() {
		return (Object.keys(buckets).map(function (cnt) {
			return (parseInt(cnt, 10));
		}).sort().reverse().filter(function (cnt) {
			return (cnt > max || total > want);
		}));
	}

	var fatbkts = findFatBuckets();
	while (fatbkts.length > 0) {
		var b = fatbkts[0];
		var ks = buckets[b];
		ks.forEach(function (k) {
			if (b <= max && total <= want)
				return;
			--total;

			var fsm = spares[k].shift();
			plan.remove.push(fsm);

			if (buckets[b - 1] === undefined)
				buckets[b - 1] = [];
			buckets[b - 1].push(k);
		});
		delete (buckets[b]);
		fatbkts = findFatBuckets();
	}

	while (total < want) {
		var bs = Object.keys(buckets).map(function (cnt) {
			return (parseInt(cnt, 10));
		}).sort();
		b = bs[0];
		ks = buckets[b];
		var i = 0;
		while (total < want && i < ks.length) {
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
