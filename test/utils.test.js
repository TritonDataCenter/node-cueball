/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_tape = require('tape');

const mod_utils = require('../lib/utils');

mod_tape.test('rebalance: simple addition', function (t) {
	var spares = {
		'b1': []
	};
	var plan = mod_utils.planRebalance(spares, 0, 4);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b1', 'b1', 'b1']);
	t.end();
});

mod_tape.test('rebalance: addition over 2 options', function (t) {
	var spares = {
		'b1': [],
		'b2': []
	};
	var plan = mod_utils.planRebalance(spares, 0, 5);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b2', 'b1', 'b2', 'b1']);
	t.end();
});

mod_tape.test('rebalance: add with existing', function (t) {
	var spares = {
		'b1': ['c1'],
		'b2': ['c2']
	};
	var plan = mod_utils.planRebalance(spares, 2, 4);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b2']);
	t.end();
});

mod_tape.test('rebalance: add none', function (t) {
	var spares = {
		'b1': ['c1', 'c3'],
		'b2': ['c2', 'c4']
	};
	var plan = mod_utils.planRebalance(spares, 4, 4);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: add and remove', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3'],
		'b2': ['c4']
	};
	var plan = mod_utils.planRebalance(spares, 4, 4);
	t.equal(plan.remove.length, 1);
	t.ok(['c1', 'c2', 'c3'].indexOf(plan.remove[0]) !== -1);
	t.deepEqual(plan.add, ['b2']);
	t.end();
});

mod_tape.test('rebalance: add from unbalanced', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3'],
		'b2': ['c4']
	};
	var plan = mod_utils.planRebalance(spares, 4, 6);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b2', 'b2']);
	t.end();
});

mod_tape.test('rebalance: shrink', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3'],
		'b2': ['c4', 'c5', 'c6']
	};
	var plan = mod_utils.planRebalance(spares, 6, 4);
	t.deepEqual(plan.remove, ['c1', 'c4']);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: lots of nodes', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3', 'c4'],
		'b2': [],
		'b3': [],
		'b4': [],
		'b5': [],
		'b6': [],
		'b7': []
	};
	var plan = mod_utils.planRebalance(spares, 4, 5);
	t.deepEqual(plan.remove, ['c1', 'c2', 'c3']);
	t.deepEqual(plan.add, ['b2', 'b3', 'b4', 'b5']);
	t.end();
});

mod_tape.test('rebalance: more nodes', function (t) {
	var spares = {
		'b3': [],
		'b1': [],
		'b2': [],
		'b4': [],
		'b5': ['c1', 'c2', 'c3', 'c4'],
		'b6': [],
		'b7': []
	};
	var plan = mod_utils.planRebalance(spares, 4, 6);
	t.deepEqual(plan.remove, ['c1', 'c2', 'c3']);
	t.deepEqual(plan.add, ['b3', 'b1', 'b2', 'b4', 'b6']);
	t.end();
});

mod_tape.test('rebalance: excess spread out', function (t) {
	var spares = {
		'b3': ['c1'],
		'b1': ['c2'],
		'b2': ['c3'],
		'b4': ['c4'],
		'b5': ['c5'],
		'b6': ['c6'],
		'b7': []
	};
	var plan = mod_utils.planRebalance(spares, 6, 3);
	t.deepEqual(plan.remove, ['c1', 'c2', 'c3']);
	t.deepEqual(plan.add, []);
	t.end();
});
