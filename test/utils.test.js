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
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b1', 'b1', 'b1']);
	t.end();
});

mod_tape.test('rebalance: addition over 2 options', function (t) {
	var spares = {
		'b1': [],
		'b2': []
	};
	var plan = mod_utils.planRebalance(spares, {}, 5, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b1', 'b1', 'b2', 'b2']);
	t.end();
});

mod_tape.test('rebalance: add with existing', function (t) {
	var spares = {
		'b1': ['c1'],
		'b2': ['c2']
	};
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b2']);
	t.end();
});

mod_tape.test('rebalance: add none', function (t) {
	var spares = {
		'b1': ['c1', 'c3'],
		'b2': ['c2', 'c4']
	};
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: add and remove', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3'],
		'b2': ['c4']
	};
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
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
	var plan = mod_utils.planRebalance(spares, {}, 6, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b2', 'b2']);
	t.end();
});

mod_tape.test('rebalance: shrink', function (t) {
	var spares = {
		'b1': ['c1', 'c2', 'c3'],
		'b2': ['c4', 'c5', 'c6']
	};
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
	t.deepEqual(plan.remove, ['c4', 'c1']);
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
	var plan = mod_utils.planRebalance(spares, {}, 5, 10);
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
	var plan = mod_utils.planRebalance(spares, {}, 6, 10);
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
	var plan = mod_utils.planRebalance(spares, {}, 3, 10);
	t.deepEqual(plan.remove, ['c6', 'c5', 'c4']);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: odd number', function (t) {
	var spares = {
		'b3': ['c1'],
		'b1': [],
		'b2': []
	};
	var plan = mod_utils.planRebalance(spares, {}, 4, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b3', 'b1', 'b2']);
	t.end();
});

mod_tape.test('rebalance: re-ordering', function (t) {
	var spares = {
		'b2': [],
		'b1': ['c1'],
		'b3': ['c2']
	};
	var plan = mod_utils.planRebalance(spares, {}, 2, 10);
	t.deepEqual(plan.remove, ['c2']);
	t.deepEqual(plan.add, ['b2']);
	t.end();
});

mod_tape.test('rebalance: dead replacement', function (t) {
	var spares = {
		'b1': [],
		'b2': [],
		'b3': []
	};
	var dead = {
		'b1': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 2, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b2', 'b3']);
	t.end();
});

mod_tape.test('rebalance: dead replacement and shrink', function (t) {
	var spares = {
		'b1': ['c1', 'c3'],
		'b2': ['c2'],
		'b3': []
	};
	var dead = {
		'b1': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 3, 10);
	t.deepEqual(plan.remove, ['c1']);
	t.deepEqual(plan.add, ['b2', 'b3']);
	t.end();
});

mod_tape.test('rebalance: dead again', function (t) {
	var spares = {
		'b1': ['c1'],
		'b2': ['c2']
	};
	var dead = {
		'b1': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 1, 2);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: nested dead', function (t) {
	var spares = {
		'b1': [],
		'b2': ['c2'],
		'b3': [],
		'b4': []
	};
	var dead = {
		'b1': true,
		'b3': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 2, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b3', 'b4']);
	t.end();
});

mod_tape.test('rebalance: nested dead with cap', function (t) {
	var spares = {
		'b1': [],
		'b2': ['c2'],
		'b3': [],
		'b4': []
	};
	var dead = {
		'b1': true,
		'b3': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 2, 3);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b1', 'b4']);
	t.end();
});

mod_tape.test('rebalance: dead, backend starvation', function (t) {
	var spares = {
		'b1': ['c1']
	};
	var dead = {
		'b1': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 2, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, []);
	t.end();
});

mod_tape.test('rebalance: dead, backend starvation', function (t) {
	var spares = {
		'b1': ['c1'],
		'b2': []
	};
	var dead = {
		'b1': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 3, 10);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, ['b2', 'b2', 'b2']);
	t.end();
});

mod_tape.test('bug #30', function (t) {
	var spares = {
		'16uN6JsJFild9cHyl2+LSyRHmNc=': ['c1'],
		'c7QG0UOYCpm6m/hYUX0jBenbM70=': ['c2'],
		'ashWtupYHh1QH33UP/T2+6hvi8c=': [],
		'4QMg6SChOmtF8s6lfK32lLoKUFs=': []
	};
	var dead = {
		'c7QG0UOYCpm6m/hYUX0jBenbM70=': true,
		'16uN6JsJFild9cHyl2+LSyRHmNc=': true,
		'4QMg6SChOmtF8s6lfK32lLoKUFs=': true,
		'ashWtupYHh1QH33UP/T2+6hvi8c=': true
	};
	var plan = mod_utils.planRebalance(spares, dead, 3, 4);
	t.deepEqual(plan.remove, []);
	t.deepEqual(plan.add, [
	    'ashWtupYHh1QH33UP/T2+6hvi8c=', '4QMg6SChOmtF8s6lfK32lLoKUFs=']);
	t.end();
});
