/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = Queue;

const mod_assert = require('assert-plus');

function QueueNode(queue, value) {
	this.n_value = value;
	this.n_next = null;
	this.n_prev = null;
	this.n_queue = queue;
}
QueueNode.prototype.insert = function (prev, next) {
	mod_assert.ok(this.n_next === null);
	mod_assert.ok(this.n_prev === null);
	this.n_next = next;
	this.n_prev = prev;
	prev.n_next = this;
	next.n_prev = this;
	this.n_queue.q_len++;
};
QueueNode.prototype.remove = function () {
	mod_assert.ok(this.n_value !== null);
	var next = this.n_next;
	var prev = this.n_prev;
	this.n_next = null;
	this.n_prev = null;
	prev.n_next = next;
	next.n_prev = prev;
	this.n_queue.q_len--;
};

function Queue() {
	this.q_head = new QueueNode(this, null);
	this.q_tail = new QueueNode(this, null);
	this.q_head.n_next = this.q_tail;
	this.q_tail.n_prev = this.q_head;
	this.q_len = 0;
}
Queue.prototype.isEmpty = function () {
	return (this.q_head.n_next === this.q_tail);
};
Queue.prototype.peek = function () {
	return (this.q_head.n_next.n_value);
};
Queue.prototype.push = function (v) {
	var n = new QueueNode(this, v);
	n.insert(this.q_tail.n_prev, this.q_tail);
	return (n);
};
Queue.prototype.shift = function () {
	var n = this.q_head.n_next;
	mod_assert.ok(n.value !== null);
	n.remove();
	return (n.n_value);
};
Queue.prototype.forEach = function (cb) {
	var n = this.q_head.n_next;
	while (n !== this.q_tail) {
		var next = n.n_next;
		cb(n.n_value, n);
		n = next;
	}
};
Object.defineProperty(Queue.prototype, 'length', {
	get: function () {
		return (this.q_len);
	}
});
