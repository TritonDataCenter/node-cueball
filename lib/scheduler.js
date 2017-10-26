/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2017, Joyent, Inc.
 */

module.exports = Scheduler;

const mod_assert = require('assert-plus');
const mod_codel = require('./codel');
const mod_fir = require('./fir');
const mod_utils = require('./utils');
const Queue = require('./queue');

const SCHED_TAPS = mod_fir.genTaps(100, -0.5);

/*
 * The Scheduler implements weighted fair queuing to allow consumers
 * of a pool to guarantee fairness between different kinds of claims,
 * and to provide different kinds of target claim delays.
 *
 * An example use may be to set up different queues for read and writes,
 * so that a large influx of one kind of operation doesn't interrupt
 * the other. Another example would be setting up multiple queues that
 * are hashed onto by a request's source, so that a single service's
 * consumer can't prevent others from also using it.
 */
function Scheduler(qopts, qrequired) {
	mod_assert.array(qopts, 'qopts');
	mod_assert.bool(qrequired, 'qrequired');

	this.sc_qrequired = qrequired;
	this.sc_wsum = 0;
	this.sc_len = 0;

	for (i = 0; i < qopts.length; ++i) {
		this.sc_wsum += qopts[i].weight;
	}

	this.sc_codels = new Array(qopts.length);
	this.sc_queues = new Array(qopts.length);
	this.sc_filters = new Array(qopts.length);
	this.sc_weights = new Array(qopts.length);
	this.sc_lastVirFinish = new Array(qopts.length);

	for (var i = 0; i < qopts.length; ++i) {
		this.sc_codels[i] = Number.isFinite(qopts[i].targetClaimDelay)
		    ? new mod_codel.ControlledDelay(qopts[i].targetClaimDelay)
		    : null;
		this.sc_queues[i] = new Queue();
		this.sc_weights[i] = qopts[i].weight / this.sc_wsum;
		this.sc_filters[i] = new mod_fir.FIRFilter(SCHED_TAPS);
		this.sc_lastVirFinish[i] = 0;
	}
}

/*
 * Get the predicted virtual time that we expect this task to finish at.
 */
Scheduler.prototype._getVirFinish = function (n, now) {
	var virStart = Math.max(now, this.sc_lastVirFinish[n]);
	var virElapse = this.sc_filters[n].get() / this.sc_weights[n];
	return (virStart + virElapse);
};

/*
 * Add a new claim handle to one of the queues.
 */
Scheduler.prototype.enqueue = function (n, handle) {
	mod_assert.number(n, 'n');
	mod_assert.object(handle, 'handle');

	this.sc_queues[n].push(handle);
	this.sc_len += 1;
};

/*
 * Determine the next claim handle we should try. This method returns null when
 * there are no more claims to handle, or when we would have handled a request
 * but it's timed out.
 *
 * dequeue() should be called repeatedly until it returns a non-null object,
 * or the scheduler's length is 0.
 *
 * This is where the bulk of the work is performed for calculating what queue
 * to try next. Since we drop timed out handles from a queue (either due to
 * controlled delay or timeouts provided during .claim()), we need to do these
 * calculations per queue, instead of per "packet" as is normally shown in
 * fair queuing pseudocode when adding new items.
 */
Scheduler.prototype.dequeue = function () {
	var self = this;
	var now = mod_utils.currentMillis();
	var minVirFinish = Infinity;
	var match = -1;

	for (var i = 0; i < self.sc_queues.length; ++i) {
		if (self.sc_queues[i].length === 0) {
			continue;
		}

		var virFinish = self._getVirFinish(i, now);
		if (virFinish < minVirFinish) {
			minVirFinish = virFinish;
			match = i;
		}
	}

	if (match === -1) {
		return (null);
	}

	while (self.sc_queues[match].length > 0) {
		var handle = self.sc_queues[match].shift();
		var drop = self.sc_codels[match] !== null &&
		    self.sc_codels[match].overloaded(handle.ch_started);

		self.sc_len -= 1;

		if (!handle.isInState('waiting')) {
			continue;
		}

		if (drop) {
			handle.timeout();
			continue;
		}

		self.sc_lastVirFinish[match] = minVirFinish;

		handle.on('elapsed', function (elapsed) {
			self.finish(match, elapsed);
		});

		return (handle);
	}

	if (self.sc_codels[match] !== null) {
		self.sc_codels[match].empty();
	}

	return (null);
};

/*
 * When a handle is released, this function should be called with the
 * duration it was checked out to help inform future scheduling for
 * the queue it came from.
 */
Scheduler.prototype.finish = function (n, elapsed) {
	this.sc_filters[n].put(elapsed);
};

/*
 * Drain all of the Scheduler's queues, and call a cleanup function on
 * all pending claims.
 */
Scheduler.prototype.drain = function (cleanup) {
	var queue, waiter;

	for (var i = 0; i < this.sc_queues.length; ++i) {
		queue = this.sc_queues[i];
		while (!queue.isEmpty()) {
			handle = queue.shift();
			cleanup(handle);
		}
	}

	this.sc_len = 0;
};

Scheduler.prototype.getQueue = function (queue) {
	if (!this.sc_qrequired) {
		mod_assert.equal(queue, undefined,
		    '"queue" is only valid when "queues" has been specified');
		return (0);
	}

	mod_utils.assertPositiveInteger(queue, 'options.queue');
	mod_assert.ok(queue < this.sc_queues.length,
	    'queue < this.sc_queues.length');

	return (queue);
};

Scheduler.prototype.getTimeout = function (n, timeout) {
	if (this.sc_codels[n] !== null) {
		if (typeof (timeout) === 'number') {
			throw (new Error('options.timeout not allowed when ' +
			    'targetDelay has been set'));
		}

		return (this.sc_codels[n].getMaxIdle());
	} else if (typeof (timeout) === 'number') {
		return (timeout);
	} else {
		return (Infinity);
	}
};

Object.defineProperty(Scheduler.prototype, 'length', {
	get: function () {
		return (this.sc_len);
	}
});
