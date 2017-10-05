/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2017, Joyent, Inc.
 */

module.exports = {
	ControlledDelay: ControlledDelay
};

const mod_assert = require('assert-plus');
const mod_utils = require('./utils');

const CODEL_INTERVAL = 100;

/*
 * This class implements the Controlled Delay algorithm as described
 * here:
 *
 *     https://queue.acm.org/appendices/codel.html
 */
function ControlledDelay(targetClaimDelay) {
	mod_assert.finite(targetClaimDelay, 'targetClaimDelay');

	this.cd_targdelay = targetClaimDelay;
	this.cd_first_above_time = 0;
	this.cd_drop_next = 0;
	this.cd_count = 0;
	this.cd_dropping = false;
}

ControlledDelay.prototype.canDrop = function (now, start) {
	var sojournTime = now - start;

	if (sojournTime < this.cd_targdelay) {
		this.cd_first_above_time = 0;
	} else if (this.cd_first_above_time === 0) {
		this.cd_first_above_time = now + CODEL_INTERVAL;
	} else if (now >= this.cd_first_above_time) {
		return (true);
	}

	return (false);
};

ControlledDelay.prototype.getDropNext = function (now) {
	return (now + CODEL_INTERVAL / Math.sqrt(this.cd_count));
};

/*
 * This method is fed the starting time for each claim, and determines
 * whether we should timeout the claim or not.
 */
ControlledDelay.prototype.overloaded = function (start) {
	mod_assert.number(start, 'start');
	var now = mod_utils.currentMillis();
	var okToDrop = this.canDrop(now, start);
	var dropClaim = false;

	if (this.cd_dropping) {
		if (!okToDrop) {
			this.cd_dropping = false;
		} else if (now >= this.cd_drop_next) {
			dropClaim = true;
			this.cd_count += 1;
		}
	} else if (okToDrop &&
	    ((now - this.cd_drop_next < CODEL_INTERVAL) ||
	     (now - this.cd_first_above_time >= CODEL_INTERVAL))) {
		dropClaim = true;
		this.cd_dropping = true;

		if (now - this.cd_drop_next < CODEL_INTERVAL) {
			this.cd_count = this.cd_count > 2
			    ? this.cd_count - 2 : 1;
		} else {
			this.cd_count = 1;
		}

		this.cd_drop_next = this.getDropNext(now);
	}

	return (dropClaim);
};

/*
 * Called to indicate that the queue has been cleared out.
 */
ControlledDelay.prototype.empty = function () {
	this.cd_last_empty = mod_utils.currentMillis();
	this.cd_first_above_time = 0;
};

/*
 * Get a maximum period of time for a request to sit idle in the queue
 * before timing out. While we normally determine whether to timeout a
 * claim at the point it's dequeued, if checkout durations become very
 * long, then we may not get that opportunity soon enough to maintain
 * our target delay.
 *
 * Because of this, we normally keep this value high in a healthy
 * system, but lower it closer to our target delay when we're
 * overloaded.
 *
 * See http://queue.acm.org/detail.cfm?id=2839461
 */
ControlledDelay.prototype.getMaxIdle = function () {
	var bound = this.cd_targdelay * 10;
	var now = mod_utils.currentMillis();

	if (this.cd_last_empty < (now - bound)) {
		return (this.cd_targdelay * 3);
	} else {
		return (bound);
	}
};
