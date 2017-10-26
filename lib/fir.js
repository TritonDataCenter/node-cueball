/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2017 Joyent, Inc.
 */

module.exports = {
	genTaps: genTaps,
	FIRFilter: FIRFilter
};

/*
 * Generates a set of EMA taps: count is the number of taps to generate, and
 * tc is the value of the time constant of decay. This should be negative
 * and fractional. The closer it gets to 0.0 (while remaining negative), the
 * lower its low-pass cutoff frequency will be and the sharper the filter
 * roll-off becomes.
 *
 * A tc of -0.2 at 5Hz creates an EMA filter with a pass band extending out
 * to about 0.25Hz (4-second period), -10dB point at 0.5Hz, -20dB point at
 * 2.5Hz. This means that the filter reacts most strongly to trends with
 * principal components of around 4 seconds period or more -- any faster
 * trends will be gradually suppressed, and for trends of 400ms or less, down
 * to about 1% of their original magnitude.
 */
function genTaps(count, tc) {
	var taps = new Float64Array(count);
	var sum = 0.0;
	for (var i = 0; i < count; ++i) {
		taps[i] = Math.exp(tc * i);
		sum += taps[i];
	}
	for (i = 0; i < count; ++i) {
		taps[i] /= sum;
	}
	return (taps);
}

/* A simple FIR filter with a circular buffer. */
function FIRFilter(taps) {
	this.f_taps = taps;
	this.f_buf = new Float64Array(taps.length);
	this.f_ptr = 0;
}

FIRFilter.prototype.put = function (v) {
	this.f_buf[this.f_ptr++] = v;
	/* Wrap around to zero if we go off the end. */
	if (this.f_ptr === this.f_taps.length)
		this.f_ptr = 0;
};

FIRFilter.prototype.get = function () {
	var i = this.f_ptr - 1;
	if (i < 0)
		i += this.f_taps.length;
	var acc = 0.0;
	for (var j = 0; j < this.f_taps.length; ++j) {
		acc += this.f_buf[i] * this.f_taps[j];
		if (--i < 0)
			i += this.f_taps.length;
	}
	return (acc);
};
