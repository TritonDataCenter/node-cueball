/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	ClaimTimeoutError: ClaimTimeoutError,
	NoBackendsError: NoBackendsError,
	ConnectionTimeoutError: ConnectionTimeoutError,
	ConnectionClosedError: ConnectionClosedError
};

const mod_util = require('util');
const mod_assert = require('assert-plus');

function ClaimTimeoutError(pool) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, ClaimTimeoutError);
	this.pool = pool;
	this.name = 'ClaimTimeoutError';
	this.message = 'Timed out while waiting for connection in pool ' +
	    pool.p_uuid + ' (' + pool.p_domain + ')';
}
mod_util.inherits(ClaimTimeoutError, Error);

function NoBackendsError(pool) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, NoBackendsError);
	this.pool = pool;
	this.name = 'NoBackendsError';
	this.message = 'No backends available in pool ' + pool.p_uuid +
	    ' (' + pool.p_domain + ')';
}
mod_util.inherits(NoBackendsError, Error);

function ConnectionTimeoutError(fsm) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, ConnectionTimeoutError);
	this.fsm = fsm;
	this.backend = fsm.cf_backend;
	this.name = 'ConnectionTimeoutError';
	this.message = 'Connection timed out to backend ' +
	    JSON.stringify(this.backend);
}
mod_util.inherits(ConnectionTimeoutError, Error);

function ConnectionClosedError(fsm) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, ConnectionClosedError);
	this.fsm = fsm;
	this.backend = fsm.cf_backend;
	this.name = 'ConnectionClosedError';
	this.message = 'Connection closed unexpectedly to backend ' +
	    JSON.stringify(this.backend);
}
mod_util.inherits(ConnectionClosedError, Error);
