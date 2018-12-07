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
	ConnectionClosedError: ConnectionClosedError,
	PoolFailedError: PoolFailedError,
	PoolStoppingError: PoolStoppingError,
	ClaimHandleMisusedError: ClaimHandleMisusedError,
	ConnectionError: ConnectionError
};

const mod_util = require('util');
const mod_assert = require('assert-plus');
const mod_verror = require('verror');
const VError = mod_verror.VError;

function ClaimHandleMisusedError() {
	var opts = {};
	opts.constructorOpt = ClaimHandleMisusedError;
	VError.call(this, opts, 'CueBall claim handle used as if it was a ' +
	    'socket (Check the order and number of arguments in ' +
	    'your claim callbacks)');
}
mod_util.inherits(ClaimHandleMisusedError, VError);
ClaimHandleMisusedError.prototype.name = 'ClaimHandleMisusedError';

function ClaimTimeoutError(pool) {
	var opts = {};
	opts.constructorOpt = ClaimTimeoutError;
	this.pool = pool;
	VError.call(this, opts, 'Timed out while waiting for connection in ' +
	    'pool %s (%s)', pool.p_uuid, pool.p_domain);
}
mod_util.inherits(ClaimTimeoutError, VError);
ClaimTimeoutError.prototype.name = 'ClaimTimeoutError';

function NoBackendsError(pool, cause) {
	var opts = {};
	opts.constructorOpt = NoBackendsError;
	opts.cause = cause;
	this.pool = pool;
	VError.call(this, opts, 'No backends available in pool %s (%s)',
	    pool.p_uuid, pool.p_domain);
}
mod_util.inherits(NoBackendsError, VError);
NoBackendsError.prototype.name = 'NoBackendsError';

function PoolFailedError(pool, cause) {
	var opts = {};
	opts.constructorOpt = PoolFailedError;
	opts.cause = cause;
	this.pool = pool;
	var dead = Object.keys(pool.p_dead).length;
	var avail = pool.p_keys.length;
	VError.call(this, opts, 'Connections to backends of pool %s (%s) are ' +
	    'persistently failing; request aborted (%d of %d declared dead, ' +
	    'in state "failed")', pool.p_uuid.split('-')[0], pool.p_domain,
	    dead, avail);
}
mod_util.inherits(PoolFailedError, VError);
PoolFailedError.prototype.name = 'PoolFailedError';

function PoolStoppingError(pool) {
	var opts = {};
	opts.constructorOpt = PoolStoppingError;
	this.pool = pool;
	VError.call(this, opts, 'Pool %s (%s) is stopping and cannot take ' +
	    'new requests', pool.p_uuid.split('-')[0], pool.p_domain);
}
mod_util.inherits(PoolStoppingError, VError);
PoolStoppingError.prototype.name = 'PoolStoppingError';

function ConnectionError(backend, event, state, cause) {
	var opts = {};
	opts.constructorOpt = ConnectionError;
	opts.cause = cause;
	this.backend = backend;
	VError.call(this, opts, 'Connection to backend %s (%s:%d) emitted ' +
	    '"%s" during %s', backend.name || backend.key, backend.address,
	    backend.port, event, state);
}
mod_util.inherits(ConnectionError, VError);
ConnectionError.prototype.name = 'ConnectionError';

function ConnectionTimeoutError(backend) {
	var opts = {};
	opts.constructorOpt = ConnectionTimeoutError;
	this.backend = backend;
	VError.call(this, opts, 'Connection timed out to backend %s (%s:%d)',
	    backend.name || backend.key, backend.address, backend.port);
}
mod_util.inherits(ConnectionTimeoutError, VError);
ConnectionTimeoutError.prototype.name = 'ConnectionTimeoutError';

function ConnectionClosedError(backend) {
	var opts = {};
	opts.constructorOpt = ConnectionClosedError;
	this.backend = backend;
	VError.call(this, opts, 'Connection closed unexpectedly to backend ' +
	    '%s (%s:%d)', backend.name || backend.key, backend.address,
	    backend.port);
}
mod_util.inherits(ConnectionClosedError, VError);
ConnectionClosedError.prototype.name = 'ConnectionClosedError';
