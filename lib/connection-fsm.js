/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = ConnectionFSM;

const mod_events = require('events');
const mod_net = require('net');
const mod_util = require('util');
const mod_mooremachine = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_utils = require('./utils');
const mod_vasync = require('vasync');
const mod_bunyan = require('bunyan');
const mod_resolver = require('./resolver');
const mod_uuid = require('node-uuid');
const mod_errors = require('./errors');

const mod_monitor = require('./pool-monitor');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

const Queue = require('./queue');

/*
 * ConnectionFSM is the state machine for a "connection" -- an abstract entity
 * that is managed by a ConnectionPool. ConnectionFSMs are associated with a
 * particular 'backend', and hold a backing object cf_conn. Typically this
 * backing object is a TCP socket, but may be any EventEmitter that emits
 * 'close' and 'error'.
 */
function ConnectionFSM(options) {
	mod_assert.object(options, 'options');
	mod_assert.object(options.pool, 'options.pool');
	mod_assert.func(options.constructor, 'options.constructor');
	mod_assert.object(options.backend, 'options.backend');
	mod_assert.object(options.log, 'options.log');
	mod_assert.optionalFunc(options.checker, 'options.checker');
	mod_assert.optionalNumber(options.checkTimeout, 'options.checkTimeout');

	this.cf_pool = options.pool;
	this.cf_constructor = options.constructor;
	this.cf_backend = options.backend;
	this.cf_claimed = false;
	this.cf_claimStack = [];
	this.cf_releaseStack = [];
	this.cf_lastError = undefined;
	this.cf_conn = undefined;
	this.cf_shadow = undefined;
	this.cf_closeAfter = false;
	mod_assert.optionalBool(options.doRef, 'options.doRef');
	this.cf_doRef = options.doRef;
	if (this.cf_doRef === undefined)
		this.cf_doRef = true;
	this.cf_oldListeners = {};
	this.cf_checkTimeout = options.checkTimeout;
	this.cf_checker = options.checker;
	this.cf_lastCheck = new Date();
	this.cf_log = options.log.child({
		backend: this.cf_backend.key
	});

	mod_assert.object(options.recovery, 'options.recovery');

	var connectRecov = options.recovery.default;
	var initialRecov = options.recovery.default;
	if (options.recovery.connect) {
		initialRecov = options.recovery.connect;
		connectRecov = options.recovery.connect;
	}
	if (options.recovery.initial)
		initialRecov = options.recovery.initial;
	mod_utils.assertRecovery(connectRecov, 'recovery.connect');
	mod_utils.assertRecovery(initialRecov, 'recovery.initial');

	this.cf_initialRecov = initialRecov;
	this.cf_connectRecov = connectRecov;

	this.cf_retries = initialRecov.retries;
	this.cf_retriesLeft = initialRecov.retries;
	this.cf_minDelay = initialRecov.delay;
	this.cf_delay = initialRecov.delay;
	this.cf_maxDelay = initialRecov.maxDelay || Infinity;
	this.cf_timeout = initialRecov.timeout;
	this.cf_maxTimeout = initialRecov.maxTimeout || Infinity;

	/*
	 * If our parent pool thinks this backend is dead, resume connection
	 * attempts with the maximum delay and timeout. Something is going
	 * wrong, let's not retry too aggressively and make it worse.
	 */
	if (this.cf_pool.isDeclaredDead(this.cf_backend.key)) {
		/*
		 * We might be given an infinite maxDelay or maxTimeout. If
		 * we are, then multiply it by 2^(retries) to get to what the
		 * value would have been before.
		 */
		var mult = 1 << this.cf_retries;
		this.cf_delay = this.cf_maxDelay;
		if (!isFinite(this.cf_delay))
			this.cf_delay = initialRecov.delay * mult;
		this.cf_timeout = this.cf_maxTimeout;
		if (!isFinite(this.cf_timeout))
			this.cf_timeout = initialRecov.timeout * mult;
		/* Keep retrying a failed backend forever */
		this.cf_retries = Infinity;
		this.cf_retriesLeft = Infinity;
	}

	this.allStateEvent('closeAsserted');

	FSM.call(this, 'init');
}
mod_util.inherits(ConnectionFSM, FSM);

/*
 * Return true if this connection was closed due to retry exhaustion.
 */
ConnectionFSM.prototype.retriesExhausted = function () {
	return (this.isInState('closed') && this.cf_retriesLeft <= 0);
};

ConnectionFSM.prototype.getConnection = function () {
	mod_assert.ok(this.isInState('idle'));
	return (this.cf_conn);
};

/*
 * Mark this Connection as "claimed"; in use by a particular client of the
 * pool.
 *
 * Normally this will be called by the pool itself, which will give the 'stack'
 * argument as a copy of the stack trace from its caller.
 *
 * We keep track of the stack trace of our last claimer and releaser to aid
 * in debugging.
 */
ConnectionFSM.prototype.claim = function (stack, cb) {
	mod_assert.ok(this.cf_claimed === false);
	mod_assert.strictEqual(this.getState(), 'idle');
	if (typeof (stack) === 'function') {
		cb = stack;
		stack = undefined;
	}
	mod_assert.func(cb, 'callback');
	if (stack === undefined) {
		var e = {};
		Error.captureStackTrace(e);
		stack = e.stack;
	}
	this.cf_claimStack = stack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /, '')); });
	this.cf_claimed = true;
	var self = this;
	this.on('stateChanged', onStateChanged);
	function onStateChanged(st) {
		if (st === 'busy' && self.isInState('busy')) {
			self.removeListener('stateChanged', onStateChanged);
			cb(null, self.cf_shadow, self.cf_conn);
		} else if (st !== 'busy') {
			self.removeListener('stateChanged', onStateChanged);
			cb(new Error('Claimed connection entered state ' +
			    st + ' during claim, instead of "busy"'));
		}
	}
	this.emit('claimAsserted');
};

/*
 * Mark this Connection as "free" and ready to be re-used. This is normally
 * called via the ConnectionHandle.
 */
ConnectionFSM.prototype.release = function (cb) {
	mod_assert.ok(this.cf_claimed === true);
	mod_assert.ok(['busy', 'ping'].indexOf(this.getState()) !== -1,
	    'connection is not held');

	var e = {};
	Error.captureStackTrace(e);
	this.cf_releaseStack = e.stack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /, '')); });
	this.once('stateChanged', function (st) {
		mod_assert.notStrictEqual(st, 'busy');
		if (cb)
			cb(null);
	});
	this.emit('releaseAsserted');
};

ConnectionFSM.prototype.close = function (cb) {
	if (cb) {
		this.on('stateChanged', function (st) {
			if (st === 'closed')
				cb();
		});
	}
	this.emit('closeAsserted');
};

ConnectionFSM.prototype.start = function () {
	this.emit('startAsserted');
};

ConnectionFSM.prototype.closeAfterRelease = function () {
	this.cf_closeAfter = true;
};

ConnectionFSM.prototype.state_init = function (S) {
	S.validTransitions(['connect', 'closed']);
	S.on(this, 'startAsserted', function () {
		S.gotoState('connect');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_connect = function (S) {
	S.validTransitions(['error', 'idle', 'closed']);
	var self = this;
	S.timeout(this.cf_timeout, function () {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
	});
	this.cf_conn = this.cf_constructor(this.cf_backend);
	mod_assert.object(this.cf_conn, 'constructor return value');
	this.cf_conn.cf_fsm = this;
	S.on(this.cf_conn, 'connect', function () {
		S.gotoState('idle');
	});
	S.on(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		S.gotoState('error');
		self.cf_pool._incrCounter('error-during-connect');
	});
	S.on(this.cf_conn, 'connectError', function (err) {
		self.cf_lastError = err;
		S.gotoState('error');
		self.cf_pool._incrCounter('error-during-connect');
	});
	S.on(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('close-during-connect');
	});
	S.on(this.cf_conn, 'timeout', function () {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('timeout-during-connect');
	});
	S.on(this.cf_conn, 'connectTimeout', function (err) {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('timeout-during-connect');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_closed = function (S) {
	S.validTransitions([]);
	if (this.cf_conn)
		this.cf_conn.destroy();
	this.cf_conn = undefined;
	this.cf_closeAfter = false;
	this.cf_lastError = undefined;
	this.cf_log.trace('ConnectionFSM closed');
	S.on(this, 'closeAsserted', function () { });
};

ConnectionFSM.prototype.state_error = function (S) {
	S.validTransitions(['delay', 'closed']);

	S.on(this, 'closeAsserted', function () { });

	if (this.cf_conn)
		this.cf_conn.destroy();
	this.cf_conn = undefined;

	if (this.cf_shadow) {
		this.cf_shadow.sh_error = true;
		this.cf_shadow = undefined;
	}

	/*
	 * If the closeAfter flag is set, and this is a connection to a "dead"
	 * backend (i.e., a "monitor" watching to see when it comes back), then
	 * exit now. For an ordinary backend, we don't want to do this,
	 * because we want to give ourselves the opportunity to run out of
	 * retries.
	 *
	 * Otherwise, in a situation where we have two connections that were
	 * created at the same time, one to a failing backend that's already
	 * declared dead, and one to a different failing backend not yet
	 * declared, we may never learn that the second backend is down and
	 * declare it dead. The already declared dead backend may exit first
	 * during a pool reshuffle and cause this one to exit prematurely
	 * (there's a race in who exits first and causes the planner to engage)
	 */
	if (this.cf_retries === Infinity && this.cf_closeAfter) {
		this.cf_retriesLeft = 0;
		S.gotoState('closed');
		return;
	}

	/*
	 * If this backend has been removed from the Resolver, we should not
	 * attempt any kind of reconnection. Exit now.
	 */
	if (!this.cf_pool.shouldRetryBackend(this.cf_backend.key)) {
		S.gotoState('closed');
		return;
	}

	if (this.cf_retries !== Infinity)
		--this.cf_retriesLeft;

	if (this.cf_retries === Infinity || this.cf_retriesLeft > 0) {
		S.gotoState('delay');
	} else {
		this.cf_log.warn(this.cf_lastError, 'failed to connect to ' +
		    'backend %s (%j)', this.cf_backend.key, this.cf_backend);
		this.cf_pool._incrCounter('retries-exhausted');
		S.gotoState('closed');
	}
};

ConnectionFSM.prototype.state_delay = function (S) {
	S.validTransitions(['connect', 'closed']);
	var delay = this.cf_delay;

	this.cf_delay *= 2;
	this.cf_timeout *= 2;
	if (this.cf_timeout > this.cf_maxTimeout)
		this.cf_timeout = this.cf_maxTimeout;
	if (this.cf_delay > this.cf_maxDelay)
		this.cf_delay = this.cf_maxDelay;

	var t = S.timeout(delay, function () {
		S.gotoState('connect');
	});
	t.unref();
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_idle = function (S) {
	S.validTransitions(['busy', 'error', 'closed']);
	var self = this;

	this.cf_claimed = false;
	this.cf_claimStack = [];
	this.cf_log.trace('connected, idling');

	if (this.cf_shadow) {
		this.cf_shadow.sh_claimed = false;
		this.cf_shadow.sh_releaseStack = this.cf_releaseStack;
		this.cf_shadow = undefined;
	}

	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var newCount = countListeners(self.cf_conn, evt);
		var oldCount = self.cf_oldListeners[evt];
		if (oldCount !== undefined && newCount > oldCount) {
			var info = {};
			info.stack = self.cf_releaseStack;
			info.countBeforeClaim = oldCount;
			info.countAfterRelease = newCount;
			info.handlers = self.cf_conn.listeners(evt).map(
			    function (f) {
				/*
				 * node's EventEmitter#once function is actually
				 * equivalent to calling #on() with a wrapper
				 * function named 'g'. #once() sets up a
				 * property 'listener' on these wrappers, which
				 * points at the original handler. It's much
				 * more useful for the user to see the original
				 * function than the 'g'.
				 */
				if (f.name === 'g' && f.listener)
					return (f.listener.toString());
				return (f.toString());
			});
			info.event = evt;
			self.cf_log.warn(info, 'connection claimer looks ' +
			    'like it leaked event handlers');
		}
	});

	/*
	 * Reset retries and retry delay to their defaults since we are now
	 * connected.
	 */
	this.cf_retries = this.cf_connectRecov.retries;
	this.cf_retriesLeft = this.cf_connectRecov.retries;
	this.cf_minDelay = this.cf_connectRecov.delay;
	this.cf_delay = this.cf_connectRecov.delay;
	this.cf_maxDelay = this.cf_connectRecov.maxDelay || Infinity;
	this.cf_timeout = this.cf_connectRecov.timeout;
	this.cf_maxTimeout = this.cf_connectRecov.maxTimeout || Infinity;

	if (this.cf_closeAfter === true) {
		this.cf_closeAfter = false;
		this.cf_lastError = undefined;
		S.on(this, 'closeAsserted', function () { });
		S.gotoState('closed');
		return;
	}

	if (this.cf_doRef)
		this.cf_conn.unref();

	S.on(this, 'claimAsserted', function () {
		S.gotoState('busy');
	});
	S.on(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		S.gotoState('error');
		self.cf_pool._incrCounter('error-during-idle');
	});
	S.on(this.cf_conn, 'close', function () {
		/*
		 * If we receive 'close' while idle with our closeAfter flag
		 * set, we were going to be removed anyway. Just go to closed.
		 * Going to error would give us a chance to learn about a dead
		 * backend, but a clean 'close' with no 'error' is probably
		 * not dead.
		 *
		 * This is particularly important with ConnectionSets, where
		 * this is the normal path that's taken for the Set's consumer
		 * to notify it that this connection has drained and closed.
		 */
		if (self.cf_closeAfter === true) {
			S.gotoState('closed');
		} else {
			self.cf_lastError =
			    new mod_errors.ConnectionClosedError(self);
			S.gotoState('error');
			self.cf_pool._incrCounter('close-during-idle');
		}
	});
	S.on(this.cf_conn, 'end', function () {
		if (self.cf_closeAfter === true) {
			S.gotoState('closed');
		} else {
			self.cf_lastError = new
			    mod_errors.ConnectionClosedError(self);
			S.gotoState('error');
			self.cf_pool._incrCounter('end-during-idle');
		}
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
	if (this.cf_checkTimeout !== undefined) {
		var now = new Date();
		var sinceLast = (now - this.cf_lastCheck);
		var delay;
		if (sinceLast > this.cf_checkTimeout) {
			delay = 1000;
		} else {
			delay = this.cf_checkTimeout - sinceLast;
			if (delay < 1000)
				delay = 1000;
		}
		var t = S.timeout(delay, function () {
			S.gotoState('ping');
		});
		t.unref();
	}
};

ConnectionFSM.prototype.state_ping = function (S) {
	S.validTransitions(['error', 'closed', 'idle']);
	this.cf_lastCheck = new Date();

	this.cf_claimStack = [
	    'ConnectionFSM.prototype.state_ping',
	    '(periodic_health_check)'];
	this.cf_claimed = true;

	var self = this;
	if (this.cf_doRef)
		this.cf_conn.ref();

	this.cf_releaseStack = [];
	this.cf_log.trace('doing health check');

	/*
	 * Write down the count of event handlers on the backing object so that
	 * we can spot if the client leaked any common ones in release().
	 */
	this.cf_oldListeners = {};
	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var count = countListeners(self.cf_conn, evt);
		self.cf_oldListeners[evt] = count;
	});

	/*
	 * The ConnectionHandle is a one-time use object that proxies calls to
	 * our release() and close() functions. We use it so that we can assert
	 * that this particular client only releases us once. If we only
	 * asserted on our current state, there could be a race where we get
	 * claimed by a different client in the meantime.
	 */
	this.cf_shadow = new ConnectionHandle(this);

	S.on(this, 'releaseAsserted', function () {
		if (self.cf_closeAfter === true) {
			S.gotoState('closed');
		} else {
			S.gotoState('idle');
		}
	});
	S.on(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		S.gotoState('error');
		self.cf_pool._incrCounter('error-during-ping');
	});
	S.on(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('close-during-ping');
	});
	S.on(this.cf_conn, 'end', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('end-during-ping');
	});
	S.on(this, 'closeAsserted', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
	});
	var t = S.timeout(this.cf_checkTimeout, function () {
		var info = {};
		info.stack = self.cf_claimStack;
		self.cf_log.warn(info, 'health check is taking too ' +
		    'long to run (has been more than %d ms)',
		    self.cf_checkTimeout);
	});
	t.unref();

	S.immediate(function () {
		self.cf_checker.call(null, self.cf_shadow, self.cf_conn);
	});
};

function countListeners(eve, event) {
	var ls = eve.listeners(event);
	ls = ls.filter(function (h) {
		if (typeof (h) !== 'function')
			return (false);
		/*
		 * Ignore a #once() called freeSocketErrorListener -- in node
		 * v4 and later this is always present as a debugging aide, set
		 * up by the http client framework.
		 */
		if (h.name === 'g' && h.listener &&
		    h.listener.name === 'freeSocketErrorListener') {
			return (false);
		}
		return (true);
	});
	return (ls.length);
}

ConnectionFSM.prototype.state_busy = function (S) {
	S.validTransitions(['error', 'closed', 'idle']);
	var self = this;
	if (this.cf_doRef)
		this.cf_conn.ref();

	this.cf_releaseStack = [];
	this.cf_log.trace('busy, claimed by %s',
	    this.cf_claimStack[1].split(' ')[0]);

	/*
	 * Write down the count of event handlers on the backing object so that
	 * we can spot if the client leaked any common ones in release().
	 */
	this.cf_oldListeners = {};
	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var count = countListeners(self.cf_conn, evt);
		self.cf_oldListeners[evt] = count;
	});

	/*
	 * The ConnectionHandle is a one-time use object that proxies calls to
	 * our release() and close() functions. We use it so that we can assert
	 * that this particular client only releases us once. If we only
	 * asserted on our current state, there could be a race where we get
	 * claimed by a different client in the meantime.
	 */
	this.cf_shadow = new ConnectionHandle(this);

	S.on(this, 'releaseAsserted', function () {
		if (self.cf_closeAfter === true) {
			S.gotoState('closed');
		} else {
			S.gotoState('idle');
		}
	});
	S.on(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		S.gotoState('error');
		self.cf_pool._incrCounter('error-during-busy');
	});
	S.on(this.cf_conn, 'end', function () {
		self.cf_closeAfter = true;
		self.cf_pool._incrCounter('end-during-busy');
	});
	S.on(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
		self.cf_pool._incrCounter('close-during-busy');
	});
	S.on(this, 'closeAsserted', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
	});
	if (this.cf_checkTimeout !== undefined) {
		var t = S.timeout(this.cf_checkTimeout, function () {
			var info = {};
			info.stack = self.cf_claimStack;
			self.cf_log.warn(info, 'connection held for longer ' +
			    'than checkTimeout (%d ms), may have been leaked',
			    self.cf_checkTimeout);
		});
		t.unref();
	}
};

function ConnectionHandle(cf) {
	this.sh_cf = cf;
	this.sh_claimed = true;
	this.sh_error = false;
	this.sh_releaseStack = [];
}
ConnectionHandle.prototype.close = function () {
	mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
	    'this handle, released by ' + this.sh_releaseStack[2]);
	if (this.sh_error) {
		this.sh_claimed = false;
		return (undefined);
	}
	return (this.sh_cf.close.apply(this.sh_cf, arguments));
};
ConnectionHandle.prototype.release = function () {
	mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
	    'this handle, released by ' + this.sh_releaseStack[2]);
	if (this.sh_error) {
		this.sh_claimed = false;
		return (undefined);
	}
	return (this.sh_cf.release.apply(this.sh_cf, arguments));
};
ConnectionHandle.prototype.closeAfterRelease = function () {
	mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
	    'this handle, released by ' + this.sh_releaseStack[2]);
	if (this.sh_error) {
		this.sh_claimed = false;
		return (undefined);
	}
	return (this.sh_cf.closeAfterRelease.apply(this.sh_cf,
	    arguments));
};
