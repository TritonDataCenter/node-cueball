/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2017 Joyent, Inc.
 */

module.exports = {
	ConnectionSlotFSM: ConnectionSlotFSM,
	CueBallClaimHandle: CueBallClaimHandle
};

const mod_events = require('events');
const mod_net = require('net');
const mod_util = require('util');
const mod_mooremachine = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_utils = require('./utils');
const mod_vasync = require('vasync');
const mod_verror = require('verror');
const mod_bunyan = require('bunyan');
const mod_resolver = require('./resolver');
const mod_errors = require('./errors');

const mod_monitor = require('./pool-monitor');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

const Queue = require('./queue');

/*
 * This file contains 3 key FSMs used to manage connections:
 *  - SocketMgrFSM
 *  - ConnectionSlotFSM
 *  - CueBallClaimHandle (also an FSM, despite the name)
 *
 * Only the ConnectionSlotFSM and CueBallClaimHandle are exported and used
 * elsewhere in cueball. The SocketMgrFSM is an implementation detail of the
 * ConnectionSlotFSM (which is the only thing that constructs and manages it).
 *
 * Pools and Sets construct one ConnectionSlotFSM for each "slot" they have
 * (a "slot" being a position in their list of connections, sort of like a
 * vnode versus pnode in consistent hashing terminology).
 *
 * Pools and Sets also can construct ClaimHandles, which attempt to "claim" a
 * slot. Once claimed, the ClaimHandle will call a callback and then hold that
 * slot until a release() or close() method on it is called. It is used to
 * manage the process of "giving out" connections to downstream users of
 * cueball (this process involves multiple steps and possible state-change
 * races, so it is encapsulated in an FSM).
 *
 * Note on state transition diagrams in this file:
 *
 * In the condition notation on edges:
 *    'connect'   -- means "when the event 'connect' is emitted"
 *    .connect()  -- means "when the signal function .connect() is called"
 *    unwanted    -- means "if the flag 'unwanted' is true" or
 *                         "when the flag 'unwanted' changes to true"
 *
 * Generally the first condition on an edge is the trigger condition -- we will
 * only transition when it changes. The subsequent conditions joined by && are
 * evaluated only after a change happens in the first condition.
 */


/*
 * The "Socket Manager FSM" (SocketMgrFSM) is an abstraction for a "socket"/
 * connection, which handles retry and backoff, as well as de-duplication of
 * error and closure related events.
 *
 * A single SocketMgrFSM may construct multiple sockets over its lifetime --
 * it creates them by calling the "constructor" function passed in from the
 * public-facing API (e.g. the ConnectionPool options). Every time we enter the
 * "connecting" state we will construct a new socket, and we destroy it in
 * "error" or "closed".
 *
 * A SocketMgrFSM is constructed and managed only by a ConnectionSlotFSM
 * (there is a 1:1 relationship between the two). Many points in the SocketMgr
 * state graph stop and wait for signals from the SlotFSM on how to proceed.
 *
 * These signals are "signal functions" in the mooremachine style -- see
 * SocketMgrFSM.prototype.connect() and .retry().
 *
 *                                   +------------+
 *                                   |            |
 *                                   |   failed   |
 *              +------+             |            |
 *              | init |             +------------+
 *              +------+                ^
 *                  |                   |
 *                  |                   |retries
 *                  v                   |exhausted
 *           +------------+          +--+---------+
 *           |            |   timeout|            |
 *   +-----> | connecting | <--------+  backoff   | <--+
 *   |       |            |          |            |    |
 *   |       +--+----+----+          +------------+    |
 *   |          |    |                  ^              |
 *   | 'connect'|    | timeout/'error'  |              |
 *   |          |    +---------+        |              |
 *   |          v              |        | .retry()     |
 *   |       +------------+    |     +--+---------+    |
 *   |       |            |    +---> |            |    |
 *   |       | connected  +--------> |   error    |    |
 *   |       |            | 'error'  |            |    |
 *   |       +--+---------+          +------------+    |
 *   |   'close'|                                      |
 *   |  .close()|                                      |
 *   |          |                                      |
 *   |          v                                      |
 *   |       +------------+                            |
 *   |       |            |                            |
 *   +-------+   closed   +----------------------------+
 * .connect()|            | .retry()
 *           +------------+
 */
function SocketMgrFSM(options) {
	mod_assert.object(options, 'options');
	mod_assert.func(options.constructor, 'options.constructor');
	mod_assert.object(options.backend, 'options.backend');
	mod_assert.object(options.log, 'options.log');
	mod_assert.object(options.slot, 'options.slot');
	mod_assert.bool(options.monitor, 'options.monitor');
	mod_assert.object(options.pool, 'options.pool');

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

	this.sm_initialRecov = initialRecov;
	this.sm_connectRecov = connectRecov;

	this.sm_pool = options.pool;
	this.sm_backend = options.backend;
	this.sm_constructor = options.constructor;
	this.sm_slot = options.slot;

	this.sm_log = options.log.child({
		component: 'CueBallSocketMgrFSM',
		backend: this.sm_backend.key,
		address: this.sm_backend.address,
		port: this.sm_backend.port
	});

	this.sm_lastError = undefined;
	this.sm_socket = undefined;

	/*
	 * We start off in the "init" state, waiting for the SlotFSM to call
	 * connect().
	 */
	FSM.call(this, 'init');

	this.sm_monitor = undefined;
	this.setMonitor(options.monitor);
}
mod_util.inherits(SocketMgrFSM, FSM);

/*
 * Sets whether we are in "monitor" mode or not. In "monitor" mode the
 * SocketMgrFSM has infinite retries and does not use exponential back-off
 * (timeout and delay are fixed at their max values).
 */
SocketMgrFSM.prototype.setMonitor = function (value) {
	mod_assert.ok(this.isInState('init') || this.isInState('connected'));
	if (value === this.sm_monitor)
		return;
	this.sm_monitor = value;
	this.resetBackoff();
};

SocketMgrFSM.prototype.resetBackoff = function () {
	var initialRecov = this.sm_initialRecov;

	this.sm_retries = initialRecov.retries;
	this.sm_retriesLeft = initialRecov.retries;
	this.sm_minDelay = initialRecov.delay;
	this.sm_delay = initialRecov.delay;
	this.sm_maxDelay = initialRecov.maxDelay || Infinity;
	this.sm_timeout = initialRecov.timeout;
	this.sm_maxTimeout = initialRecov.maxTimeout || Infinity;

	if (this.sm_monitor === true) {
		var mult = 1 << this.sm_retries;
		this.sm_delay = this.sm_maxDelay;
		if (!isFinite(this.sm_delay))
			this.sm_delay = initialRecov.delay * mult;
		this.sm_timeout = this.sm_maxTimeout;
		if (!isFinite(this.sm_timeout))
			this.sm_timeout = initialRecov.timeout * mult;
		/* Keep retrying a failed backend forever */
		this.sm_retries = Infinity;
		this.sm_retriesLeft = Infinity;
	}
};

SocketMgrFSM.prototype.connect = function () {
	mod_assert.ok(this.isInState('init') || this.isInState('closed'),
	    'SocketMgrFSM#connect may only be called in state "init" or ' +
	    '"closed" (is in "' + this.getState() + '")');
	this.emit('connectAsserted');
};

SocketMgrFSM.prototype.retry = function () {
	mod_assert.ok(this.isInState('closed') || this.isInState('error'),
	    'SocketMgrFSM#retry may only be called in state "closed" or ' +
	    '"error" (is in "' + this.getState() + '")');
	this.emit('retryAsserted');
};

SocketMgrFSM.prototype.close = function () {
	mod_assert.ok(this.isInState('connected') || this.isInState('backoff'),
	    'SocketMgrFSM#close may only be called in state "connected" or ' +
	    '"backoff" (is in "' + this.getState() + '")');
	this.emit('closeAsserted');
};

SocketMgrFSM.prototype.getLastError = function () {
	return (this.sm_lastError);
};

SocketMgrFSM.prototype.getSocket = function () {
	mod_assert.ok(this.isInState('connected'), 'Sockets may only be ' +
	    'retrieved from SocketMgrFSMs in "connected" state (is in ' +
	    'state "' + this.getState() + '")');
	return (this.sm_socket);
};

SocketMgrFSM.prototype.state_init = function (S) {
	S.validTransitions(['connecting']);
	S.on(this, 'connectAsserted', function () {
		S.gotoState('connecting');
	});
};

SocketMgrFSM.prototype.state_connecting = function (S) {
	S.validTransitions(['connected', 'error']);
	var self = this;

	S.timeout(this.sm_timeout, function () {
		self.sm_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
		self.sm_pool._incrCounter('timeout-during-connect');
	});

	this.sm_log.trace('calling constructor to open new connection');
	this.sm_socket = this.sm_constructor(this.sm_backend);
	mod_assert.object(this.sm_socket, 'constructor return value');
	this.sm_socket.sm_fsm = this;

	S.on(this.sm_socket, 'connect', function () {
		S.gotoState('connected');
	});

	S.on(this.sm_socket, 'error', function socketMgrErrorListener(err) {
		self.sm_lastError = err;
		S.gotoState('error');
		self.sm_log.trace(err, 'emitted error while connecting');
		self.sm_pool._incrCounter('error-during-connect');
	});
	S.on(this.sm_socket, 'connectError', function (err) {
		self.sm_lastError = err;
		S.gotoState('error');
		self.sm_log.trace(err, 'emitted connectError while connecting');
		self.sm_pool._incrCounter('error-during-connect');
	});
	S.on(this.sm_socket, 'close', function () {
		self.sm_lastError = new mod_errors.ConnectionClosedError(self);
		S.gotoState('error');
		self.sm_log.trace('closed while connecting');
		self.sm_pool._incrCounter('close-during-connect');
	});
	S.on(this.sm_socket, 'timeout', function () {
		self.sm_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
		self.sm_log.trace('timed out while connecting');
		self.sm_pool._incrCounter('timeout-during-connect');
	});
	S.on(this.sm_socket, 'connectTimeout', function () {
		self.sm_lastError = new mod_errors.ConnectionTimeoutError(self);
		S.gotoState('error');
		self.sm_log.trace('timed out while connecting');
		self.sm_pool._incrCounter('timeout-during-connect');
	});
};

SocketMgrFSM.prototype.state_connected = function (S) {
	S.validTransitions(['error', 'closed']);
	var self = this;

	if (typeof (self.sm_socket.localPort) === 'number') {
		this.sm_log = this.sm_log.child({
			localPort: this.sm_socket.localPort
		});
	}

	this.sm_log.trace('connected');

	this.resetBackoff();

	S.on(this.sm_socket, 'error', function socketMgrErrorListener(err) {
		self.sm_lastError = err;
		S.gotoState('error');
		self.sm_pool._incrCounter('error-while-connected');
		self.sm_log.trace(err, 'emitted error while connected');
	});
	S.on(this.sm_socket, 'close', function () {
		S.gotoState('closed');
	});

	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

SocketMgrFSM.prototype.state_error = function (S) {
	S.validTransitions(['backoff']);
	if (this.sm_socket) {
		this.sm_socket.destroy();
		this.sm_log = this.sm_log.child({ localPort: null });
	}
	this.sm_socket = undefined;

	S.on(this, 'retryAsserted', function () {
		S.gotoState('backoff');
	});
};

SocketMgrFSM.prototype.state_backoff = function (S) {
	S.validTransitions(['failed', 'connecting', 'closed']);

	/*
	 * Unfortunately, "retries" actually means "attempts" in the cueball
	 * API. To preserve compatibility we have to compare to 1 here instead
	 * of 0.
	 */
	if (this.sm_retriesLeft !== Infinity && this.sm_retriesLeft <= 1) {
		S.gotoState('failed');
		return;
	}

	var delay = this.sm_delay;

	if (this.sm_retries !== Infinity) {
		--this.sm_retriesLeft;

		this.sm_delay *= 2;
		this.sm_timeout *= 2;
		if (this.sm_timeout > this.sm_maxTimeout)
			this.sm_timeout = this.sm_maxTimeout;
		if (this.sm_delay > this.sm_maxDelay)
			this.sm_delay = this.sm_maxDelay;
	}

	S.timeout(delay, function () {
		S.gotoState('connecting');
	});

	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

SocketMgrFSM.prototype.state_closed = function (S) {
	S.validTransitions(['backoff', 'connecting']);
	var log = this.sm_log;
	if (this.sm_socket) {
		this.sm_socket.destroy();
		this.sm_log = this.sm_log.child({ localPort: null });
	}
	this.sm_socket = undefined;

	log.trace('connection closed');

	S.on(this, 'retryAsserted', function () {
		S.gotoState('backoff');
	});
	S.on(this, 'connectAsserted', function () {
		S.gotoState('connecting');
	});
};

SocketMgrFSM.prototype.state_failed = function (S) {
	S.validTransitions([]);
	this.sm_log.warn(this.sm_lastError, 'failed to connect to ' +
	    'backend, retries exhausted');
	this.sm_pool._incrCounter('retries-exhausted');
};

/*
 * ClaimHandle is a state machine representing a "claim handle". We give
 * these out to clients of the Cueball pool, so that they can call either
 * release() or close() on them when their use of the connection is complete.
 *
 * They also handle the process of finding a Slot in the pool to fulfill the
 * claim and getting the connection successfully returned to the user.
 *
 * Some ancilliary responsibilities relate to the user interface features we
 * support, like detecting leaked event handlers on re-useable connections and
 * cancelling the process mid-way.
 *
 * A ClaimHandle is created immediately when .claim() is called on a pool.
 * The pool then chooses slots that it will attempt to use to fulfill the claim.
 * Each time the pool tries another slot, it calls .try() on the ClaimHandle.
 * The SlotFSM will then either call .accept() or .reject() on the handle. If
 * .reject() is called, we try another slot. If .accept() is called, we proceed
 * to state "claimed". From "claimed", the end-user of the cueball pool calls
 * either one of .release() or .close() to relinquish their claim.
 *
 *               timeout               +--------+
 *              +--------------------> |        |
 *              |                      |        |
 *              |                      | failed |
 *         +----+----+ .fail()         |        |
 *         |         +---------------> |        |
 *         |         |                 +--------+
 * init -> | waiting |
 *         |         | .cancel()
 *         |         +---------------------+
 *         |         |                     |
 *         +-+-------+                     |
 *    .try() |    ^                        |
 *           |    |                        |
 *           |    |                        v
 *           |    |                  +-----------+
 *           |    |                  |           |
 *           |    |                  | cancelled |
 *           |    |                  |           |
 *           |    | .reject()        +-----------+
 *           v    | && !cancel             ^
 *         +------+---+                    |
 *         |          |                    |
 *         | claiming +--------------------+
 *         |          | .reject() &&       ^
 *         +----+-----+ cancel             |
 *    .accept() |                          |
 *              |             +------------+
 *              |             |
 *              v             |
 *         +---------+ cancel |
 *         |         +--------+      +--------+
 *         | claimed |               |        |
 *         |         | .close()      | closed |
 *         |         +-------------> |        |
 *         +----+----+               +--------+
 *              | .release()
 *              |
 *              |
 *              v
 *         +----------+
 *         |          |
 *         | released |
 *         |          |
 *         +----------+
 *
 */
function CueBallClaimHandle(options) {
	mod_assert.object(options, 'options');

	mod_assert.number(options.claimTimeout, 'options.claimTimeout');
	this.ch_claimTimeout = options.claimTimeout;

	mod_assert.object(options.pool, 'options.pool');
	this.ch_pool = options.pool;

	mod_assert.optionalBool(options.throwError, 'options.throwError');
	this.ch_throwError = options.throwError;
	if (options.throwError === undefined || options.throwError === null)
		this.ch_throwError = true;

	mod_assert.string(options.claimStack, 'options.claimStack');
	this.ch_claimStack = options.claimStack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /, '')); });

	/* The user callback provided to Pool#claim() */
	mod_assert.func(options.callback, 'options.callback');
	this.ch_callback = options.callback;

	mod_assert.object(options.log, 'options.log');
	this.ch_log = options.log.child({
		component: 'CueBallClaimHandle'
	});

	this.ch_slot = undefined;
	this.ch_releaseStack = undefined;
	this.ch_connection = undefined;
	this.ch_preListeners = {};
	this.ch_cancelled = false;
	this.ch_lastError = undefined;
	this.ch_doReleaseLeakCheck = true;

	FSM.call(this, 'waiting');
}
mod_util.inherits(CueBallClaimHandle, FSM);

Object.defineProperty(CueBallClaimHandle.prototype, 'writable', {
	get: function () {
		throw (new mod_errors.ClaimHandleMisusedError());
	}
});

Object.defineProperty(CueBallClaimHandle.prototype, 'readable', {
	get: function () {
		throw (new mod_errors.ClaimHandleMisusedError());
	}
});

CueBallClaimHandle.prototype.disableReleaseLeakCheck = function () {
	this.ch_doReleaseLeakCheck = false;
};

CueBallClaimHandle.prototype.on = function (evt) {
	if (evt === 'readable' || evt === 'close') {
		throw (new mod_errors.ClaimHandleMisusedError());
	}
	return (EventEmitter.prototype.on.apply(this, arguments));
};

CueBallClaimHandle.prototype.once = function (evt) {
	if (evt === 'readable' || evt === 'close') {
		throw (new mod_errors.ClaimHandleMisusedError());
	}
	return (EventEmitter.prototype.once.apply(this, arguments));
};

CueBallClaimHandle.prototype.try = function (slot) {
	mod_assert.ok(this.isInState('waiting'), 'ClaimHandle#try may only ' +
	    'be called in state "waiting" (is in "' + this.getState() + '")');
	mod_assert.ok(slot.isInState('idle'), 'ClaimHandle#try may only ' +
	    'be called on a slot in state "idle" (is in "' + slot.getState() +
	    '")');
	this.ch_slot = slot;
	this.emit('tryAsserted');
};

CueBallClaimHandle.prototype.accept = function (connection) {
	mod_assert.ok(this.isInState('claiming'));
	this.ch_connection = connection;
	this.emit('accepted');
};

CueBallClaimHandle.prototype.reject = function () {
	mod_assert.ok(this.isInState('claiming'));
	this.emit('rejected');
};

CueBallClaimHandle.prototype.cancel = function () {
	if (this.isInState('claimed')) {
		this.release();
	} else {
		this.ch_cancelled = true;
		this.emit('cancelled');
	}
};

CueBallClaimHandle.prototype.fail = function (err) {
	this.emit('error', err);
};

CueBallClaimHandle.prototype.relinquish = function (event) {
	if (!this.isInState('claimed')) {
		if (this.isInState('released') || this.isInState('closed')) {
			var err = new Error('Connection not claimed by ' +
			    'this handle, released by ' +
			    this.ch_releaseStack[2]);
			throw (err);
		}
		throw (new Error('ClaimHandle#release() called while in ' +
		    'state "' + this.getState() + '"'));
	}
	var e = mod_utils.maybeCaptureStackTrace();
	this.ch_releaseStack = e.stack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /, '')); });
	this.emit(event);
};

CueBallClaimHandle.prototype.release = function () {
	this.relinquish('releaseAsserted');
};

CueBallClaimHandle.prototype.close = function () {
	this.relinquish('closeAsserted');
};

CueBallClaimHandle.prototype.state_waiting = function (S) {
	S.validTransitions(['claiming', 'cancelled', 'failed']);
	var self = this;

	this.ch_slot = undefined;

	S.on(this, 'tryAsserted', function () {
		S.gotoState('claiming');
	});

	if (isFinite(this.ch_claimTimeout)) {
		S.timeout(this.ch_claimTimeout, function () {
			self.ch_lastError = new mod_errors.ClaimTimeoutError(
			    self.ch_pool);
			S.gotoState('failed');
		});
	}

	S.on(this, 'error', function (err) {
		self.ch_lastError = err;
		S.gotoState('failed');
	});

	S.on(this, 'cancelled', function () {
		S.gotoState('cancelled');
	});
};

CueBallClaimHandle.prototype.state_claiming = function (S) {
	S.validTransitions(['claimed', 'waiting']);
	var self = this;

	S.on(this, 'accepted', function () {
		S.gotoState('claimed');
	});

	S.on(this, 'rejected', function () {
		if (self.ch_cancelled) {
			S.gotoState('cancelled');
		} else {
			S.gotoState('waiting');
		}
	});

	this.ch_slot.claim(this);
};

CueBallClaimHandle.prototype.state_claimed = function (S) {
	var self = this;
	S.validTransitions(['released', 'closed']);

	S.on(this, 'releaseAsserted', function () {
		S.gotoState('released');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});

	if (this.ch_cancelled) {
		S.gotoState('released');
		return;
	}

	this.ch_preListeners = {};
	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var count = countListeners(self.ch_connection, evt);
		self.ch_preListeners[evt] = count;
	});

	S.on(this.ch_connection, 'error', function clHandleErrorListener(err) {
		var count = countListeners(self.ch_connection, 'error');
		if (count === 0 && self.ch_throwError) {
			/*
			 * Our end-user never set up an 'error' event listener
			 * and the socket emitted 'error'. We want to act like
			 * nothing is listening for it at all and throw.
			 */
			throw (err);
		}
		self.ch_log.warn(err, 'connection emitted error while ' +
		    'claimed (for claim callback "%s")', self.ch_callback.name);
		self.ch_pool._incrCounter('error-while-claimed');
	});

	var lport = undefined;
	if (typeof (this.ch_connection.localPort) === 'number')
		lport = this.ch_connection.localPort;
	this.ch_log = this.ch_slot.makeChildLogger({
		component: 'CueBallClaimHandle',
		localPort: lport
	});

	this.ch_callback(null, this, this.ch_connection);
};

CueBallClaimHandle.prototype.state_released = function (S) {
	S.validTransitions([]);

	if (!this.ch_doReleaseLeakCheck)
		return;

	var conn = this.ch_connection;
	var self = this;

	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var newCount = countListeners(conn, evt);
		var oldCount = self.ch_preListeners[evt];
		if (oldCount !== undefined && newCount > oldCount) {
			var info = {};
			info.stack = self.ch_stack;
			info.countBeforeClaim = oldCount;
			info.countAfterRelease = newCount;
			info.handlers = conn.listeners(evt).map(
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
			self.ch_log.warn(info, 'connection claimer looks ' +
			    'like it leaked event handlers');
		}
	});
};

CueBallClaimHandle.prototype.state_closed = function (S) {
	S.validTransitions([]);
	/*
	 * Don't bother to check for leaked event handlers here, as the
	 * connection is being closed anyway.
	 */
};

CueBallClaimHandle.prototype.state_cancelled = function (S) {
	S.validTransitions([]);
	/*
	 * The public API requires that the callback function not be called at
	 * all when .cancel() has been used. So we do nothing here.
	 */
};

CueBallClaimHandle.prototype.state_failed = function (S) {
	S.validTransitions([]);
	var self = this;
	S.immediate(function () {
		self.ch_callback(self.ch_lastError);
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
		/* Don't count the error listeners set up by cueball. */
		if (h.name === 'socketMgrErrorListener' ||
		    h.name === 'clHandleErrorListener') {
			return (false);
		}
		return (true);
	});
	return (ls.length);
}

/*
 * ConnectionSlotFSM is a large part of the guts of cueball connection
 * management. Its primary job is to construct and manage a SocketMgrFSM,
 * making decisions about when to retry and what to do when it's time to
 * give up.
 *
 * It also handles reporting to the Pool or Set that hosts this slot, as its
 * state graph represents only the transitions that the Pool or Set actually
 * cares about.
 *
 * SlotFSMs have two key flags passed in by the Pool or Set: "wanted" and
 * "monitor". "Monitor" indicates that this connection is a monitor connection
 * (i.e. the backend that it is connecting to is expected to be dead, and this
 * FSM's job is to monitor it for when it comes back). The "wanted" flag is
 * always true at construction but may be set to false by the Pool or Set. When
 * so set, it indicates that this slot is no longer "wanted" and should cease
 * operation as soon as possible (by moving to "failed" or "stopped").
 *
 *   init (startup)
 *    |                           +--------+
 *    |             +-----------> | failed | <-----------+
 *    |             |             +--------+             |
 *    |             |                                    |
 *    |             |                                    |      +---------+
 *    |        smgr |                               smgr |      |         |
 *    |      failed |                             failed |      |         |
 *    |             |                                    |      v    smgr |
 *    |    +--------+-------+                    +-------+--------+ error |
 *    +--> |   connecting   |     smgr error     |    retrying    +-------+
 * (A)---> |                +------------------> |                |
 *         | smgr.connect() |                    |  smgr.retry()  | <-+
 *         +-------+--------+                    +-------+--------+   |
 *                 |  ^                               ^  |            |
 *            smgr |  |                               |  | smgr       |
 *       connected |  |                               |  | connected  |
 *                 |  |     smgr              smgr    |  |            |
 *                 |  |   closed  +--------+  error   |  |            |
 *                 |  +-----------+        +----------+  |            |
 *                 |              |        |             |            |
 *                 +------------> |  idle  | <-----------+            |
 *                                |        |                          |
 *         +----------------------+        | <-----------+            |
 *         |     !wanted && smgr  +---+----+             |            |
 *         |           connected      | .claim()         |            |
 *         v                          |                  |            |
 *  +--------------+                  |                  |            |
 *  |   stopping   |                  v                  |            |
 *  |              |              +--------+             |            |
 *  | smgr.close() | <------------+        +-------------+            |
 *  +------+-------+  hdl rel. && |        | hdl released &&          |
 *         |         smgr conn && |        | smgr connected &&        |
 *   !smgr |              !wanted |        | wanted                   |
 * connctd |                      |        |                          |
 *         |                      |  busy  +----------------->(A)     |
 *         v                      |        | hdl released &&          |
 *    +---------+                 |        | smgr closed              |
 *    | stopped | <---------------+        |                          |
 *    +---------+     hdl rel. && |        +--------------------------+
 *                !(smgr conn) && |        | hdl closed &&            ^
 *                        !wanted +-----+--+ !(smgr connected)        |
 *                                      |                             |
 *                                      | hdl closed &&               |
 *                                      | smgr connected              |
 *                                      |                             |
 *                                      v                             |
 *                                    +--------------+                |
 *                                    |   killing    +----------------+
 *                                    |              | !(smgr connected)
 *                                    | smgr.close() |
 *                                    +--------------+
 *
 * Note that "!(smgr connected)" is often expanded out as (smgr closed) ||
 * (smgr error), since 'closed' and 'error' are the two states the smgr pauses
 * in when disconnected.
 *
 * Transitions out of the 'busy' state have to deal with the fact that they are
 * entered on a handle transition and exited on an smgr transition, so they have
 * to take a possible 'error' or 'closed' transition into consideration that
 * they might not yet have observed the event for (it's still on the toEmit
 * list, since it happened in the same event loop spin as the handle event).
 */
function ConnectionSlotFSM(options) {
	mod_assert.object(options, 'options');
	mod_assert.object(options.pool, 'options.pool');
	mod_assert.func(options.constructor, 'options.constructor');
	mod_assert.object(options.backend, 'options.backend');
	mod_assert.object(options.log, 'options.log');
	mod_assert.object(options.recovery, 'options.recovery');
	mod_assert.bool(options.monitor, 'options.monitor');

	mod_assert.optionalFunc(options.checker, 'options.checker');
	mod_assert.optionalNumber(options.checkTimeout, 'options.checkTimeout');

	this.csf_pool = options.pool;
	this.csf_backend = options.backend;
	this.csf_wanted = true;
	this.csf_handle = undefined;
	this.csf_prevHandle = undefined;
	this.csf_monitor = options.monitor;

	this.csf_checker = options.checker;
	this.csf_checkTimeout = options.checkTimeout;

	this.csf_log = options.log.child({
		component: 'CueBallConnectionSlotFSM',
		backend: this.csf_backend.key,
		address: this.csf_backend.address,
		port: this.csf_backend.port
	});

	var smgrOpts = {
		pool: options.pool,
		constructor: options.constructor,
		backend: options.backend,
		log: options.log,
		recovery: options.recovery,
		monitor: options.monitor,
		slot: this
	};
	this.csf_smgr = new SocketMgrFSM(smgrOpts);

	FSM.call(this, 'init');
}
mod_util.inherits(ConnectionSlotFSM, FSM);

ConnectionSlotFSM.prototype.setUnwanted = function () {
	if (this.csf_wanted === false)
		return;
	this.csf_wanted = false;
	this.emit('unwanted');
};

ConnectionSlotFSM.prototype.start = function () {
	mod_assert.ok(this.isInState('init'));
	this.emit('startAsserted');
};

ConnectionSlotFSM.prototype.claim = function (handle) {
	mod_assert.ok(this.isInState('idle'));
	mod_assert.strictEqual(this.csf_handle, undefined);
	this.csf_handle = handle;
	this.emit('claimAsserted');
};

ConnectionSlotFSM.prototype.makeChildLogger = function (args) {
	return (this.csf_log.child(args));
};

ConnectionSlotFSM.prototype.getSocketMgr = function () {
	return (this.csf_smgr);
};

ConnectionSlotFSM.prototype.isRunningPing = function () {
	return (this.isInState('busy') && this.csf_handle &&
	    this.csf_handle.csf_pinger);
};

ConnectionSlotFSM.prototype.state_init = function (S) {
	S.on(this, 'startAsserted', function () {
		S.gotoState('connecting');
	});
};

ConnectionSlotFSM.prototype.state_connecting = function (S) {
	S.validTransitions(['failed', 'retrying', 'idle']);
	var smgr = this.csf_smgr;
	S.on(smgr, 'stateChanged', function (st) {
		switch (st) {
		case 'init':
		case 'connecting':
			break;
		case 'failed':
			S.gotoState('failed');
			break;
		case 'error':
			S.gotoState('retrying');
			break;
		case 'connected':
			S.gotoState('idle');
			break;
		default:
			throw (new Error('Unhandled smgr state transition: ' +
			    '.connect() => "' + st + '"'));
		}
	});
	smgr.connect();
};

ConnectionSlotFSM.prototype.state_failed = function (S) {
	S.validTransitions([]);
	mod_assert.ok(this.csf_smgr.isInState('failed'),
	    'smgr must be failed');
};

ConnectionSlotFSM.prototype.state_retrying = function (S) {
	S.validTransitions(['idle', 'failed', 'retrying', 'stopped',
	    'stopping']);
	var self = this;
	var smgr = this.csf_smgr;
	S.on(smgr, 'stateChanged', function (st) {
		switch (st) {
		case 'backoff':
		case 'connecting':
			break;
		case 'failed':
			S.gotoState('failed');
			break;
		case 'error':
			if (self.csf_monitor && !self.csf_wanted) {
				S.gotoState('stopped');
			} else {
				S.gotoState('retrying');
			}
			break;
		case 'connected':
			S.gotoState('idle');
			break;
		default:
			throw (new Error('Unhandled smgr state transition: ' +
			    '.retry() => "' + st + '"'));
		}
	});
	S.on(this, 'unwanted', function () {
		if (self.csf_monitor && smgr.isInState('backoff')) {
			S.gotoState('stopping');
		}
	});
	smgr.retry();
};

ConnectionSlotFSM.prototype.state_idle = function (S) {
	var self = this;
	var smgr = this.csf_smgr;

	if (this.csf_handle !== undefined)
		this.csf_prevHandle = this.csf_handle;
	this.csf_handle = undefined;

	/* Monitor successfully connected: make it into a normal slot now. */
	if (this.csf_monitor === true) {
		this.csf_monitor = false;
		smgr.setMonitor(false);
	}

	if (!this.csf_wanted) {
		onUnwanted();
		return;
	}
	S.on(this, 'unwanted', onUnwanted);

	function onUnwanted() {
		if (smgr.isInState('connected')) {
			S.gotoState('stopping');
		}
	}

	S.on(smgr, 'stateChanged', function (st) {
		switch (st) {
		case 'error':
			S.gotoState('retrying');
			break;
		case 'closed':
			if (!self.csf_wanted) {
				S.gotoState('stopped');
			} else {
				S.gotoState('connecting');
			}
			break;
		default:
			throw (new Error('Unhandled smgr state transition: ' +
			    'connected => "' + st + '"'));
		}
	});

	S.on(this, 'claimAsserted', function () {
		S.gotoState('busy');
	});

	if (this.csf_checkTimeout !== undefined &&
	    this.csf_checker !== undefined) {
		S.timeout(this.csf_checkTimeout, function () {
			doPingCheck(self, self.csf_checker);
		});
	}
};

function doPingCheck(fsm, checker) {
	var hdlOpts = {
		pool: fsm.csf_pool,
		claimStack: 'Error\n' +
		    'at claim\n' +
		    'at cueball.doPingCheck\n' +
		    'at cueball.doPingCheck\n',
		callback: function pingCheckAdapter(err, hdl, conn) {
			/*
			 * Since we give an infinite timeout and never call
			 * .fail() on the handle, we should never be called
			 * with a non-null "err" parameter here.
			 */
			mod_assert.strictEqual(err, null);
			checker(hdl, conn);
		},
		log: fsm.csf_log,
		claimTimeout: Infinity
	};
	var handle = new CueBallClaimHandle(hdlOpts);
	handle.csf_pinger = true;
	/*
	 * Don't bother handling a return to "waiting" state here: if we
	 * fail, it's fine, just let go of this handle entirely.
	 */
	handle.try(fsm);
}

ConnectionSlotFSM.prototype.state_busy = function (S) {
	S.validTransitions(['idle', 'stopping', 'stopped', 'retrying',
	    'killing', 'connecting']);
	var self = this;
	var smgr = this.csf_smgr;
	var hdl = this.csf_handle;
	var smgrState = 'connected';

	S.on(smgr, 'stateChanged', function (st) {
		smgrState = st;
	});

	function onRelease() {
		if (smgrState === 'connected') {
			if (self.csf_wanted) {
				S.gotoState('idle');
			} else {
				S.gotoState('stopping');
			}
		} else if (smgrState === 'closed') {
			if (self.csf_wanted) {
				S.gotoState('connecting');
			} else {
				S.gotoState('stopped');
			}
		} else if (smgrState === 'error') {
			S.gotoState('retrying');
		} else {
			throw (new Error('Handle released while smgr was ' +
			    'in unhandled state "' + smgr.getState() + '"'));
		}
	}

	function onClose() {
		if (smgrState === 'connected') {
			S.gotoState('killing');
		} else {
			S.gotoState('retrying');
		}
	}

	S.on(hdl, 'stateChanged', function (st) {
		switch (st) {
		case 'released':
			onRelease();
			break;
		case 'closed':
			onClose();
			break;
		default:
			break;
		}
	});

	/*
	 * It's possible that the smgr has already moved out of 'connected'
	 * by the time we get here.
	 *
	 * If we lose the race, treat it like our handle was released.
	 */
	if (smgr.isInState('connected')) {
		var sock = smgr.getSocket();
		hdl.accept(sock);
	} else {
		hdl.reject();
		this.csf_handle = undefined;
		onRelease();
	}
};

ConnectionSlotFSM.prototype.state_killing = function (S) {
	S.validTransitions(['retrying']);
	var smgr = this.csf_smgr;

	S.on(smgr, 'stateChanged', function (st) {
		if (st === 'closed' || st === 'error') {
			S.gotoState('retrying');
		}
	});

	/*
	 * It's possible to get to this state when something else has already
	 * caused the socket to close, but we haven't received the
	 * 'stateChanged' event yet. If so, don't call .close(), just wait for
	 * the event.
	 */
	if (!smgr.isInState('closed') && !smgr.isInState('error'))
		smgr.close();
};

ConnectionSlotFSM.prototype.state_stopping = function (S) {
	S.validTransitions(['stopped']);
	var smgr = this.csf_smgr;

	S.on(smgr, 'stateChanged', function (st) {
		if (st === 'closed' || st === 'error') {
			S.gotoState('stopped');
		}
	});

	/*
	 * See above (deal with possible pending stateChanged event).
	 */
	if (!smgr.isInState('closed') && !smgr.isInState('error'))
		smgr.close();
};

ConnectionSlotFSM.prototype.state_stopped = function (S) {
	S.validTransitions([]);
	var smgr = this.csf_smgr;
	mod_assert.ok(smgr.isInState('closed') || smgr.isInState('error') ||
	    smgr.isInState('failed'), 'smgr must be stopped');

};
