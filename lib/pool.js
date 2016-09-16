/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	ConnectionPool: CueBallConnectionPool
};

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
	if (this.cf_pool.p_dead[this.cf_backend.key] === true) {
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
	if (stack === undefined) {
		var e = {};
		Error.captureStackTrace(e);
		stack = e.stack;
	}
	this.cf_claimStack = stack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /, '')); });
	this.cf_claimed = true;
	if (cb) {
		var self = this;
		this.onState('busy', function () {
			/*
			 * Give the client our ConnectionHandle, and the
			 * backing object.
			 *
			 * They use the ConnectionHandle to call release().
			 */
			cb(null, self.cf_shadow, self.cf_conn);
		});
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

	if (cb)
		this.onState('idle', cb);
	this.emit('releaseAsserted');
};

ConnectionFSM.prototype.close = function (cb) {
	if (cb)
		this.onState('closed', cb);
	this.emit('closeAsserted');
};

ConnectionFSM.prototype.start = function () {
	this.emit('startAsserted');
};

ConnectionFSM.prototype.closeAfterRelease = function () {
	this.cf_closeAfter = true;
};

ConnectionFSM.prototype.state_init = function (on) {
	this.validTransitions(['connect', 'closed']);
	var self = this;
	on(this, 'startAsserted', function () {
		self.gotoState('connect');
	});
	on(this, 'closeAsserted', function () {
		self.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_connect = function (on, once, timeout) {
	this.validTransitions(['error', 'idle', 'closed']);
	var self = this;
	timeout(this.cf_timeout, function () {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		self.gotoState('error');
	});
	this.cf_conn = this.cf_constructor(this.cf_backend);
	mod_assert.object(this.cf_conn, 'constructor return value');
	this.cf_conn.cf_fsm = this;
	once(this.cf_conn, 'connect', function () {
		self.gotoState('idle');
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'connectError', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this.cf_conn, 'timeout', function () {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		self.gotoState('error');
	});
	once(this.cf_conn, 'connectTimeout', function (err) {
		self.cf_lastError = new mod_errors.ConnectionTimeoutError(self);
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_closed = function (on) {
	this.validTransitions([]);
	if (this.cf_conn && this.cf_conn.destroy)
		this.cf_conn.destroy();
	this.cf_conn = undefined;
	this.cf_closeAfter = false;
	this.cf_lastError = undefined;
	this.cf_log.trace('ConnectionFSM closed');
	on(this, 'closeAsserted', function () { });
};

ConnectionFSM.prototype.state_error = function (on, once, timeout) {
	this.validTransitions(['delay', 'closed']);

	var self = this;
	on(this, 'closeAsserted', function () {
		self.gotoState('closed');
	});

	if (this.cf_conn && this.cf_conn.destroy)
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
		this.gotoState('closed');
		return;
	}

	if (this.cf_retries !== Infinity)
		--this.cf_retriesLeft;

	if (this.cf_retries === Infinity || this.cf_retriesLeft > 0) {
		this.gotoState('delay');
	} else {
		this.cf_log.warn(this.cf_lastError, 'failed to connect to ' +
		    'backend %s (%j)', this.cf_backend.key, this.cf_backend);
		this.gotoState('closed');
	}
};

ConnectionFSM.prototype.state_delay = function (on, once, timeout) {
	this.validTransitions(['connect', 'closed']);
	var delay = this.cf_delay;

	this.cf_delay *= 2;
	this.cf_timeout *= 2;
	if (this.cf_timeout > this.cf_maxTimeout)
		this.cf_timeout = this.cf_maxTimeout;
	if (this.cf_delay > this.cf_maxDelay)
		this.cf_delay = this.cf_maxDelay;

	var self = this;
	var t = timeout(delay, function () {
		self.gotoState('connect');
	});
	t.unref();
	once(this, 'closeAsserted', function () {
		self.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_idle = function (on, once, timeout) {
	this.validTransitions(['busy', 'error', 'closed']);
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
		var newCount = self.cf_conn.listeners(evt).filter(
		    function (h) { return (typeof (h) === 'function'); }).
		    length;
		var oldCount = self.cf_oldListeners[evt];
		if (oldCount !== undefined && newCount > oldCount) {
			var info = {};
			info.stack = self.cf_releaseStack;
			info.handlers = self.cf_conn.listeners(evt).map(
			    function (f) { return (f.toString()); });
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
		this.gotoState('closed');
		on(this, 'closeAsserted', function () { });
		return;
	}

	this.cf_conn.unref();

	once(this, 'claimAsserted', function () {
		self.gotoState('busy');
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this.cf_conn, 'end', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.gotoState('closed');
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
		var t = timeout(delay, function () {
			self.gotoState('ping');
		});
		t.unref();
	}
};

ConnectionFSM.prototype.state_ping = function (on, once, timeout) {
	this.validTransitions(['error', 'closed', 'idle']);
	this.cf_lastCheck = new Date();

	this.cf_claimStack = [
	    'ConnectionFSM.prototype.state_ping',
	    '(periodic_health_check)'];
	this.cf_claimed = true;

	var self = this;
	this.cf_conn.ref();

	this.cf_releaseStack = [];
	this.cf_log.trace('doing health check');

	/*
	 * Write down the count of event handlers on the backing object so that
	 * we can spot if the client leaked any common ones in release().
	 */
	this.cf_oldListeners = {};
	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var count = self.cf_conn.listeners(evt).filter(
		    function (h) { return (typeof (h) === 'function'); }).
		    length;
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

	once(this, 'releaseAsserted', function () {
		if (this.cf_closeAfter === true) {
			this.gotoState('closed');
		} else {
			self.gotoState('idle');
		}
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this.cf_conn, 'end', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	var t = timeout(this.cf_checkTimeout, function () {
		var info = {};
		info.stack = self.cf_claimStack;
		self.cf_log.warn(info, 'health check is taking too ' +
		    'long to run (has been more than %d ms)',
		    self.cf_checkTimeout);
	});
	t.unref();

	this.cf_checker.call(undefined, this.cf_shadow, this.cf_conn);
};

ConnectionFSM.prototype.state_busy = function (on, once, timeout) {
	this.validTransitions(['error', 'closed', 'idle']);
	var self = this;
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
		var count = self.cf_conn.listeners(evt).filter(
		    function (h) { return (typeof (h) === 'function'); }).
		    length;
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

	once(this, 'releaseAsserted', function () {
		if (this.cf_closeAfter === true) {
			this.gotoState('closed');
		} else {
			self.gotoState('idle');
		}
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'end', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.cf_lastError = new mod_errors.ConnectionClosedError(self);
		self.gotoState('error');
	});
	if (this.cf_checkTimeout !== undefined) {
		var t = timeout(this.cf_checkTimeout, function () {
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

/*
 * A ConnectionPool holds a pool of ConnectionFSMs that are kept up to date
 * based on the output of a Resolver. At any given time the pool may contain:
 *
 *  - "busy" connections in use by a client
 *  - "init" connections being established for the first time
 *  - "idle" connections waiting to be re-used
 *
 * The 'maximum' option gives a maximum limit on the size of all connections
 * combined (from any of these 3 classes). The 'spares' option limits the
 * number of "idle" and "init" connections that can be in the pool at any time
 * (any excess will be closed and discarded).
 *
 * The 'spares' option also sets a target for the number of "idle" connections
 * that should be maintained. The pool will attempt to add more connections
 * in order to keep this number of idle connections around at all times.
 *
 * Clients obtain a connection from the pool for their use by calling the
 * claim() function. If an idle connection is available and ready, the callback
 * given to claim() will be called immediately. Otherwise, it will wait until
 * a connection is available. If the pool is below its 'maximum' size, a request
 * to expand the pool will be queued when a claim() request comes in.
 */
function CueBallConnectionPool(options) {
	mod_assert.object(options);

	mod_assert.func(options.constructor, 'options.constructor');

	this.p_uuid = mod_uuid.v4();
	this.p_constructor = options.constructor;

	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.optionalObject(options.resolver, 'options.resolver');
	mod_assert.string(options.domain, 'options.domain');
	this.p_domain = options.domain;
	mod_assert.optionalString(options.service, 'options.service');
	mod_assert.optionalNumber(options.maxDNSConcurrency,
	    'options.maxDNSConcurrency');
	mod_assert.optionalNumber(options.defaultPort, 'options.defaultPort');

	mod_assert.object(options.recovery, 'options.recovery');
	this.p_recovery = options.recovery;

	mod_assert.optionalObject(options.log, 'options.log');
	this.p_log = options.log || mod_bunyan.createLogger({
		name: 'CueBallConnectionPool'
	});
	this.p_log = this.p_log.child({
		domain: options.domain,
		service: options.service,
		pool: this.p_uuid
	});

	mod_assert.number(options.spares, 'options.spares');
	mod_assert.number(options.maximum, 'options.maximum');
	this.p_spares = options.spares;
	this.p_max = options.maximum;

	mod_assert.optionalNumber(options.checkTimeout, 'options.checkTimeout');
	mod_assert.optionalFunc(options.checker, 'options.checker');
	this.p_checker = options.checker;
	this.p_checkTimeout = options.checkTimeout;

	this.p_keys = [];
	this.p_backends = {};
	this.p_connections = {};
	this.p_dead = {};

	this.p_lastRebalance = undefined;
	this.p_inRebalance = false;
	this.p_startedResolver = false;

	this.p_idleq = new Queue();
	this.p_initq = new Queue();
	this.p_waiters = new Queue();

	var self = this;
	if (options.resolver !== undefined && options.resolver !== null) {
		this.p_resolver = options.resolver;
		this.p_resolver_custom = true;
	} else {
		this.p_resolver = new mod_resolver.Resolver({
			resolvers: options.resolvers,
			domain: options.domain,
			service: options.service,
			maxDNSConcurrency: options.maxDNSConcurrency,
			defaultPort: options.defaultPort,
			log: this.p_log,
			recovery: options.recovery
		});
		this.p_resolver_custom = false;
	}

	/*
	 * Periodically rebalance() so that we catch any connections that
	 * come off "busy" (we're lazy about these and don't rebalance every
	 * single time they return to the pool).
	 */
	this.p_rebalTimer = new EventEmitter();
	this.p_rebalTimerInst = setInterval(function () {
		self.p_rebalTimer.emit('timeout');
	}, 10000);
	this.p_rebalTimerInst.unref();

	this.p_shuffleTimer = new EventEmitter();
	this.p_shuffleTimerInst = setInterval(function () {
		self.p_shuffleTimer.emit('timeout');
	}, 60000);
	this.p_shuffleTimerInst.unref();

	FSM.call(this, 'starting');
}
mod_util.inherits(CueBallConnectionPool, FSM);

CueBallConnectionPool.prototype.on_resolver_added = function (k, backend) {
	backend.key = k;
	var idx = Math.floor(Math.random() * (this.p_keys.length + 1));
	this.p_keys.splice(idx, 0, k);
	this.p_backends[k] = backend;
	this.rebalance();
};

CueBallConnectionPool.prototype.on_resolver_removed = function (k) {
	var idx = this.p_keys.indexOf(k);
	if (idx !== -1)
		this.p_keys.splice(idx, 1);
	delete (this.p_backends[k]);
	(this.p_connections[k] || []).forEach(function (fsm) {
		if (!fsm.isInState('busy'))
			fsm.close();
	});
	delete (this.p_connections[k]);
	this.rebalance();
};

CueBallConnectionPool.prototype.state_starting =
    function (on, once, timeout, onState) {
	this.validTransitions(['failed', 'running', 'stopping']);
	mod_monitor.monitor.registerPool(this);

	on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));

	var self = this;

	if (this.p_resolver.isInState('failed')) {
		this.p_log.warn('pre-provided resolver has already failed, ' +
		    'pool will start up in "failed" state');
		this.gotoState('failed');
		return;
	}

	onState(this.p_resolver, 'failed', function () {
		self.p_log.warn('underlying resolver failed, moving pool ' +
		    'to "failed" state');
		self.gotoState('failed');
	});

	if (this.p_resolver.isInState('running')) {
		var backends = this.p_resolver.list();
		Object.keys(backends).forEach(function (k) {
			var backend = backends[k];
			self.on_resolver_added(k, backend);
		});
	} else if (this.p_resolver.isInState('stopped') &&
	    !this.p_resolver_custom) {
		this.p_resolver.start();
		this.p_startedResolver = true;
	}

	on(this, 'connectedToBackend', function () {
		self.gotoState('running');
	});

	on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.p_dead).length;
		if (dead >= self.p_keys.length) {
			self.p_log.error(
			    { dead: dead },
			    'pool has exhausted all retries, now moving to ' +
			    '"failed" state');
			self.gotoState('failed');
		}
	});

	on(this, 'stopAsserted', function () {
		self.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_failed = function (on) {
	this.validTransitions(['running', 'stopping']);
	on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));
	on(this.p_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	var self = this;
	on(this, 'connectedToBackend', function () {
		mod_assert.ok(!self.p_resolver.isInState('failed'));
		self.p_log.info('successfully connected to a backend, ' +
		    'moving back to running state');
		self.gotoState('running');
	});

	on(this, 'stopAsserted', function () {
		self.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_running = function (on) {
	this.validTransitions(['failed', 'stopping']);
	var self = this;
	on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));
	on(this.p_rebalTimer, 'timeout', this.rebalance.bind(this));
	on(this.p_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.p_dead).length;
		if (dead >= self.p_keys.length) {
			self.p_log.error(
			    { dead: dead },
			    'pool has exhausted all retries, now moving to ' +
			    '"failed" state');
			self.gotoState('failed');
		}
	});

	on(this, 'stopAsserted', function () {
		self.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_stopping =
    function (on, once, timeout, onState) {
	this.validTransitions(['stopping.backends']);
	var self = this;
	if (this.p_startedResolver) {
		onState(this.p_resolver, 'stopped', function () {
			self.gotoState('stopping.backends');
		});
		this.p_resolver.stop();
	} else {
		this.gotoState('stopping.backends');
	}
};

CueBallConnectionPool.prototype.state_stopping.backends = function () {
	this.validTransitions(['stopped']);
	var conns = this.p_connections;
	var self = this;
	var fsms = [];
	Object.keys(conns).forEach(function (k) {
		conns[k].forEach(function (fsm) {
			fsms.push(fsm);
		});
	});
	mod_vasync.forEachParallel({
		func: closeBackend,
		inputs: fsms
	}, function () {
		self.gotoState('stopped');
	});
	function closeBackend(fsm, cb) {
		if (fsm.isInState('busy')) {
			fsm.closeAfterRelease();
			fsm.onState('closed', cb);
		} else {
			fsm.close(cb);
		}
	}
};

CueBallConnectionPool.prototype.state_stopped = function () {
	this.validTransitions([]);
	mod_monitor.monitor.unregisterPool(this);
	this.p_keys = [];
	this.p_connections = {};
	this.p_backends = {};
	clearInterval(this.p_rebalTimerInst);
	clearInterval(this.p_shuffleTimerInst);
};

CueBallConnectionPool.prototype.reshuffle = function () {
	var taken = this.p_keys.pop();
	var idx = Math.floor(Math.random() * (this.p_keys.length + 1));
	this.p_keys.splice(idx, 0, taken);
	this.rebalance();
};

/* Stop and kill everything. */
CueBallConnectionPool.prototype.stop = function () {
	this.emit('stopAsserted');
};

/*
 * Rebalance the pool, by looking at the distribution of connections to
 * backends amongst the "init" and "idle" queues.
 *
 * If the connections are not evenly distributed over the available backends,
 * then planRebalance() will return a plan to take us back to an even
 * distribution, which we then apply.
 */
CueBallConnectionPool.prototype.rebalance = function () {
	var self = this;

	if (this.p_keys.length < 1)
		return;

	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	if (this.p_inRebalance !== false)
		return;
	this.p_inRebalance = true;

	var total = 0;
	var conns = {};
	this.p_keys.forEach(function (k) {
		conns[k] = (self.p_connections[k] || []).slice();
		total += conns[k].length;
	});
	Object.keys(this.p_connections).forEach(function (k) {
		if (conns[k] === undefined &&
		    self.p_connections[k] !== undefined &&
		    self.p_connections[k].length > 0) {
			conns[k] = (self.p_connections[k] || []).slice();
		}
	});
	var spares = this.p_idleq.length + this.p_initq.length -
	    this.p_waiters.length;
	if (spares < 0)
		spares = 0;
	var busy = total - spares;
	if (busy < 0)
		busy = 0;
	var extras = this.p_waiters.length - this.p_initq.length;
	if (extras < 0)
		extras = 0;

	var target = busy + extras + this.p_spares;
	if (target > this.p_max)
		target = this.p_max;

	var plan = mod_utils.planRebalance(conns, self.p_dead, target,
	    self.p_max);

	if (plan.remove.length > 0 || plan.add.length > 0) {
		this.p_log.trace('rebalancing pool, remove %d, ' +
		    'add %d (busy = %d, spares = %d, target = %d)',
		    plan.remove.length, plan.add.length,
		    busy, spares, target);
	}
	plan.remove.forEach(function (fsm) {
		/*
		 * Only tell the FSM to quit *right now* if either:
		 *   1. it's idle
		 *   2. there are other FSMs for this backend
		 *   2. it is connected to a backend that has been
		 *      removed from the resolver
		 * Otherwise get it to quit gracefully once it's done
		 * doing whatever it's doing (using closeAfterRelease).
		 * This way we when we have a failing backend that we
		 * want to mark as "dead" ASAP, we don't give up early
		 * and never figure out if it's actually dead or not.
		 */
		var fsmIdx = self.p_connections[fsm.cf_backend.key].
		    indexOf(fsm);
		if (fsm.isInState('idle') || fsmIdx > 0 ||
		    self.p_keys.indexOf(fsm.cf_backend.key) === -1) {
			fsm.close();
			--total;
		} else {
			fsm.closeAfterRelease();
		}
	});
	plan.add.forEach(function (k) {
		/* Make sure we *never* exceed our socket limit. */
		if (++total > self.p_max)
			return;
		self.addConnection(k);
	});

	this.p_inRebalance = false;
	this.p_lastRebalance = new Date();
};

CueBallConnectionPool.prototype.addConnection = function (key) {
	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	var backend = this.p_backends[key];
	backend.key = key;

	var fsm = new ConnectionFSM({
		constructor: this.p_constructor,
		backend: backend,
		log: this.p_log,
		pool: this,
		checker: this.p_checker,
		checkTimeout: this.p_checkTimeout,
		recovery: this.p_recovery
	});
	if (this.p_connections[key] === undefined)
		this.p_connections[key] = [];
	this.p_connections[key].push(fsm);

	fsm.p_initq_node = this.p_initq.push(fsm);

	var self = this;
	fsm.on('stateChanged', function (newState) {
		var doRebalance = false;

		if (fsm.p_initq_node) {
			/* These transitions mean we're still starting up. */
			if (newState === 'delay' || newState === 'error' ||
			    newState === 'connect')
				return;
			/*
			 * As soon as we transition out of the init stages
			 * we should drop ourselves from the init queue.
			 */
			fsm.p_initq_node.remove();
			delete (fsm.p_initq_node);

			if (newState === 'idle') {
				self.emit('connectedToBackend', key, fsm);

				if (self.p_dead[key] !== undefined) {
					delete (self.p_dead[key]);
					doRebalance = true;
				}
			}
		}

		if (newState === 'idle') {
			/*
			 * This backend has just become available, either
			 * because its previous user released it, or because
			 * it has finished connecting (and was previously in
			 * "init").
			 */

			/* Check to see if this backend has gone away. */
			if (self.p_backends[key] === undefined) {
				fsm.close();
				return;
			}

			/*
			 * Try to eat up any waiters that are waiting on a
			 * new connection.
			 */
			if (self.p_waiters.length > 0) {
				var cb = self.p_waiters.shift();
				fsm.claim(cb.stack, cb);
				return;
			}

			/* Otherwise, onto the idle queue we go! */
			var node = self.p_idleq.push(fsm);
			fsm.p_idleq_node = node;

			return;
		}

		/*
		 * Connections that are doing a health check get put on the
		 * initq so they don't get counted as "busy".
		 */
		if (newState === 'ping' && !fsm.p_initq_node)
			fsm.p_initq_node = self.p_initq.push(fsm);

		if (newState === 'closed') {
			if (self.p_connections[key]) {
				var idx = self.p_connections[key].indexOf(fsm);
				self.p_connections[key].splice(idx, 1);
			}
			if (fsm.retriesExhausted()) {
				self.p_dead[key] = true;
			}
			self.emit('closedBackend', key, fsm);
			doRebalance = true;
		}

		if (fsm.p_idleq_node) {
			/*
			 * This connection was idle, now it isn't. Remove it
			 * from the idle queue.
			 */
			fsm.p_idleq_node.remove();
			delete (fsm.p_idleq_node);

			/* Also rebalance, in case we were closed or died. */
			doRebalance = true;
		}

		if (doRebalance)
			self.rebalance();
	});

	fsm.start();
};

CueBallConnectionPool.prototype.claimSync = function () {
	if (this.isInState('stopping') || this.isInState('stopped'))
		throw (new mod_errors.PoolStoppingError(this));
	if (this.isInState('failed'))
		throw (new mod_errors.PoolFailedError(this));

	var e = {};
	Error.captureStackTrace(e);

	/* If there are idle connections sitting around, take one. */
	if (this.p_idleq.length > 0) {
		var fsm = this.p_idleq.shift();
		delete (fsm.p_idleq_node);
		fsm.claim(e.stack);
		mod_assert.ok(fsm.cf_shadow);
		return ({
			handle: fsm.cf_shadow,
			connection: fsm.cf_conn
		});
	}

	throw (new mod_errors.NoBackendsError(this));
};

CueBallConnectionPool.prototype.claim = function (options, cb) {
	var self = this;
	var done = false;

	if (typeof (options) === 'function' && cb === undefined) {
		cb = options;
		options = {};
	}
	mod_assert.object(options, 'options');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	var timeout = options.timeout || Infinity;
	mod_assert.optionalBool(options.errorOnEmpty, 'options.errorOnEmpty');
	var errOnEmpty = options.errorOnEmpty;

	if (this.isInState('stoppping') || this.isInState('stopped')) {
		setImmediate(function () {
			if (!done)
				cb(new mod_errors.PoolStoppingError(self));
			done = true;
		});
		return ({
			cancel: function () { done = true; }
		});
	}
	if (this.isInState('failed')) {
		setImmediate(function () {
			if (!done)
				cb(new mod_errors.PoolFailedError(self));
			done = true;
		});
		return ({
			cancel: function () { done = true; }
		});
	}

	var e = {};
	Error.captureStackTrace(e);

	/* If there are idle connections sitting around, take one. */
	if (this.p_idleq.length > 0) {
		var fsm = this.p_idleq.shift();
		delete (fsm.p_idleq_node);
		fsm.claim(e.stack, cb);
		return (undefined);
	}

	if (errOnEmpty && this.p_resolver.count() < 1) {
		setImmediate(function () {
			if (!done)
				cb(new mod_errors.NoBackendsError(self));
			done = true;
		});
		return ({
			cancel: function () { done = true; }
		});
	}

	/* Otherwise add an entry on the "waiter" queue. */
	var timer;
	var waiter = function () {
		if (timer !== undefined)
			clearTimeout(timer);
		timer = undefined;
		done = true;
		cb.apply(this, arguments);
	};
	waiter.stack = e.stack;
	var qnode = this.p_waiters.push(waiter);
	if (timeout !== Infinity) {
		timer = setTimeout(function () {
			if (timer === undefined)
				return;

			qnode.remove();
			done = true;
			cb(new mod_errors.ClaimTimeoutError(self));
		}, timeout);
	}
	this.rebalance();

	var handle = {};
	handle.cancel = function () {
		mod_assert.ok(done === false, 'callback was already called ' +
		    'for this waiter handle');
		if (timer !== undefined)
			clearTimeout(timer);
		timer = undefined;
		qnode.remove();
		done = true;
	};
	return (handle);
};
