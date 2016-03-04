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

	FSM.call(this, 'init');
}
mod_util.inherits(ConnectionFSM, FSM);

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

ConnectionFSM.prototype.state_init = function (on, once) {
	var self = this;
	once(this, 'startAsserted', function () {
		self.gotoState('connect');
	});
};

ConnectionFSM.prototype.state_connect = function (on, once, timeout) {
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

ConnectionFSM.prototype.state_closed = function () {
	if (this.cf_conn && this.cf_conn.destroy)
		this.cf_conn.destroy();
	this.cf_conn = undefined;
	this.cf_closeAfter = false;
	this.cf_lastError = undefined;
	this.cf_log.trace('closed');
};

ConnectionFSM.prototype.state_error = function (on, once, timeout) {
	if (this.cf_conn && this.cf_conn.destroy)
		this.cf_conn.destroy();
	this.cf_conn = undefined;

	if (this.cf_shadow) {
		this.cf_shadow.sh_error = true;
		this.cf_shadow = undefined;
	}

	if (this.cf_retries === Infinity || --this.cf_retriesLeft > 0) {
		this.gotoState('delay');
	} else {
		this.cf_log.warn(this.cf_lastError, 'failed to connect to ' +
		    'backend %s (%j)', this.cf_backend.key, this.cf_backend);
		this.gotoState('closed');
	}
};

ConnectionFSM.prototype.state_delay = function (on, once, timeout) {
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

	this.cf_retriesLeft = this.cf_retries;

	if (this.cf_closeAfter === true) {
		this.cf_closeAfter = false;
		this.cf_lastError = undefined;
		this.gotoState('closed');
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
	EventEmitter.call(this);
	mod_assert.object(options);

	mod_assert.func(options.constructor, 'options.constructor');

	this.p_uuid = mod_uuid.v4();
	this.p_constructor = options.constructor;

	mod_assert.optionalArrayOfString(options.resolvers,
	    'options.resolvers');
	mod_assert.string(options.domain, 'options.domain');
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

	this.p_lastRebalance = undefined;
	this.p_inRebalance = false;

	this.p_idleq = new Queue();
	this.p_initq = new Queue();
	this.p_waiters = new Queue();

	var self = this;
	this.p_resolver = new mod_resolver.Resolver({
		resolvers: options.resolvers,
		domain: options.domain,
		service: options.service,
		maxDNSConcurrency: options.maxDNSConcurrency,
		defaultPort: options.defaultPort,
		log: this.p_log,
		recovery: options.recovery
	});
	this.p_resolver.on('added', function (k, backend) {
		backend.key = k;
		self.p_keys.push(k);
		mod_utils.shuffle(self.p_keys);
		self.p_backends[k] = backend;
		self.rebalance();
	});
	this.p_resolver.on('removed', function (k) {
		var idx = self.p_keys.indexOf(k);
		self.p_keys.splice(idx, 1);
		delete (self.p_backends[k]);
		self.p_connections[k].forEach(function (fsm) {
			if (fsm.getState() !== 'busy')
				fsm.close();
		});
		delete (self.p_connections[k]);
		self.rebalance();
	});
	/*
	 * Periodically rebalance() so that we catch any connections that
	 * come off "busy" (we're lazy about these and don't rebalance every
	 * single time they return to the pool).
	 */
	this.p_rebalTimer = setInterval(function () {
		self.rebalance();
	}, 10000);
	this.p_rebalTimer.unref();

	this.p_resolver.start();

	mod_monitor.monitor.registerPool(this);
}
mod_util.inherits(CueBallConnectionPool, EventEmitter);

/* Stop and kill everything. */
CueBallConnectionPool.prototype.stop = function () {
	mod_monitor.monitor.unregisterPool(this);

	clearInterval(this.p_rebalTimer);
	this.p_resolver.stop();
	var conns = this.p_connections;
	Object.keys(conns).forEach(function (k) {
		conns[k].forEach(function (fsm) {
			mod_assert.ok(fsm.getState() !== 'busy');
			fsm.close();
		});
	});
	this.p_keys = [];
	this.p_connections = {};
	this.p_backends = {};
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

	if (this.p_inRebalance !== false)
		return;
	this.p_inRebalance = true;

	var total = 0;
	var conns = {};
	this.p_keys.forEach(function (k) {
		conns[k] = (self.p_connections[k] || []).slice();
		total += conns[k].length;
	});
	var spares = this.p_idleq.length + this.p_initq.length -
	    this.p_waiters.length;
	if (spares < 0)
		spares = 0;
	var busy = total - spares;
	var extras = this.p_waiters.length - this.p_initq.length;
	if (extras < 0)
		extras = 0;

	var target = busy + extras + this.p_spares;
	if (target > this.p_max)
		target = this.p_max;

	var plan = mod_utils.planRebalance(conns, total, target);
	if (plan.remove.length > 0 || plan.add.length > 0) {
		this.p_log.trace('rebalancing pool, remove %d, ' +
		    'add %d (busy = %d, idle = %d, target = %d)',
		    plan.remove.length, plan.add.length,
		    busy, spares, target);
	}
	plan.remove.forEach(function (fsm) {
		if (fsm.getState() === 'busy') {
			fsm.closeAfterRelease();
		} else {
			fsm.close();
			--total;
		}
	});
	plan.add.forEach(function (k) {
		if (total > self.p_max)
			return;
		self.addConnection(k);
		++total;
	});

	this.p_inRebalance = false;
	this.p_lastRebalance = new Date();
};

CueBallConnectionPool.prototype.addConnection = function (key) {
	var backend = this.p_backends[key];

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
			var idx = self.p_connections[key].indexOf(fsm);
			self.p_connections[key].splice(idx, 1);
		}

		if (fsm.p_idleq_node) {
			/*
			 * This connection was idle, now it isn't. Remove it
			 * from the idle queue.
			 */
			fsm.p_idleq_node.remove();
			delete (fsm.p_idleq_node);

			/* Also rebalance, in case we were closed or died. */
			self.rebalance();
		}
	});

	fsm.start();
};

CueBallConnectionPool.prototype.claimSync = function () {
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
	if (typeof (options) === 'function' && cb === undefined) {
		cb = options;
		options = {};
	}
	mod_assert.object(options, 'options');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	var timeout = options.timeout || Infinity;
	mod_assert.optionalBool(options.errorOnEmpty, 'options.errorOnEmpty');
	var errOnEmpty = options.errorOnEmpty;

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
		cb(new mod_errors.NoBackendsError(self));
		return (undefined);
	}

	/* Otherwise add an entry on the "waiter" queue. */
	var timer;
	var done = false;
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
