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
const ConnectionFSM = require('./connection-fsm');

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
	this.p_rebalScheduled = false;
	this.p_startedResolver = false;

	this.p_idleq = new Queue();
	this.p_initq = new Queue();
	this.p_waiters = new Queue();

	this.p_counters = {};

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

CueBallConnectionPool.prototype._incrCounter = function (counter) {
	if (this.p_counters[counter] === undefined)
		this.p_counters[counter] = 0;
	++this.p_counters[counter];
};

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

CueBallConnectionPool.prototype.state_starting = function (S) {
	S.validTransitions(['failed', 'running', 'stopping']);
	mod_monitor.monitor.registerPool(this);

	S.on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));

	var self = this;

	if (this.p_resolver.isInState('failed')) {
		this.p_log.warn('pre-provided resolver has already failed, ' +
		    'pool will start up in "failed" state');
		S.gotoState('failed');
		return;
	}

	S.on(this.p_resolver, 'stateChanged', function (state) {
		if (state === 'failed') {
			self.p_log.warn('underlying resolver failed, moving ' +
			    'pool to "failed" state');
			S.gotoState('failed');
		}
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

	S.on(this, 'connectedToBackend', function () {
		S.gotoState('running');
	});

	S.on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.p_dead).length;
		if (dead >= self.p_keys.length) {
			self.p_log.error(
			    { dead: dead },
			    'pool has exhausted all retries, now moving to ' +
			    '"failed" state');
			S.gotoState('failed');
		}
	});

	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_failed = function (S) {
	S.validTransitions(['running', 'stopping']);
	S.on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));
	S.on(this.p_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	var self = this;
	S.on(this, 'connectedToBackend', function () {
		mod_assert.ok(!self.p_resolver.isInState('failed'));
		self.p_log.info('successfully connected to a backend, ' +
		    'moving back to running state');
		S.gotoState('running');
	});

	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_running = function (S) {
	S.validTransitions(['failed', 'stopping']);
	var self = this;
	S.on(this.p_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.p_resolver, 'removed', this.on_resolver_removed.bind(this));
	S.on(this.p_rebalTimer, 'timeout', this.rebalance.bind(this));
	S.on(this.p_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	S.on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.p_dead).length;
		if (dead >= self.p_keys.length) {
			self.p_log.error(
			    { dead: dead },
			    'pool has exhausted all retries, now moving to ' +
			    '"failed" state');
			S.gotoState('failed');
		}
	});

	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
};

CueBallConnectionPool.prototype.state_stopping = function (S) {
	S.validTransitions(['stopping.backends']);
	if (this.p_startedResolver) {
		S.on(this.p_resolver, 'stateChanged', function (s) {
			if (s === 'stopped') {
				S.gotoState('stopping.backends');
			}
		});
		this.p_resolver.stop();
		if (this.p_resolver.isInState('stopped')) {
			S.gotoState('stopping.backends');
		}
	} else {
		S.gotoState('stopping.backends');
	}
};

CueBallConnectionPool.prototype.state_stopping.backends = function (S) {
	S.validTransitions(['stopped']);
	var conns = this.p_connections;
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
		S.gotoState('stopped');
	});
	function closeBackend(fsm, cb) {
		if (fsm.isInState('busy')) {
			fsm.closeAfterRelease();
			fsm.on('stateChanged', function (st) {
				if (st === 'closed')
					cb();
			});
		} else {
			fsm.close(cb);
		}
	}
};

CueBallConnectionPool.prototype.state_stopped = function (S) {
	S.validTransitions([]);
	mod_monitor.monitor.unregisterPool(this);
	this.p_keys = [];
	this.p_connections = {};
	this.p_backends = {};
	clearInterval(this.p_rebalTimerInst);
	clearInterval(this.p_shuffleTimerInst);
};

CueBallConnectionPool.prototype.isDeclaredDead = function (backend) {
	return (this.p_dead[backend] === true);
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

CueBallConnectionPool.prototype.rebalance = function () {
	if (this.p_keys.length < 1)
		return;

	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	if (this.p_rebalScheduled !== false)
		return;

	this.p_rebalScheduled = true;

	var self = this;
	setImmediate(function () {
		self._rebalance();
	});
};

/*
 * Rebalance the pool, by looking at the distribution of connections to
 * backends amongst the "init" and "idle" queues.
 *
 * If the connections are not evenly distributed over the available backends,
 * then planRebalance() will return a plan to take us back to an even
 * distribution, which we then apply.
 */
CueBallConnectionPool.prototype._rebalance = function () {
	var self = this;

	if (this.p_inRebalance !== false)
		return;
	this.p_inRebalance = true;
	this.p_rebalScheduled = false;

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
		if (fsm.p_initq_node) {
			/* These transitions mean we're still starting up. */
			if (newState === 'init' || newState === 'delay' ||
			    newState === 'error' || newState === 'connect')
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
					self.rebalance();
				}
			}
		}

		if (newState === 'idle' && fsm.isInState('idle')) {
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
			self.rebalance();
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

CueBallConnectionPool.prototype.claim = function (options, cb) {
	var self = this;
	var done = false;

	if (typeof (options) === 'function' && cb === undefined) {
		cb = options;
		options = {};
	}
	mod_assert.object(options, 'options');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	var timeout = options.timeout;
	if (timeout === undefined)
		timeout = Infinity;
	mod_assert.optionalBool(options.errorOnEmpty, 'options.errorOnEmpty');
	var errOnEmpty = options.errorOnEmpty;

	if (this.isInState('stopping') || this.isInState('stopped')) {
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
		fsm.claim(e.stack, function (err, hdl, conn) {
			if (err) {
				if (!done)
					cb(err);
				done = true;
				return;
			}
			if (done) {
				hdl.release();
				return;
			}
			done = true;
			cb(err, hdl, conn);
		});
		return ({
			cancel: function () { done = true; }
		});
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
