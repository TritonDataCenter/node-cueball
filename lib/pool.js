/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2018 Joyent, Inc.
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
const mod_uuid = require('uuid');
const mod_errors = require('./errors');
const mod_verror = require('verror');

const mod_codel = require('./codel');
const mod_monitor = require('./pool-monitor');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

const Queue = require('./queue');
const mod_connfsm = require('./connection-fsm');
const ConnectionSlotFSM = mod_connfsm.ConnectionSlotFSM;
const CueBallClaimHandle = mod_connfsm.CueBallClaimHandle;

/*
 * Parameters for the EMA/low-pass filter that is used to limit pool shrinkage
 * under sustained load. The idea is to stop the pool from shrinking too
 * rapidly if the average load has been high recently. This stops us from
 * over-reacting to small transients in load and generally stablizes behavior.
 */
/* Sample rate of the filter, in Hz. */
var LP_RATE = 5;
/* One sampling interval. */
var LP_INT = Math.round(1000 / LP_RATE);
/* The filter taps array. See below for parameters' meaning. */
var LP_TAPS = genTaps(128, -0.2);

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
	mod_utils.assertClaimDelay(options.targetClaimDelay);

	mod_assert.object(options.recovery, 'options.recovery');
	mod_utils.assertRecoverySet(options.recovery);
	this.p_recovery = options.recovery;

	mod_assert.optionalObject(options.log, 'options.log');
	this.p_log = options.log || mod_bunyan.createLogger({
		name: 'cueball'
	});
	this.p_log = this.p_log.child({
		component: 'CueBallConnectionPool',
		domain: options.domain,
		service: options.service,
		pool: this.p_uuid
	});

	this.p_collector = mod_utils.createErrorMetrics(options);

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
	this.p_lastrate = {};

	mod_assert.optionalNumber(options.maxChurnRate, 'options.maxChurnRate');
	if (options.maxChurnRate !== null &&
	    options.maxChurnRate !== undefined) {
		this.p_maxrate = options.maxChurnRate;
	} else {
		this.p_maxrate = Infinity;
	}

	this.p_lastRebalance = undefined;
	this.p_inRebalance = false;
	this.p_rebalScheduled = false;
	this.p_startedResolver = false;
	this.p_lpf = new FIRFilter(LP_TAPS);

	this.p_idleq = new Queue();
	this.p_initq = new Queue();
	this.p_waiters = new Queue();

	this.p_codel = null;

	if (Number.isFinite(options.targetClaimDelay)) {
		this.p_codel =
		    new mod_codel.ControlledDelay(options.targetClaimDelay);
	}

	this.p_lastError = undefined;

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

	mod_assert.optionalFinite(options.decoherenceInterval,
	    'options.decoherenceInterval');
	var shuffleIntvl = options.decoherenceInterval;
	if (shuffleIntvl === undefined || shuffleIntvl === null ||
	    shuffleIntvl < 60) {
		shuffleIntvl = 60;
	}
	this.p_shuffleTimer = new EventEmitter();
	this.p_shuffleTimerInst = setInterval(function () {
		self.p_shuffleTimer.emit('timeout');
	}, shuffleIntvl * 1000);
	this.p_shuffleTimerInst.unref();

	this.p_lastRebalClamped = false;

	this.p_rateDelayTimer = undefined;

	this.p_lpTimer = setInterval(function () {
		var conns = 0;
		Object.keys(self.p_connections).forEach(function (k) {
			conns += self.p_connections[k].length;
		});
		var spares = self.p_idleq.length + self.p_initq.length;
		var busy = conns - spares;
		self.p_lpf.put(busy + self.p_spares);

		if (self.p_lastRebalClamped)
			self.rebalance();
	}, LP_INT);
	this.p_lpTimer.unref();

	FSM.call(this, 'starting');
}
mod_util.inherits(CueBallConnectionPool, FSM);

CueBallConnectionPool.prototype._incrCounter = function (counter) {
	mod_utils.updateErrorMetrics(this.p_collector, this.p_uuid, counter);
	if (this.p_counters[counter] === undefined)
		this.p_counters[counter] = 0;
	++this.p_counters[counter];
};

CueBallConnectionPool.prototype._hwmCounter = function (counter, val) {
	if (this.p_counters[counter] === undefined) {
		this.p_counters[counter] = val;
		return;
	}
	if (this.p_counters[counter] < val)
		this.p_counters[counter] = val;
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
	mod_assert.notStrictEqual(idx, -1, 'resolver key ' + k + ' not found');
	this.p_keys.splice(idx, 1);
	delete (this.p_backends[k]);
	delete (this.p_dead[k]);

	/*
	 * The p_connections entry for this backend will be deleted once it's
	 * empty (in the stateChanged handler for the slot FSM). That handler
	 * will also call rebalance() for us. All we have to do here is set
	 * "unwanted" on all of them.
	 *
	 * Note that the same resolver could be added and removed multiple
	 * times by the time these slot FSMs have come to rest.
	 */
	var conns = this.p_connections[k] || [];
	conns.forEach(function (fsm) {
		fsm.setUnwanted();
	});
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
		this.p_lastError = new mod_verror.VError(
		    this.p_resolver.getLastError(),
		    'Pool resolver entered state "failed"');
		S.gotoState('failed');
		return;
	}

	S.on(this.p_resolver, 'stateChanged', function (state) {
		if (state === 'failed') {
			self.p_log.warn('underlying resolver failed, moving ' +
			    'pool to "failed" state');
			self.p_lastError = new mod_verror.VError(
			    self.p_resolver.getLastError(),
			    'Pool resolver entered state "failed"');
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
		this._hwmCounter('max-dead-backends', dead);
		if (dead >= self.p_keys.length) {
			self.p_log.warn(
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

	this._incrCounter('failed-state');

	/* Fail all outstanding claims that are waiting for a connection. */
	while (!this.p_waiters.isEmpty()) {
		var hdl = this.p_waiters.shift();
		if (hdl.isInState('waiting')) {
			hdl.fail(new mod_errors.PoolFailedError(self,
			    self.p_lastError));
		}
	}
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
		this._hwmCounter('max-dead-backends', dead);
		if (dead >= self.p_keys.length) {
			self.p_log.warn(
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
	this.p_closingForEach = mod_vasync.forEachParallel({
		func: closeBackend,
		inputs: fsms
	}, function () {
		S.gotoState('stopped');
	});
	function closeBackend(fsm, cb) {
		fsm.setUnwanted();
		if (fsm.isInState('stopped') || fsm.isInState('failed')) {
			cb();
		} else {
			fsm.on('stateChanged', function (st) {
				if (st === 'stopped' || st === 'failed')
					cb();
			});
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
	clearInterval(this.p_lpTimer);
};

CueBallConnectionPool.prototype.shouldRetryBackend = function (backend) {
	return (this.p_backends[backend] !== undefined);
};

CueBallConnectionPool.prototype.isDeclaredDead = function (backend) {
	return (this.p_dead[backend] === true);
};

CueBallConnectionPool.prototype.getLastError = function () {
	return (this.p_lastError);
};

CueBallConnectionPool.prototype.reshuffle = function () {
	if (this.p_keys.length <= 1)
		return;
	var taken = this.p_keys.pop();
	var idx = Math.floor(Math.random() * (this.p_keys.length + 1));

	var self = this;
	var conns = 0;
	Object.keys(this.p_connections).forEach(function (k) {
		conns += self.p_connections[k].length;
	});
	if (this.p_keys.length > conns && idx < conns) {
		this.p_log.info('random shuffle puts backend "%s" at idx %d',
		    taken, idx);
	}

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

	var min = Math.ceil(this.p_lpf.get());
	if (target < min * 1.05) {
		target = min;
		this.p_lastRebalClamped = true;
	} else {
		this.p_lastRebalClamped = false;
	}

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
	var now = Date.now() / 1000.0;
	var rateDelay;
	plan.remove.forEach(function (fsm) {
		var k = fsm.getBackend().key;
		var lastrate = self.p_lastrate[k];
		var n = (self.p_connections[k] || []).length - 1;
		if (lastrate) {
			var tdelta = now - lastrate.time;
			var ndelta = n - lastrate.count;
			var rate = Math.abs(ndelta / tdelta);
			if (rate > self.p_maxrate) {
				var tnext = lastrate.time +
				    Math.abs(ndelta) / self.p_maxrate;
				var delay = tnext - now;
				if (rateDelay === undefined ||
				    delay < rateDelay)
					rateDelay = delay;
				return;
			}
		}
		self.p_lastrate[k] = { time: now, count: n };

		/* This slot is no longer wanted. */
		fsm.setUnwanted();
		/*
		 * We may have changed to stopped or failed synchronously after
		 * setting unwanted. If we have, don't count this as a socket
		 * against our cap (it's been destroyed).
		 */
		if (fsm.isInState('stopped') || fsm.isInState('failed')) {
			--total;
		}
	});
	plan.add.forEach(function (k) {
		var lastrate = self.p_lastrate[k];
		var n = (self.p_connections[k] || []).length + 1;
		if (lastrate) {
			var tdelta = now - lastrate.time;
			var ndelta = n - lastrate.count;
			var rate = Math.abs(ndelta / tdelta);
			if (rate > self.p_maxrate) {
				var tnext = lastrate.time +
				    Math.abs(ndelta) / self.p_maxrate;
				var delay = tnext - now;
				if (rateDelay === undefined ||
				    delay < rateDelay)
					rateDelay = delay;
				return;
			}
		}
		self.p_lastrate[k] = { time: now, count: n };
		/* Make sure we *never* exceed our socket limit. */
		if (++total > self.p_max)
			return;
		self.addConnection(k);
	});

	if (rateDelay !== undefined) {
		if (this.p_rateDelayTimer !== undefined)
			clearTimeout(this.p_rateDelayTimer);
		this.p_rateDelayTimer = setTimeout(function () {
			self.rebalance();
		}, Math.round(rateDelay * 1000 + 10));
	}

	this.p_inRebalance = false;
	this.p_lastRebalance = new Date();
};

CueBallConnectionPool.prototype.addConnection = function (key) {
	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	var backend = this.p_backends[key];
	backend.key = key;

	var fsm = new ConnectionSlotFSM({
		constructor: this.p_constructor,
		backend: backend,
		log: this.p_log,
		pool: this,
		checker: this.p_checker,
		checkTimeout: this.p_checkTimeout,
		recovery: this.p_recovery,
		monitor: (this.p_dead[key] === true)
	});
	if (this.p_connections[key] === undefined)
		this.p_connections[key] = [];
	this.p_connections[key].push(fsm);

	fsm.p_initq_node = this.p_initq.push(fsm);

	var self = this;
	fsm.on('stateChanged', function (newState) {
		if (fsm.p_initq_node) {
			/* These transitions mean we're still starting up. */
			if (newState === 'init' || newState === 'connecting' ||
			    newState === 'retrying')
				return;
			/*
			 * As soon as we transition out of the init stages
			 * we should drop ourselves from the init queue.
			 */
			fsm.p_initq_node.remove();
			delete (fsm.p_initq_node);
		}

		if (newState === 'idle') {
			self.emit('connectedToBackend', key, fsm);

			if (self.p_dead[key] !== undefined) {
				delete (self.p_dead[key]);
				self.rebalance();
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
				fsm.setUnwanted();
				return;
			}

			/*
			 * Try to eat up any waiters that are waiting on a
			 * new connection.
			 */
			while (self.p_waiters.length > 0) {
				var hdl = self.p_waiters.shift();
				var drop = self.p_codel !== null &&
				    self.p_codel.overloaded(hdl.ch_started);

				if (!hdl.isInState('waiting')) {
					continue;
				}

				if (drop) {
					hdl.timeout();
					continue;
				}

				hdl.try(fsm);
				return;
			}

			if (self.p_codel !== null) {
				self.p_codel.empty();
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
		if (newState === 'busy' && fsm.isRunningPing() &&
		    !fsm.p_initq_node) {
			fsm.p_initq_node = self.p_initq.push(fsm);
		}

		if (newState === 'failed') {
			/*
			 * Don't add a "dead" marking if the backend has
			 * been removed from the resolver.
			 */
			if (self.p_backends[key] !== undefined)
				self.p_dead[key] = true;

			var err = fsm.getSocketMgr().getLastError();
			if (err !== undefined)
				self.p_lastError = err;
		}

		if (newState === 'stopped' || newState === 'failed') {
			if (self.p_connections[key]) {
				var idx = self.p_connections[key].indexOf(fsm);
				mod_assert.notStrictEqual(idx, -1);
				self.p_connections[key].splice(idx, 1);
				if (self.p_connections[key].length < 1)
					delete (self.p_connections[key]);
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

CueBallConnectionPool.prototype.printConnections = function () {
	var self = this;
	var obj = { connections: {} };
	var ks = self.p_keys.slice();
	Object.keys(self.p_connections).forEach(function (k) {
		if (ks.indexOf(k) === -1)
			ks.push(k);
	});
	ks.forEach(function (k) {
		var conns = self.p_connections[k] || [];
		obj.connections[k] = {};
		conns.forEach(function (fsm) {
			var s = fsm.getState();
			if (obj.connections[k][s] === undefined)
				obj.connections[k][s] = 0;
			++obj.connections[k][s];
		});
	});
	console.log('live:', obj.connections);
	console.log('dead:', self.p_dead);
};

CueBallConnectionPool.prototype.getStats = function () {
	var self = this;

	// Get a snapshot of current counter values.
	var counters = {};
	Object.keys(this.p_counters).forEach(function (k) {
		counters[k] = self.p_counters[k];
	});

	var tconns = 0;
	Object.keys(self.p_connections).forEach(function (k) {
		tconns += self.p_connections[k].length;
	});

	var stats = {
		'counters': counters,
		'totalConnections': tconns,
		'idleConnections': self.p_idleq.length,
		'pendingConnections': self.p_initq.length,
		'waiterCount': self.p_waiters.length
	};

	return (stats);
};

CueBallConnectionPool.prototype.claim = function (options, cb) {
	var self = this;
	var done = false;
	var handle;
	var timeout;

	if (typeof (options) === 'function' && cb === undefined) {
		cb = options;
		options = {};
	}
	mod_assert.object(options, 'options');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	mod_assert.optionalBool(options.errorOnEmpty, 'options.errorOnEmpty');
	var errOnEmpty = options.errorOnEmpty;

	if (this.p_codel !== null) {
		if (typeof (options.timeout) === 'number') {
			throw (new Error('options.timeout not allowed when ' +
			    'targetDelay has been set'));
		}

		timeout = this.p_codel.getMaxIdle();
	} else if (typeof (options.timeout) === 'number') {
		timeout = options.timeout;
	} else {
		timeout = Infinity;
	}

	this._incrCounter('claim');

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
			if (!done) {
				cb(new mod_errors.PoolFailedError(self,
				    self.p_lastError));
			}
			done = true;
		});
		return ({
			cancel: function () { done = true; }
		});
	}

	var e = mod_utils.maybeCaptureStackTrace();

	handle = new CueBallClaimHandle({
		pool: this,
		claimStack: e.stack,
		callback: cb,
		log: this.p_log,
		claimTimeout: timeout
	});

	function waitingListener(st) {
		if (st === 'waiting') {
			tryNext();
		}
	}
	handle.on('stateChanged', waitingListener);

	function tryNext() {
		if (!handle.isInState('waiting'))
			return;

		/* If there are idle connections sitting around, take one. */
		while (self.p_idleq.length > 0) {
			var fsm = self.p_idleq.shift();
			delete (fsm.p_idleq_node);
			/*
			 * Since 'stateChanged' is emitted async from
			 * mooremachine, things may be on the idle queue still
			 * but not actually idle. If we find one, just rip it
			 * off the queue (which we've already done) and try the
			 * next thing. The state mgmt callback from
			 * addConnection will cope.
			 */
			if (!fsm.isInState('idle'))
				continue;

			handle.try(fsm);

			return;
		}

		if (errOnEmpty && self.p_resolver.count() < 1) {
			var err = new mod_errors.NoBackendsError(self,
			    self.p_resolver.getLastError());
			handle.fail(err);
		}

		/* Otherwise add an entry on the "waiter" queue. */
		self.p_waiters.push(handle);

		self._hwmCounter('max-claim-queue', self.p_waiters.length);
		self._incrCounter('queued-claim');

		self.rebalance();
	}

	return (handle);
};
