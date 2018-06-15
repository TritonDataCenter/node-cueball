/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2018 Joyent, Inc.
 */

module.exports = {
	ConnectionSet: CueBallConnectionSet
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
const mod_monitor = require('./pool-monitor');

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

const Queue = require('./queue');
const mod_cfsm = require('./connection-fsm');
const ConnectionSlotFSM = mod_cfsm.ConnectionSlotFSM;
const CueBallClaimHandle = mod_cfsm.CueBallClaimHandle;

function CueBallConnectionSet(options) {
	mod_assert.object(options);

	mod_assert.func(options.constructor, 'options.constructor');

	this.cs_uuid = mod_uuid.v4();
	this.cs_constructor = options.constructor;

	mod_assert.object(options.resolver, 'options.resolver');
	this.cs_resolver = options.resolver;

	mod_assert.object(options.recovery, 'options.recovery');
	mod_utils.assertRecoverySet(options.recovery);
	this.cs_recovery = options.recovery;

	mod_assert.optionalBool(options.connectionHandlesError,
	    'options.connectionHandlesError');
	this.cs_connHandlesErr = !!(options.connectionHandlesError);

	mod_assert.optionalObject(options.log, 'options.log');
	this.cs_log = options.log || mod_bunyan.createLogger({
		name: 'cueball'
	});
	this.cs_log = this.cs_log.child({
		component: 'CueBallConnectionSet',
		domain: options.domain,
		service: options.service,
		cset: this.cs_uuid
	});

	this.cs_collector = mod_utils.createErrorMetrics(options);

	mod_assert.number(options.target, 'options.target');
	mod_assert.number(options.maximum, 'options.maximum');
	this.cs_target = options.target;
	this.cs_max = options.maximum;

	/* Array of string keys for all currently available backends. */
	this.cs_keys = [];
	/* Map of backend key => backend info objects. */
	this.cs_backends = {};
	/* Map of backend key => ConnectionSlotFSM instance. */
	this.cs_fsm = {};
	/* Map of backend key => bool, if true the backend is declared dead. */
	this.cs_dead = {};

	/*
	 * Map of backend key => integer, latest serial number for connections
	 * to each backend. We use the serial numbers to generate the per-
	 * connection keys.
	 */
	this.cs_serials = {};
	/*
	 * Map of connection key => connection instance (as returned by
	 * cs_constructor).
	 */
	this.cs_connections = {};
	/* Map of backend key => Array of connection keys. */
	this.cs_connectionKeys = {};
	/* Map of ckey => LogicalConnection. */
	this.cs_lconns = {};


	/* For debugging, track when we last rebalanced. */
	this.cs_lastRebalance = undefined;

	/*
	 * If true, we are currently doing a rebalance. Rebalancing may involve
	 * closing connections or opening new ones, which can then trigger a
	 * recursive call into rebalance(), so we use this flag to avoid
	 * problems caused by this.
	 */
	this.cs_inRebalance = false;
	/*
	 * If true, a rebalance() is scheduled for the next turn of the event
	 * loop (in a setImmediate).
	 */
	this.cs_rebalScheduled = false;

	/* Debugging counters. */
	this.cs_counters = {};
	this.cs_lastError = undefined;

	var self = this;

	this.cs_rebalTimer = new EventEmitter();
	this.cs_rebalTimerInst = setInterval(function () {
		self.cs_rebalTimer.emit('timeout');
	}, 10000);
	this.cs_rebalTimerInst.unref();

	this.cs_shuffleTimer = new EventEmitter();
	this.cs_shuffleTimerInst = setInterval(function () {
		self.cs_shuffleTimer.emit('timeout');
	}, 60000);
	this.cs_shuffleTimerInst.unref();

	FSM.call(this, 'starting');
}
mod_util.inherits(CueBallConnectionSet, FSM);

CueBallConnectionSet.prototype.on_resolver_added = function (k, backend) {
	backend.key = k;
	mod_assert.strictEqual(this.cs_keys.indexOf(k), -1,
	    'Resolver key is a duplicate');
	var idx = Math.floor(Math.random() * (this.cs_keys.length + 1));
	this.cs_keys.splice(idx, 0, k);
	this.cs_backends[k] = backend;
	this.rebalance();
};

CueBallConnectionSet.prototype.on_resolver_removed = function (k) {
	var self = this;

	var idx = this.cs_keys.indexOf(k);
	mod_assert.notStrictEqual(idx, -1, 'Resolver removed key that is not ' +
	    'present in cs_keys');
	this.cs_keys.splice(idx, 1);
	delete (this.cs_backends[k]);
	delete (this.cs_dead[k]);

	var fsm = self.cs_fsm[k];
	if (fsm !== undefined)
		fsm.setUnwanted();

	var cks = this.cs_connectionKeys[k];
	(cks || []).forEach(function (ck) {
		var lconn = self.cs_lconns[ck];
		if (!lconn.isInState('stopped'))
			lconn.drain();
	});
};

CueBallConnectionSet.prototype.isDeclaredDead = function (backend) {
	return (this.cs_dead[backend] === true);
};

CueBallConnectionSet.prototype.shouldRetryBackend = function (backend) {
	return (this.cs_backends[backend] !== undefined);
};

CueBallConnectionSet.prototype.state_starting = function (S) {
	S.validTransitions(['failed', 'running', 'stopping']);
	mod_monitor.monitor.registerSet(this);

	S.on(this.cs_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.cs_resolver, 'removed', this.on_resolver_removed.bind(this));

	var self = this;

	if (this.cs_resolver.isInState('failed')) {
		this.cs_log.warn('resolver has already failed, cset will ' +
		    'start up in "failed" state');
		this.cs_lastError = this.cs_resolver.getLastError();
		S.gotoState('failed');
		return;
	}

	S.on(this.cs_resolver, 'stateChanged', function (st) {
		if (st === 'failed') {
			self.cs_log.warn('underlying resolver failed, moving ' +
			    'cset to "failed" state');
			self.cs_lastError = self.cs_resolver.getLastError();
			S.gotoState('failed');
		}
	});

	if (this.cs_resolver.isInState('running')) {
		var backends = this.cs_resolver.list();
		Object.keys(backends).forEach(function (k) {
			var backend = backends[k];
			self.on_resolver_added(k, backend);
		});
	}

	S.on(this, 'connectedToBackend', function () {
		S.gotoState('running');
	});

	S.on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.cs_dead).length;
		if (dead >= self.cs_keys.length) {
			self.cs_log.warn(
			    { dead: dead },
			    'cset has exhausted all retries, now moving to ' +
			    '"failed" state');
			S.gotoState('failed');
		}
	});

	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
};

CueBallConnectionSet.prototype.state_failed = function (S) {
	S.validTransitions(['running', 'stopping']);
	S.on(this.cs_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.cs_resolver, 'removed', this.on_resolver_removed.bind(this));
	S.on(this.cs_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	var self = this;
	S.on(this, 'connectedToBackend', function () {
		mod_assert.ok(!self.cs_resolver.isInState('failed'));
		self.cs_log.info('successfully connected to a backend, ' +
		    'moving back to running state');
		S.gotoState('running');
	});

	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
};

CueBallConnectionSet.prototype.state_running = function (S) {
	S.validTransitions(['failed', 'stopping']);
	var self = this;
	S.on(this.cs_resolver, 'added', this.on_resolver_added.bind(this));
	S.on(this.cs_resolver, 'removed', this.on_resolver_removed.bind(this));
	S.on(this.cs_rebalTimer, 'timeout', this.rebalance.bind(this));
	S.on(this.cs_shuffleTimer, 'timeout', this.reshuffle.bind(this));

	S.on(this, 'closedBackend', function (fsm) {
		var dead = Object.keys(self.cs_dead).length;
		if (dead >= self.cs_keys.length) {
			self.cs_log.warn(
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

CueBallConnectionSet.prototype.state_stopping = function (S) {
	S.validTransitions(['stopped']);
	var conns = this.cs_fsm;
	var fsms = [];
	var self = this;
	this.cs_backends = {};
	Object.keys(conns).forEach(function (k) {
		fsms.push(conns[k]);
	});
	mod_vasync.forEachParallel({
		func: closeBackend,
		inputs: fsms
	}, function () {
		S.gotoState('stopped');
	});
	function closeBackend(fsm, cb) {
		if (fsm.isInState('closed')) {
			cb();
			return;
		}

		var k = fsm.csf_backend.key;
		var cks = self.cs_connectionKeys[k];
		fsm.on('stateChanged', function (s) {
			if (s === 'stopped' || s === 'failed')
				cb();
		});
		fsm.setUnwanted();
		cks.forEach(function (ck) {
			/*
			 * Do this async, to avoid problems of state machine
			 * loops if .stop() was called from an 'added' handler.
			 */
			var lconn = self.cs_lconns[ck];
			setImmediate(function () {
				if (!lconn.isInState('stopped')) {
					lconn.drain();
				}
			});
		});
	}
};

CueBallConnectionSet.prototype.state_stopped = function (S) {
	S.validTransitions([]);
	mod_monitor.monitor.unregisterSet(this);
	this.cs_keys = [];
	this.cs_fsm = {};
	this.cs_connections = {};
	this.cs_backends = {};
	clearInterval(this.cs_rebalTimerInst);
	clearInterval(this.cs_shuffleTimerInst);
};

CueBallConnectionSet.prototype.isDeclaredDead = function (backend) {
	return (this.cs_dead[backend] === true);
};

CueBallConnectionSet.prototype.reshuffle = function () {
	if (this.cs_keys.length <= 1)
		return;
	var taken = this.cs_keys.pop();
	var idx = Math.floor(Math.random() * (this.cs_keys.length + 1));
	if (this.cs_keys.length > this.cs_target && idx < this.cs_target) {
		this.cs_log.info('random shuffle puts backend "%s" at idx %d',
		    taken, idx);
	}
	this.cs_keys.splice(idx, 0, taken);
	this.rebalance();
};

/* Stop and kill everything. */
CueBallConnectionSet.prototype.stop = function () {
	this.emit('stopAsserted');
};

CueBallConnectionSet.prototype.setTarget = function (target) {
	this.cs_target = target;
	this.rebalance();
};

CueBallConnectionSet.prototype.rebalance = function () {
	if (this.cs_keys.length < 1)
		return;

	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	if (this.cs_rebalScheduled !== false)
		return;

	this.p_rebalScheduled = true;
	var self = this;
	setImmediate(function () {
		self._rebalance();
	});
};

/*
 * Rebalance the set, by looking at the distribution of connections to
 * backends amongst the "init" and "idle" queues.
 *
 * If the connections are not evenly distributed over the available backends,
 * then planRebalance() will return a plan to take us back to an even
 * distribution, which we then apply.
 */
CueBallConnectionSet.prototype._rebalance = function () {
	var self = this;

	if (this.cs_inRebalance !== false)
		return;
	this.cs_inRebalance = true;
	this.cs_rebalScheduled = false;

	var conns = {};
	var total = 0;
	var busy = 0;
	this.cs_keys.forEach(function (k) {
		conns[k] = [];
		if (self.cs_fsm[k] !== undefined) {
			conns[k].push(self.cs_fsm[k]);
			if (self.cs_fsm[k].isInState('busy'))
				++busy;
			++total;
		}
	});

	var plan = mod_utils.planRebalance(
	    conns, this.cs_dead, this.cs_target, this.cs_max, true);

	if (plan.remove.length > 0 || plan.add.length > 0) {
		this.cs_log.trace('rebalancing cset, remove %d, ' +
		    'add %d (target = %d, total = %d)',
		    plan.remove.length, plan.add.length, this.cs_target,
		    total);
	}
	plan.remove.forEach(function (fsm) {
		/*
		 * Never deliberately remove our last working connection. We
		 * should wait for another connection to be up and running first
		 * and then remove this one.
		 */
		if (busy <= 1)
			return;

		var k = fsm.csf_backend.key;

		/* Subtract from "busy" so the "if" above does its job. */
		if (fsm.isInState('busy'))
			--busy;
		fsm.setUnwanted();

		/* We might have stopped synchronously. */
		if (fsm.isInState('stopped') || fsm.isInState('failed')) {
			delete (self.cs_fsm[k]);
			--total;
		}

		/*
		 * Find any advertised connections from this FSM, and (after
		 * setting the unwanted FSM flag to avoid them retrying), emit
		 * 'removed' so our consumer will close them.
		 */
		var cks = self.cs_connectionKeys[k];
		cks.forEach(function (ck) {
			var lconn = self.cs_lconns[ck];
			if (!lconn.isInState('stopped'))
				lconn.drain();
		});
	});
	plan.add.forEach(function (k) {
		/* Make sure we never exceed our socket limit. */
		if (++total > (self.cs_max + 1))
			return;
		/* Never make more than one slot for the same backend. */
		if (self.cs_fsm[k] !== undefined)
			return;

		self.addConnection(k);
	});

	this.cs_inRebalance = false;
	this.cs_lastRebalance = new Date();
};

CueBallConnectionSet.prototype.assertEmit = function () {
	var args = arguments;
	var event = args[0];
	if (this.listeners(event).length < 1) {
		throw (new Error('Event "' + event + '" on ConnectionSet ' +
		    'must be handled'));
	}
	return (this.emit.apply(this, args));
};

CueBallConnectionSet.prototype.createLogiConn = function (key) {
	var fsm = this.cs_fsm[key];
	if (this.cs_serials[key] === undefined)
		this.cs_serials[key] = 1;
	if (this.cs_connectionKeys[key] === undefined)
		this.cs_connectionKeys[key] = [];

	var serial = this.cs_serials[key]++;
	var ckey = key + '.' + serial;
	this.cs_connectionKeys[key].push(ckey);

	var lconn = new LogicalConnection({
		set: this,
		log: this.cs_log,
		key: key,
		ckey: ckey,
		fsm: fsm
	});
	this.cs_lconns[ckey] = lconn;

	var self = this;
	lconn.on('stateChanged', function (st) {
		if (st === 'stopped') {
			/*
			 * Once the Logical Connection has stopped, clean up all
			 * the entries we had about it.
			 */
			delete (self.cs_lconns[ckey]);
			var cks = self.cs_connectionKeys[key];
			var idx = cks.indexOf(ckey);
			mod_assert.notStrictEqual(idx, -1);
			cks.splice(idx, 1);

			/*
			 * Now, if this FSM is planning to contribute another
			 * connection back to the Set, we want to make a new
			 * Logical Connection for the next serial number.
			 */

			/*
			 * If the backend is gone from the resolver, we
			 * definitely aren't going to get a new connection
			 * out of this backend.
			 */
			if (self.cs_backends[key] === undefined)
				return;

			/* If the FSM has failed or stopped, likewise. */
			if (fsm.isInState('failed') || fsm.isInState('stopped'))
				return;

			self.createLogiConn(key);
		}
	});
};

CueBallConnectionSet.prototype.addConnection = function (key) {
	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	var backend = this.cs_backends[key];
	backend.key = key;

	var fsm = new ConnectionSlotFSM({
		constructor: this.cs_constructor,
		backend: backend,
		log: this.cs_log,
		pool: this,
		recovery: this.cs_recovery,
		monitor: (this.cs_dead[key] === true)
	});
	mod_assert.strictEqual(this.cs_fsm[key], undefined);
	this.cs_fsm[key] = fsm;

	this.createLogiConn(key);

	var self = this;

	/*
	 * We want to rebalance when an FSM either reaches idle or leaves it.
	 * These are the points at which we can meaningfully change what we're
	 * planning to do (rebalancing at other times is fine, but wasted
	 * effort).
	 */
	var wasIdle = false;

	fsm.on('stateChanged', function (newState) {
		if (newState === 'idle') {
			self.emit('connectedToBackend', key, fsm);

			/* We got a connection, so we're not dead. */
			if (self.cs_dead[key] !== undefined) {
				delete (self.cs_dead[key]);
			}

			self.rebalance();
			wasIdle = true;
			return;
		}

		if (wasIdle) {
			wasIdle = false;
			self.rebalance();
		}

		if (newState === 'failed') {
			/*
			 * Set the dead flag, but not on a backend that's no
			 * longer in the resolver.
			 */
			if (self.cs_backends[key] !== undefined) {
				self.cs_dead[key] = true;
				var err = fsm.getSocketMgr().getLastError();
				if (err !== undefined)
					self.cs_lastError = err;
			}
		}

		if (newState === 'stopped' || newState === 'failed') {
			delete (self.cs_fsm[key]);
			self.emit('closedBackend', fsm);
			self.rebalance();
		}
	});

	fsm.start();
};

CueBallConnectionSet.prototype.getLastError = function () {
	return (this.cs_lastError);
};

CueBallConnectionSet.prototype.getConnections = function () {
	var self = this;
	var conns = [];
	return (Object.keys(this.cs_connections).forEach(function (k) {
		var c = self.cs_connections[k];
		var fsm = self.cs_fsm[c.cs_backendKey];
		var h = self.cs_handles[k];
		if (fsm.isInState('busy') && h.isInState('claimed'))
			conns.push(c);
	}));
};

CueBallConnectionSet.prototype._incrCounter = function (counter) {
	mod_utils.updateErrorMetrics(this.cs_collector, this.cs_uuid, counter);
	if (this.cs_counters[counter] === undefined)
		this.cs_counters[counter] = 0;
	++this.cs_counters[counter];
};

/*
 * The LogicalConnection FSM is used to track the state of a "connection" in
 * a Set. It represents one "connection key", as it progresses from being
 * set up, to being advertised to the end-user ('added' emitted), to being
 * drained ('removed') and torn down.
 *
 * This FSM replaces a bunch of flags that used to exist to note down whether
 * 'added' and 'removed' had been emitted yet for each ckey. The FSM emits
 * events on the Set on its behalf now, as well as managing the ClaimHandles
 * and callbacks.
 *
 *        +
 *        |
 *        |
 *        |
 *        v
 *    +--------+ fsm failed
 *    |        | .drain()
 *    |  init  +-----------------------------+
 *    |        |                             |
 *    +---+----+                             |
 *        | handle                           |
 *        | claimed                          |
 *        |                                  |
 *        |                                  |
 *        v                                  |
 * +--------------+                          |
 * |              | handle closed            |
 * |  advertised  +------------------------->|
 * |              |                          |
 * +------+-------+                          |
 *        | .drain() ||                      |
 *        | smgr !connected                  |
 *        |                                  |
 *        |                                  |
 *        |                                  |
 *        v                                  v
 *  +------------+                     +-----------+
 *  |            |                     |           |
 *  |  draining  +-------------------> |  stopped  |
 *  |            | hdl rel./closed     |           |
 *  +------------+                     +-----------+
 *
 */
function LogicalConnection(options) {
	mod_assert.object(options, 'options');
	mod_assert.object(options.set, 'options.set');
	mod_assert.string(options.key, 'options.key');
	mod_assert.string(options.ckey, 'options.ckey');
	mod_assert.object(options.fsm, 'options.fsm');
	mod_assert.object(options.log, 'options.log');

	this.lc_set = options.set;
	this.lc_key = options.key;
	this.lc_fsm = options.fsm;
	this.lc_smgr = options.fsm.getSocketMgr();
	this.lc_conn = undefined;
	this.lc_ckey = options.ckey;
	this.lc_hdl = undefined;
	this.lc_log = options.log;

	FSM.call(this, 'init');
}
mod_util.inherits(LogicalConnection, FSM);
LogicalConnection.prototype.drain = function () {
	mod_assert.ok(!this.isInState('stopped'));
	this.emit('drainAsserted');
};
LogicalConnection.prototype.state_init = function (S) {
	S.validTransitions(['advertised', 'stopped']);
	var self = this;

	var hdlOpts = {
		pool: this.lc_set,
		claimStack: 'Error\n' +
		    ' at claim\n' +
		    ' at CueBallConnectionSet.addConnection\n' +
		    ' at CueBallConnectionSet.addConnection',
		callback: S.callback(onClaimed),
		log: this.lc_log,
		throwError: !(this.lc_set.cs_connHandlesErr),
		claimTimeout: Infinity
	};
	this.lc_hdl = new CueBallClaimHandle(hdlOpts);

	function onClaimed(err, hdl, conn) {
		mod_assert.ok(!err);
		mod_assert.strictEqual(hdl, self.lc_hdl);
		self.lc_conn = conn;
		S.gotoState('advertised');
	}

	/*
	 * Keep trying this FSM again and again until we get a claim. It's fine
	 * to attempt multiple times while in this state, because we never
	 * emitted 'added' for this ckey yet.
	 */
	S.on(this.lc_hdl, 'stateChanged', function (st) {
		if (st === 'waiting' && self.lc_hdl.isInState('waiting')) {
			if (self.lc_fsm.isInState('idle')) {
				self.lc_hdl.try(self.lc_fsm);
			}
		} else if (st === 'failed' || st === 'cancelled') {
			S.gotoState('stopped');
		}
	});

	S.on(this.lc_fsm, 'stateChanged', function (st) {
		if (st === 'idle' && self.lc_fsm.isInState('idle')) {
			if (self.lc_hdl.isInState('waiting')) {
				self.lc_hdl.try(self.lc_fsm);
			}
		} else if (st === 'failed') {
			S.gotoState('stopped');
		}
	});

	/*
	 * If the Set calls .drain(), we go straight to stopped. We're no longer
	 * wanted, and we never advertised this connection to the user.
	 */
	S.on(this, 'drainAsserted', function () {
		S.gotoState('stopped');
	});
};
LogicalConnection.prototype.state_advertised = function (S) {
	S.validTransitions(['draining', 'stopped']);

	/*
	 * According to the API docs, the user can call .close() on the handle
	 * at any time, but .release() only *after* we emit 'removed'.
	 */
	S.on(this.lc_hdl, 'stateChanged', function (st) {
		if (st === 'closed') {
			S.gotoState('stopped');
		}
		if (st === 'released') {
			throw (new Error('The .release() method may not be ' +
			    'called on a ConnectionSet handle before ' +
			    '"removed" has been emitted'));
		}
	});

	/*
	 * If the socket disconnects, then move to 'draining' to emit 'removed'.
	 */
	S.on(this.lc_smgr, 'stateChanged', function (st) {
		if (st !== 'connected') {
			S.gotoState('draining');
		}
	});

	/* And also if the Set's rebalancer decided it doesn't want us. */
	S.on(this, 'drainAsserted', function () {
		S.gotoState('draining');
	});

	this.lc_set.assertEmit('added', this.lc_ckey, this.lc_conn,
	    this.lc_hdl);
};
LogicalConnection.prototype.state_draining = function (S) {
	S.validTransitions(['stopped']);

	/*
	 * The only way out of 'draining' is when the user tells us the
	 * connection has drained. This means the handle has to transition.
	 */
	S.on(this.lc_hdl, 'stateChanged', function (st) {
		if (st === 'closed' || st === 'released' || st === 'cancelled')
			S.gotoState('stopped');
	});

	/*
	 * It's fine to call emit directly here, because the only transition
	 * out of this state is via a 'stateChanged' which is always emitted
	 * async -- i.e. we can't loop back into ourselves.
	 */
	this.lc_set.assertEmit('removed', this.lc_ckey, this.lc_conn,
	    this.lc_hdl);
};
LogicalConnection.prototype.state_stopped = function (S) {
	S.validTransitions([]);

	/* Cancel the handle if we got here from init. */
	if (this.lc_hdl.isInState('waiting') ||
	    this.lc_hdl.isInState('claiming')) {
		this.lc_hdl.cancel();
	}
};
