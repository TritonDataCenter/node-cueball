/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
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
const mod_uuid = require('node-uuid');
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
	/* Map of backend key => claim handle. */
	this.cs_handles = {};

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

	/* Map of connection key => bool, if true 'removed' has been emitted. */
	this.cs_emitted = {};

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
		var conn = self.cs_connections[ck];
		var hdl = self.cs_handles[ck];
		if (self.cs_emitted[ck] !== true) {
			self.cs_emitted[ck] = true;
			self.assertEmit('removed', ck, conn, hdl);
		}
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
		S.gotoState('failed');
		return;
	}

	S.on(this.cs_resolver, 'stateChanged', function (st) {
		if (st === 'failed') {
			self.cs_log.warn('underlying resolver failed, moving ' +
			    'cset to "failed" state');
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
			self.cs_log.error(
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
			self.cs_log.error(
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
			var conn = self.cs_connections[ck];
			var hdl = self.cs_handles[ck];
			if (self.cs_emitted[ck] !== true) {
				self.cs_emitted[ck] = true;
				self.assertEmit('removed', ck, conn, hdl);
			}
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
	this.cs_keys.forEach(function (k) {
		conns[k] = [];
		if (self.cs_fsm[k] !== undefined)
			conns[k].push(self.cs_fsm[k]);
		total += conns[k].length;
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
		 * Never deliberately remove our last connection. We should wait
		 * for another connection to be up and running first and then
		 * remove this one.
		 */
		if (total <= 1)
			return;

		var k = fsm.csf_backend.key;
		/*
		 * Find any advertised connections from this FSM, and (after
		 * setting the closeAfterRelease flag to avoid them retrying),
		 * emit 'removed' so our consumer will close them.
		 */
		var cks = Object.keys(self.cs_connections).filter(
		    function (ck) {
			return (ck.indexOf(k + '.') === 0);
		});
		fsm.setUnwanted();
		if (fsm.isInState('stopped') || fsm.isInState('failed')) {
			delete (self.cs_fsm[k]);
			--total;
		}
		cks.forEach(function (ck) {
			var conn = self.cs_connections[ck];
			var hdl = self.cs_handles[ck];
			if (self.cs_emitted[ck] !== true) {
				self.cs_emitted[ck] = true;
				self.assertEmit('removed', ck, conn, hdl);
			}
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

function forceClaim(handle, fsm) {
	handle.on('stateChanged', hdlStateListener);

	function hdlStateListener(st) {
		if (st === 'waiting' && handle.isInState('waiting')) {
			if (fsm.isInState('idle')) {
				handle.try(fsm);
			} else {
				fsm.on('stateChanged', fsmStateListener);
			}
		}
	}

	function fsmStateListener(st) {
		if (st === 'idle' && fsm.isInState('idle')) {
			fsm.removeListener('stateChanged', fsmStateListener);
			if (handle.isInState('waiting')) {
				handle.try(fsm);
			}
		}
	}
}

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
	if (this.cs_serials[key] === undefined)
		this.cs_serials[key] = 1;
	if (this.cs_connectionKeys[key] === undefined)
		this.cs_connectionKeys[key] = [];
	mod_assert.strictEqual(this.cs_fsm[key], undefined);
	this.cs_fsm[key] = fsm;
	var serial;
	var ckey;
	var smgr = fsm.getSocketMgr();

	var self = this;
	fsm.on('stateChanged', function (newState) {
		if (newState === 'busy' && fsm.isInState('busy')) {
			mod_assert.notStrictEqual(ckey, undefined);
			mod_assert.notStrictEqual(self.cs_connections[ckey],
			    undefined);
			return;
		}

		/*
		 * If we already have a ckey set for this FSM, and it exists in
		 * cs_connections, then we previously got a connection and
		 * claimed it. Any state transition (other than to "busy",
		 * which was excluded above) now means this connection's
		 * claim handle has been released/closed and we must clean up
		 * the associated entries.
		 */
		if (ckey !== undefined &&
		    self.cs_connections[ckey] !== undefined) {
			mod_assert.ok(self.cs_emitted[ckey]);

			delete (self.cs_connections[ckey]);
			delete (self.cs_emitted[ckey]);
			delete (self.cs_handles[ckey]);

			var cks = self.cs_connectionKeys[key];
			var ckIdx = cks.indexOf(ckey);
			mod_assert.notStrictEqual(ckIdx, -1);
			cks.splice(ckIdx, 1);
		}

		if (newState === 'idle' && fsm.isInState('idle')) {
			/*
			 * If the backend has been removed from the resolver,
			 * stop now.
			 */
			if (self.cs_backends[key] === undefined) {
				fsm.setUnwanted();
				return;
			}

			if (serial === undefined) {
				self.emit('connectedToBackend', key, fsm);
			}

			serial = self.cs_serials[key]++;
			ckey = key + '.' + serial;
			fsm.cs_serial = serial;

			var hdlOpts = {
				pool: self,
				claimStack: 'Error\n' +
				    ' at claim\n' +
				    ' at CueBallConnectionSet.addConnection\n' +
				    ' at CueBallConnectionSet.addConnection',
				callback: afterClaim,
				log: self.cs_log,
				claimTimeout: Infinity
			};
			var handle = new CueBallClaimHandle(hdlOpts);

			self.cs_handles[ckey] = handle;
			forceClaim(handle, fsm);

			function afterClaim(err, hdl, conn) {
				mod_assert.ok(!err);

				conn.cs_serial = serial;
				conn.cs_backendKey = key;
				self.cs_connections[ckey] = conn;
				self.cs_connectionKeys[key].push(ckey);

				self.assertEmit('added', ckey, conn, hdl);

				self.rebalance();
			}
			return;
		}

		if (newState === 'failed') {
			/*
			 * Set the dead flag, but not on a backend that's no
			 * longer in the resolver.
			 */
			if (self.cs_backends[key] !== undefined) {
				self.cs_dead[key] = true;
			}
		}

		if (newState === 'stopped' || newState === 'failed') {
			delete (self.cs_fsm[key]);
			self.emit('closedBackend', fsm);
			self.rebalance();
			return;
		}
	});

	smgr.on('stateChanged', function (newState) {
		if (!fsm.isInState('busy'))
			return;
		if (newState === 'connected')
			return;
		/*
		 * A transition out of 'connected' while the slot is still
		 * 'busy' indicates that we lost the connection. We should
		 * emit 'removed' for our clients.
		 */
		mod_assert.string(ckey);
		if (self.cs_emitted[ckey] !== true) {
			self.cs_emitted[ckey] = true;
			self.assertEmit('removed', ckey,
			    self.cs_connections[ckey], self.cs_handles[ckey]);
		}
	});

	fsm.start();
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
	if (this.cs_counters[counter] === undefined)
		this.cs_counters[counter] = 0;
	++this.cs_counters[counter];
};
