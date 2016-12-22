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
const ConnectionFSM = require('./connection-fsm');

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
		name: 'CueBallConnectionPool'
	});
	this.cs_log = this.cs_log.child({
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
	/* Map of backend key => array of ConnectionFSM instances. */
	this.cs_fsms = {};
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
	var idx = Math.floor(Math.random() * (this.cs_keys.length + 1));
	this.cs_keys.splice(idx, 0, k);
	this.cs_backends[k] = backend;
	this.rebalance();
};

CueBallConnectionSet.prototype.on_resolver_removed = function (k) {
	var self = this;

	var idx = this.cs_keys.indexOf(k);
	if (idx !== -1)
		this.cs_keys.splice(idx, 1);
	delete (this.cs_backends[k]);
	delete (this.cs_dead[k]);

	var cks = Object.keys(this.cs_connections).filter(function (ck) {
		return (ck.indexOf(k + '.') === 0);
	});

	var fsms = self.cs_fsms[k] || [];
	fsms.forEach(function (fsm) {
		if (cks.length > 0 || fsm.isInState('idle')) {
			fsm.closeAfterRelease();
		} else {
			fsm.close();
		}
	});
	cks.forEach(function (ck) {
		var conn = self.cs_connections[ck];
		delete (self.cs_connections[ck]);
		self.assertEmit('removed', ck, conn);
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
	var conns = this.cs_fsms;
	var fsms = [];
	var self = this;
	this.cs_backends = {};
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
		if (fsm.isInState('closed')) {
			cb();
			return;
		}

		var k = fsm.cf_backend.key;
		var cks = Object.keys(self.cs_connections).filter(
		    function (ck) {
			return (ck.indexOf(k + '.') === 0);
		});
		fsm.on('stateChanged', function (s) {
			if (s === 'closed')
				cb();
		});
		if (cks.length === 0 && !fsm.isInState('idle')) {
			fsm.close();
		} else {
			fsm.closeAfterRelease();
			cks.forEach(function (ck) {
				var conn = self.cs_connections[ck];
				delete (self.cs_connections[ck]);
				self.assertEmit('removed', ck, conn);
			});
		}
	}
};

CueBallConnectionSet.prototype.state_stopped = function (S) {
	S.validTransitions([]);
	mod_monitor.monitor.unregisterSet(this);
	this.cs_keys = [];
	this.cs_fsms = {};
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
		conns[k] = (self.cs_fsms[k] || []).slice();
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

		var k = fsm.cf_backend.key;
		/*
		 * Find any advertised connections from this FSM, and (after
		 * setting the closeAfterRelease flag to avoid them retrying),
		 * emit 'removed' so our consumer will close them.
		 */
		var cks = Object.keys(self.cs_connections).filter(
		    function (ck) {
			return (ck.indexOf(k + '.') === 0);
		});
		/*
		 * We can close the FSM immediately if we aren't advertising
		 * any connections for it, and we aren't waiting on our consumer
		 * to close any -- i.e., the FSM is in an error state, probably
		 * delay or connect.
		 *
		 * Still want to avoid doing .close() on the *last* one for a
		 * given backend, so it has a chance to run out of retries if
		 * the backend is in fact dead. So we do .closeAfterRelease()
		 * instead for those.
		 */
		if (cks.length === 0 && !fsm.isInState('idle')) {
			var fsmIdx = self.cs_fsms[k].indexOf(fsm);
			if (fsmIdx > 0 || self.cs_keys.indexOf(k) === -1) {
				fsm.close();
				--total;
			} else {
				fsm.closeAfterRelease();
			}
		} else {
			fsm.closeAfterRelease();
		}
		cks.forEach(function (ck) {
			var conn = self.cs_connections[ck];
			delete (self.cs_connections[ck]);
			self.assertEmit('removed', ck, conn);
		});
	});
	plan.add.forEach(function (k) {
		/* Make sure we never exceed our socket limit. */
		if (++total > (self.cs_max + 1))
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

CueBallConnectionSet.prototype.addConnection = function (key) {
	if (this.isInState('stopping') || this.isInState('stopped'))
		return;

	var backend = this.cs_backends[key];
	backend.key = key;

	var fsm = new ConnectionFSM({
		constructor: this.cs_constructor,
		backend: backend,
		log: this.cs_log,
		pool: this,
		recovery: this.cs_recovery,
		doRef: false
	});
	if (this.cs_fsms[key] === undefined)
		this.cs_fsms[key] = [];
	if (this.cs_serials[key] === undefined)
		this.cs_serials[key] = 1;
	this.cs_fsms[key].push(fsm);
	var serial;
	var ckey;

	var self = this;
	fsm.on('stateChanged', function (newState) {
		if (newState === 'idle' && fsm.isInState('idle')) {
			/*
			 * If the backend has been removed from the resolver,
			 * stop now.
			 */
			if (self.cs_backends[key] === undefined) {
				fsm.close();
				return;
			}

			if (serial === undefined) {
				self.emit('connectedToBackend', key, fsm);
			}
			if (ckey !== undefined && self.cs_connections[ckey]) {
				var conn = self.cs_connections[ckey];
				delete (self.cs_connections[ckey]);
				self.assertEmit('removed', ckey, conn);
			}

			conn = fsm.getConnection();
			serial = self.cs_serials[key]++;
			ckey = key + '.' + serial;
			conn.cs_serial = serial;
			fsm.cs_serial = serial;

			self.cs_connections[ckey] = conn;
			self.assertEmit('added', ckey, conn);

			self.rebalance();
			return;
		}

		if (newState !== 'idle') {
			if (self.cs_connections[ckey]) {
				conn = self.cs_connections[ckey];
				delete (self.cs_connections[ckey]);
				self.assertEmit('removed', ckey, conn);
			}
		}

		if (newState === 'closed') {
			if (self.cs_fsms[key]) {
				var idx = self.cs_fsms[key].indexOf(fsm);
				self.cs_fsms[key].splice(idx, 1);
			}
			/*
			 * Set the dead flag, but not on a backend that's no
			 * longer in the resolver.
			 */
			if (fsm.retriesExhausted() &&
			    self.cs_backends[key] !== undefined) {
				self.cs_dead[key] = true;
			}
			self.emit('closedBackend', fsm);
			self.rebalance();
			return;
		}
	});

	fsm.start();
};

CueBallConnectionSet.prototype.getConnections = function () {
	var self = this;
	return (Object.keys(this.cs_connections).map(function (k) {
		return (self.cs_connections[k]);
	}));
};

CueBallConnectionSet.prototype._incrCounter = function (counter) {
	if (this.cs_counters[counter] === undefined)
		this.cs_counters[counter] = 0;
	++this.cs_counters[counter];
};
