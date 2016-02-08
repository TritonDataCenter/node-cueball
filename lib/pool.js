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

const FSM = mod_mooremachine.FSM;
const EventEmitter = mod_events.EventEmitter;

function QueueNode(queue, value) {
	this.n_value = value;
	this.n_next = null;
	this.n_prev = null;
	this.n_queue = queue;
}
QueueNode.prototype.insert = function (prev, next) {
	mod_assert.ok(this.n_next === null);
	mod_assert.ok(this.n_prev === null);
	this.n_next = next;
	this.n_prev = prev;
	prev.n_next = this;
	next.n_prev = this;
	this.n_queue.q_len++;
};
QueueNode.prototype.remove = function () {
	mod_assert.ok(this.n_value !== null);
	var next = this.n_next;
	var prev = this.n_prev;
	this.n_next = null;
	this.n_prev = null;
	prev.n_next = next;
	next.n_prev = prev;
	this.n_queue.q_len--;
};

function Queue() {
	this.q_head = new QueueNode(this, null);
	this.q_tail = new QueueNode(this, null);
	this.q_head.n_next = this.q_tail;
	this.q_tail.n_prev = this.q_head;
	this.q_len = 0;
}
Queue.prototype.isEmpty = function () {
	return (this.q_head.n_next === this.q_tail);
};
Queue.prototype.peek = function () {
	return (this.q_head.n_value);
};
Queue.prototype.push = function (v) {
	var n = new QueueNode(this, v);
	n.insert(this.q_tail.n_prev, this.q_tail);
	return (n);
};
Queue.prototype.shift = function () {
	var n = this.q_head.n_next;
	mod_assert.ok(n.value !== null);
	n.remove();
	return (n.n_value);
};
Queue.prototype.forEach = function (cb) {
	var n = this.q_head.n_next;
	while (n !== this.q_tail) {
		var next = n.n_next;
		cb(n.n_value, n);
		n = next;
	}
};
Object.defineProperty(Queue.prototype, 'length', {
	get: function () {
		return (this.q_len);
	}
});

function ConnectionFSM(options) {
	mod_assert.object(options, 'options');
	mod_assert.object(options.pool, 'options.pool');
	mod_assert.func(options.constructor, 'options.constructor');
	mod_assert.object(options.backend, 'options.backend');
	mod_assert.array(options.arguments, 'options.arguments');
	mod_assert.object(options.log, 'options.log');

	this.cf_pool = options.pool;
	this.cf_constructor = options.constructor;
	this.cf_backend = options.backend;
	this.cf_args = options.arguments.slice();
	this.cf_args.push(options.backend);
	this.cf_claimed = false;
	this.cf_claimStack = [];
	this.cf_releaseStack = [];
	this.cf_lastError = undefined;
	this.cf_conn = undefined;
	this.cf_shadow = undefined;
	this.cf_closeAfter = false;
	this.cf_oldListeners = {};
	this.cf_log = options.log.child({backend: this.cf_backend.key});

	mod_assert.optionalNumber(options.retries, 'options.retries');
	mod_assert.optionalNumber(options.delay, 'options.delay');
	mod_assert.optionalNumber(options.maxDelay, 'options.maxDelay');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	mod_assert.optionalNumber(options.maxTimeout, 'options.maxTimeout');

	this.cf_retries = options.retries || 10;
	this.cf_retriesLeft = this.cf_retries;
	this.cf_minDelay = options.delay || 100;
	this.cf_delay = this.cf_minDelay;
	this.cf_maxDelay = options.maxDelay || 10000;
	this.cf_timeout = options.timeout || 2000;
	this.cf_maxTimeout = options.maxTimeout || 60000;

	FSM.call(this, 'init');
}
mod_util.inherits(ConnectionFSM, FSM);

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
	    map(function (l) { return (l.replace(/^[ ]*at /,'')); });
	this.cf_claimed = true;
	if (cb) {
		var self = this;
		this.onState('busy', function () {
			cb(null, self.cf_shadow, self.cf_conn);
		});
	}
	this.emit('claimAsserted');
};

ConnectionFSM.prototype.release = function (cb) {
	mod_assert.ok(this.cf_claimed === true);
	mod_assert.strictEqual(this.getState(), 'busy');

	var e = {};
	Error.captureStackTrace(e);
	this.cf_releaseStack = e.stack.split('\n').slice(1).
	    map(function (l) { return (l.replace(/^[ ]*at /,'')); });

	if (cb)
		this.onState('idle', cb);
	this.emit('releaseAsserted');
};

ConnectionFSM.prototype.close = function (cb) {
	if (cb)
		this.onState('closed', cb);
	this.emit('closeAsserted');
};

ConnectionFSM.prototype.closeAfterRelease = function () {
	this.cf_closeAfter = true;
};

ConnectionFSM.prototype.state_init = function (on, once, timeout) {
	var self = this;
	timeout(this.cf_timeout, function () {
		self.cf_lastError = new Error('Connection timed out');
		self.gotoState('error');
	});
	this.cf_conn = this.cf_constructor.apply(null, this.cf_args);
	once(this.cf_conn, 'connect', function () {
		self.gotoState('idle');
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new Error('Connection unexpectedly closed');
		self.gotoState('error');
	});
	once(this.cf_conn, 'timeout', function () {
		self.cf_lastError = new Error('Connection timed out');
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
	this.cf_log.trace('closed');
};

ConnectionFSM.prototype.state_error = function (on, once, timeout) {
	if (this.cf_conn && this.cf_conn.destroy)
		this.cf_conn.destroy();
	this.cf_conn = undefined;

	if (this.cf_retries === Infinity || --this.cf_retriesLeft > 0) {
		this.gotoState('delay');
	} else {
		this.emit('error', this.cf_lastError);
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
		self.gotoState('init');
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
	this.cf_retriesLeft = this.cf_retries;
	this.cf_delay = this.cf_minDelay;

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
		self.cf_lastError = new Error('Connection unexpectedly closed');
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.gotoState('closed');
	});
};

ConnectionFSM.prototype.state_busy = function (on, once, timeout) {
	var self = this;
	this.cf_conn.ref();

	this.cf_releaseStack = [];
	this.cf_log.trace('busy, claimed by %s',
	    this.cf_claimStack[1].split(' ')[0]);

	this.cf_oldListeners = {};
	['close', 'error', 'readable', 'data'].forEach(function (evt) {
		var count = self.cf_conn.listeners(evt).filter(
		    function (h) { return (typeof (h) === 'function'); }).
		    length;
		self.cf_oldListeners[evt] = count;
	});

	this.cf_shadow = new ConnectionHandle(this);
	function ConnectionHandle(cf) {
		this.sh_cf = cf;
		this.sh_claimed = true;
		this.sh_releaseStack = [];
	}
	ConnectionHandle.prototype.close = function () {
		mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
		    'this handle, released by ' + this.sh_releaseStack[2]);
		return (this.sh_cf.close.apply(this.sh_cf, arguments));
	};
	ConnectionHandle.prototype.release = function () {
		mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
		    'this handle, released by ' + this.sh_releaseStack[2]);
		return (this.sh_cf.release.apply(this.sh_cf, arguments));
	};
	ConnectionHandle.prototype.closeAfterRelease = function () {
		mod_assert.ok(this.sh_claimed, 'Connection not claimed by ' +
		    'this handle, released by ' + this.sh_releaseStack[2]);
		return (this.sh_cf.closeAfterRelease.apply(this.sh_cf,
		    arguments));
	};

	once(this, 'releaseAsserted', function () {
		self.gotoState('idle');
	});
	once(this.cf_conn, 'error', function (err) {
		self.cf_lastError = err;
		self.gotoState('error');
	});
	once(this.cf_conn, 'close', function () {
		self.cf_lastError = new Error('Connection unexpectedly closed');
		self.gotoState('error');
	});
	once(this, 'closeAsserted', function () {
		self.cf_lastError = new Error('Connection forcefully closed ' +
		    'while in use');
		self.gotoState('error');
	});
};

function CueBallConnectionPool(options) {
	EventEmitter.call(this);
	mod_assert.object(options);

	mod_assert.func(options.constructor, 'options.constructor');
	mod_assert.array(options.arguments, 'options.arguments');

	this.p_constructor = options.constructor;
	this.p_args = options.arguments;

	mod_assert.arrayOfString(options.resolvers, 'options.resolvers');
	mod_assert.string(options.domain, 'options.domain');
	mod_assert.optionalString(options.service, 'options.service');
	mod_assert.optionalNumber(options.maxDNSConcurrency,
	    'options.maxDNSConcurrency');
	mod_assert.optionalNumber(options.defaultPort, 'options.defaultPort');

	mod_assert.optionalObject(options.log, 'options.log');
	this.p_log = options.log || mod_bunyan.createLogger({
		name: 'CueBallConnectionPool'
	});
	this.p_log = this.p_log.child({domain: this.p_domain});

	var self = this;
	this.p_resolver = new mod_resolver.Resolver({
		resolvers: options.resolvers,
		domain: options.domain,
		service: options.service,
		maxDNSConcurrency: options.maxDNSConcurrency,
		defaultPort: options.defaultPort,
		log: this.p_log
	});
	this.p_resolver.on('added', function (k, backend) {
		backend.key = k;
		self.p_keys.push(k);
		mod_utils.shuffle(self.p_keys);
		self.p_backends[k] = backend;
		self.rebalance();
	});
	this.p_resolver.on('removed', function (k) {
		var idx = this.p_keys.indexOf(k);
		this.p_keys.splice(idx, 1);
		delete (this.p_backends[k]);
		this.p_connections[k].forEach(function (fsm) {
			if (fsm.getState() !== 'busy')
				fsm.close();
		});
		delete (this.p_connections[k]);
		self.rebalance();
	});
	this.p_rebalTimer = setInterval(function () {
		self.rebalance();
	}, 10000);
	this.p_rebalTimer.unref();

	mod_assert.optionalNumber(options.spares, 'options.spares');
	this.p_spares = options.spares || options.initial || 4;
	mod_assert.optionalNumber(options.maximum, 'options.maximum');
	this.p_max = options.maximum || 16;

	this.p_keys = [];
	this.p_backends = {};
	this.p_connections = {};

	this.p_idleq = new Queue();
	this.p_initq = new Queue();
	this.p_waiters = new Queue();

	this.p_resolver.start();
}
mod_util.inherits(CueBallConnectionPool, EventEmitter);

CueBallConnectionPool.prototype.rebalance = function () {
	var self = this;

	var spares = {};
	var total = 0;
	this.p_keys.forEach(function (k) {
		spares[k] = [];
	});
	this.p_idleq.forEach(function (fsm) {
		++total;
		spares[fsm.cf_backend.key].push(fsm);
	});
	this.p_initq.forEach(function (fsm) {
		++total;
		spares[fsm.cf_backend.key].push(fsm);
	});

	var plan = mod_utils.planRebalance(spares, total, this.p_spares);
	plan.remove.forEach(function (fsm) {
		fsm.close();
	});
	plan.add.forEach(function (k) {
		self.addConnection(k);
	});
};

CueBallConnectionPool.prototype.addExtra = function () {
	var self = this;

	var spares = {};
	var total = this.p_idleq.length + this.p_initq.length;

	if (this.p_keys.length > 0 && total + 1 < this.p_max) {
		this.p_keys.forEach(function (k) {
			spares[k] = [];
		});
		this.p_idleq.forEach(function (fsm) {
			spares[fsm.cf_backend.key].push(fsm);
		});
		this.p_initq.forEach(function (fsm) {
			spares[fsm.cf_backend.key].push(fsm);
		});
		var plan = mod_utils.planRebalance(spares, total, total + 1);
		this.p_log.trace('adding extras to the pool due to load ' + 
		    'spike: %j', plan.add);
		plan.add.forEach(function (k) {
			self.addConnection(k);
		});
	}
};

CueBallConnectionPool.prototype.addConnection = function (key) {
	var backend = this.p_backends[key];

	var fsm = new ConnectionFSM({
		constructor: this.p_constructor,
		arguments: this.p_args,
		backend: backend,
		log: this.p_log,
		pool: this
	});
	if (this.p_connections[key] === undefined)
		this.p_connections[key] = [];
	this.p_connections[key].push(fsm);

	fsm.p_initq_node = this.p_initq.push(fsm);

	var self = this;
	fsm.on('stateChanged', function (newState) {
		if (fsm.p_initq_node) {
			fsm.p_initq_node.remove();
			delete (fsm.p_initq_node);
		}

		if (newState === 'idle') {
			if (self.p_backends[key] === undefined) {
				fsm.close();
				return;
			}

			if (self.p_waiters.length > 0) {
				var cb = self.p_waiters.shift();
				fsm.claim(cb.stack, cb);
				return;
			}
			var node = self.p_idleq.push(fsm);
			fsm.p_idleq_node = node;

		} else if (fsm.p_idleq_node) {
			fsm.p_idleq_node.remove();
			delete (fsm.p_idleq_node);

			self.rebalance();
		}
	});
}

CueBallConnectionPool.prototype.claim = function (options, cb) {
	if (typeof (options) === 'function' && cb === undefined) {
		cb = options;
		options = {};
	}
	mod_assert.object(options, 'options');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	var timeout = options.timeout || 5000;

	var e = {};
	Error.captureStackTrace(e);

	if (this.p_idleq.length > 0) {
		var fsm = this.p_idleq.shift();
		delete (fsm.p_idleq_node);
		fsm.claim(e.stack, cb);
		return;
	}

	var timer;
	var waiter = function () {
		clearTimeout(timer);
		timer = undefined;
		cb.apply(this, arguments);
	};
	waiter.stack = e.stack;
	var qnode = this.p_waiters.push(waiter);
	timer = setTimeout(function () {
		if (timer === undefined)
			return;

		qnode.remove();
		cb(new Error('Timed out while waiting for connection in ' +
		    'pool "' + this.p_domain + '"'));
	}, timeout);
	this.addExtra();
};
