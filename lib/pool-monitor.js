/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

var monitor = new CueBallPoolMonitor();

module.exports = {
	monitor: monitor
};

const mod_assert = require('assert-plus');
const mod_pool = require('./pool');
const mod_cset = require('./set');
const mod_os = require('os');

function CueBallPoolMonitor() {
	this.pm_pools = {};
	this.pm_sets = {};
}

CueBallPoolMonitor.prototype.registerPool = function (pool) {
	mod_assert.ok(pool instanceof mod_pool.ConnectionPool);
	this.pm_pools[pool.p_uuid] = pool;
};

CueBallPoolMonitor.prototype.unregisterPool = function (pool) {
	mod_assert.ok(pool instanceof mod_pool.ConnectionPool);
	mod_assert.ok(this.pm_pools[pool.p_uuid]);
	delete (this.pm_pools[pool.p_uuid]);
};

CueBallPoolMonitor.prototype.registerSet = function (set) {
	mod_assert.ok(set instanceof mod_cset.ConnectionSet);
	this.pm_sets[set.cs_uuid] = set;
};

CueBallPoolMonitor.prototype.unregisterSet = function (set) {
	mod_assert.ok(set instanceof mod_cset.ConnectionSet);
	mod_assert.ok(this.pm_sets[set.cs_uuid]);
	delete (this.pm_sets[set.cs_uuid]);
};

CueBallPoolMonitor.prototype.toKangOptions = function () {
	var self = this;

	function listTypes() {
		return (['pool', 'set']);
	}

	function listObjects(type) {
		if (type === 'pool') {
			return (Object.keys(self.pm_pools));
		} else if (type === 'set') {
			return (Object.keys(self.pm_sets));
		} else {
			throw (new Error('Invalid type "' + type + '"'));
		}
	}

	function get(type, id) {
		if (type === 'pool') {
			return (getPool(id));
		} else if (type === 'set') {
			return (getSet(id));
		} else {
			throw (new Error('Invalid type "' + type + '"'));
		}
	}

	function getPool(id) {
		var pool = self.pm_pools[id];
		mod_assert.object(pool);

		var obj = {};
		obj.backends = pool.p_backends;
		obj.connections = {};
		var ks = pool.p_keys.slice();
		Object.keys(pool.p_connections).forEach(function (k) {
			if (ks.indexOf(k) === -1)
				ks.push(k);
		});
		ks.forEach(function (k) {
			var conns = pool.p_connections[k] || [];
			obj.connections[k] = {};
			conns.forEach(function (fsm) {
				var s = fsm.getState();
				if (obj.connections[k][s] === undefined)
					obj.connections[k][s] = 0;
				++obj.connections[k][s];
			});
		});
		obj.dead_backends = Object.keys(pool.p_dead);
		if (pool.p_lastRebalance !== undefined) {
			obj.last_rebalance = Math.round(
			    pool.p_lastRebalance.getTime() / 1000);
		}
		obj.resolvers = pool.p_resolver.r_resolvers;
		obj.state = pool.getState();
		obj.counters = pool.p_counters;
		obj.options = {};
		obj.options.domain = pool.p_resolver.r_domain;
		obj.options.service = pool.p_resolver.r_service;
		obj.options.defaultPort = pool.p_resolver.r_defport;
		obj.options.spares = pool.p_spares;
		obj.options.maximum = pool.p_max;
		return (obj);
	}

	function getSet(id) {
		var cset = self.pm_sets[id];
		mod_assert.object(cset);

		var obj = {};
		obj.backends = cset.cs_backends;
		obj.fsms = {};
		obj.connections = Object.keys(cset.cs_connections);
		var ks = cset.cs_keys.slice();
		Object.keys(cset.cs_fsm).forEach(function (k) {
			if (ks.indexOf(k) === -1)
				ks.push(k);
		});
		ks.forEach(function (k) {
			var fsm = cset.cs_fsm[k];
			obj.fsms[k] = {};
			var s = fsm.getState();
			if (obj.fsms[k][s] === undefined)
				obj.fsms[k][s] = 0;
			++obj.fsms[k][s];
		});
		obj.dead_backends = Object.keys(cset.cs_dead);
		if (cset.cs_lastRebalance !== undefined) {
			obj.last_rebalance = Math.round(
			    cset.cs_lastRebalance.getTime() / 1000);
		}
		obj.resolvers = cset.cs_resolver.r_resolvers;
		obj.state = cset.getState();
		obj.counters = cset.cs_counters;
		obj.target = cset.cs_target;
		obj.maximum = cset.cs_max;
		obj.options = {};
		obj.options.domain = cset.cs_resolver.r_domain;
		obj.options.service = cset.cs_resolver.r_service;
		obj.options.defaultPort = cset.cs_resolver.r_defport;
		return (obj);
	}

	function stats() {
		return ({});
	}

	return ({
		uri_base: '/kang',
		service_name: 'cueball',
		version: '1.0.0',
		ident: mod_os.hostname(),
		list_types: listTypes,
		list_objects: listObjects,
		get: get,
		stats: stats
	});
};
