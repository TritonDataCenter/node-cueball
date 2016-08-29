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
const mod_os = require('os');

function CueBallPoolMonitor() {
	this.pm_pools = {};
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

CueBallPoolMonitor.prototype.toKangOptions = function () {
	var self = this;

	function listTypes() {
		return (['pool']);
	}

	function listObjects(type) {
		mod_assert.strictEqual(type, 'pool');
		return (Object.keys(self.pm_pools));
	}

	function get(type, id) {
		mod_assert.strictEqual(type, 'pool');
		var pool = self.pm_pools[id];
		mod_assert.object(pool);

		var obj = {};
		obj.backends = pool.p_backends;
		obj.connections = {};
		Object.keys(pool.p_connections).forEach(function (k) {
			obj.connections[k] = pool.p_connections[k].length;
		});
		obj.last_rebalance = Math.round(
		    pool.p_lastRebalance.getTime() / 1000);
		obj.resolvers = pool.p_resolver.r_resolvers;
		obj.state = pool.getState();
		obj.options = {};
		obj.options.domain = pool.p_resolver.r_domain;
		obj.options.service = pool.p_resolver.r_service;
		obj.options.defaultPort = pool.p_resolver.r_defport;
		obj.options.spares = pool.p_spares;
		obj.options.maximum = pool.p_max;
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
