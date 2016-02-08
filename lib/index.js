/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */
 
const mod_agent = require('./agent');
const mod_pool = require('./pool');
const mod_resolver = require('./resolver');
const mod_pmonitor = require('./pool-monitor');

module.exports = {
	Agent: mod_agent.Agent,
	ConnectionPool: mod_pool.ConnectionPool,
	Resolver: mod_resolver.Resolver,
	poolMonitor: mod_pmonitor.monitor
};
