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
const mod_errors = require('./errors');
const mod_cset = require('./set');
const mod_utils = require('./utils');

module.exports = {
	HttpAgent: mod_agent.HttpAgent,
	HttpsAgent: mod_agent.HttpsAgent,
	ConnectionPool: mod_pool.ConnectionPool,
	ConnectionSet: mod_cset.ConnectionSet,
	Resolver: mod_resolver.Resolver,
	DNSResolver: mod_resolver.DNSResolver,
	StaticIpResolver: mod_resolver.StaticIpResolver,
	resolverForIpOrDomain: mod_resolver.resolverForIpOrDomain,

	poolMonitor: mod_pmonitor.monitor,
	enableStackTraces: function () {
		mod_utils.stackTracesEnabled.ENABLED = true;
	},

	ClaimTimeoutError: mod_errors.ClaimTimeoutError,
	NoBackendsError: mod_errors.NoBackendsError,
	ConnectionTimeoutError: mod_errors.ConnectionTimeoutError,
	ConnectionClosedError: mod_errors.ConnectionClosedError,
	PoolStoppingError: mod_errors.PoolStoppingError,
	PoolFailedError: mod_errors.PoolFailedError
};
