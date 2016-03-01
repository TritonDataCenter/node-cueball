cueball
=======

`cueball` is a node.js library for "playing pool" -- managing a pool of
connections to a multi-node service where nodes are listed in DNS.

It supports DNS SRV record style services, as well as the simpler kind with
multiple A/AAAA records under the same name, correctly respecting record
TTLs and making use of Additional sections where possible.

The library also includes an HTTP `Agent` which can be used with the regular
node `http` client stack, including libraries like `restify` which layer on
top of it. The `Agent` transparently creates pools for services as you make
requests to connect to them.

Install
-------

```
npm install cueball
```

Example
-------

Using the `HttpsAgent`:

```js
const mod_cueball = require('cueball');
const mod_restify = require('restify-clients');

var client = mod_restify.createStringClient({
    url: 'https://us-east.manta.joyent.com',
    agent: new mod_cueball.HttpsAgent({
        spares: 4, maximum: 10,
        recovery: {
            default: {
                timeout: 2000,
                retries: 5,
                delay: 250,
                maxDelay: 1000
            }
        }
    })
});

client.get('/foobar/public', function (err, req, res, data) {
    ...
});
```

This will create a connection pool that aims to keep 4 spare connections at
all times, up to a maximum of 10 total connections, for
`us-east.manta.joyent.com`.

It will respect DNS TTLs and automatically rebalance the pool as IP addresses
are added to or removed from DNS.

API
---

## Agent

### `new mod_cueball.HttpAgent(options)`
### `new mod_cueball.HttpsAgent(options)`

Creates an HTTP(S) agent that can be used with the node `http` client API.

Parameters

- `options` -- Object, with keys:
  - `recovery` -- Object, a recovery spec (see below)
  - `resolvers` -- optional Array of String, either containing IP addresses to
    use as nameservers, or a single string for Dynamic Resolver mode
  - `log` -- optional Object, a `bunyan`-style logger to use
  - `spares` -- optional Number, number of spares wanted in the pool per host
  - `maximum` -- optional Number, maximum number of connections per host
  - `ping` -- optional String, URL path to use for health checking. Connection
    is considered still viable if this URL returns a non-5xx response code.
  - `pingInterval` -- optional Number, interval between health check pings
  - `errorOnEmpty` -- optional Boolean, 

## Pool

### `new mod_cueball.ConnectionPool(options)`

Creates a new pool of connections.

Parameters

- `options` -- Object, with keys:
  - `constructor` -- Function(backend) -> object, must open a new connection 
    to the given backend and return it
  - `domain` -- String, name to look up to find backends
  - `recovery` -- Object, a recovery spec (see below)
  - `service` -- optional String, name of SRV service (e.g. `_http._tcp`)
  - `defaultPort` -- optional Number, port to use for plain A/AAAA records
  - `resolvers` -- optional Array of String, either containing IP addresses to
    use as nameservers, or a single string for Dynamic Resolver mode (default 
    uses system resolvers from `/etc/resolv.conf`)
  - `log` -- optional Object, a `bunyan`-style logger to use
  - `spares` -- optional Number, number of spares wanted in the pool per host
  - `maximum` -- optional Number, maximum number of connections per host
  - `maxDNSConcurrency` -- optional Number, max number of DNS queries to issue 
    at once (default 5)
  - `checkTimeout` -- optional Number, milliseconds of idle time before
    running `checker` on a connection
  - `checker` -- optional Function(handle, connection), to be run on idle
    connections

### `ConnectionPool#stop()`

Stops the connection pool and its `Resolver`, then destroys all connections.

### `ConnectionPool#claim([options, ]callback)`

Claims a connection from the pool ready for use.

Parameters

- `options` -- optional Object, with keys:
  - `timeout` -- optional Number, timeout for request in ms
    (default `Infinity`)
  - `errorOnEmpty` -- optional Boolean, if true return error straight away
    if the pool has no backends at all (i.e., nothing was found in DNS)
- `callback` -- Function(err[, handle, connection]), parameters:
  - `err` -- an Error object, if the request could not be fulfilled or timed
    out
  - `handle` -- Object, handle to be used to release the connection back to 
    the pool when work is complete
  - `connection` -- Object, the actual connection (as returned by the 
    `constructor` given to `new ConnectionPool()`)

Returns either `undefined` (if the callback was called immediately), or a
"waiter handle", which is an Object having a `cancel()` method. The `cancel()`
method may be called at any time up to when the `callback` is run, to cancel
the request to the pool and relinquish any queue positions held.

When a client is done with a connection, they must call `handle.release()` to
return it to the pool. All event handlers should be disconnected from the
`connection` prior to calling `release()`.

### `ConnectionPool#claimSync()`

Claims a connection from the pool, only if an idle one is immediately
available. Otherwise, throws an Error.

Returns an Object with keys:
 - `handle` -- Object, handle to be used to release the connection
 - `connection` -- Object, actual connection

## Resolver

### `new mod_cueball.Resolver(options)`

Creates a "resolver" -- an object which tracks a given service in DNS and
emits events when backends are added or removed.

Parameters

- `options` -- Object, with keys:
  - `domain` -- String, name to look up to find backends
  - `recovery` -- Object, a recovery spec (see below)
  - `service` -- optional String, name of SRV service (e.g. `_http._tcp`)
  - `defaultPort` -- optional Number, port to use for plain A/AAAA records
  - `resolvers` -- optional Array of String, either containing IP addresses to
    use as nameservers, or a single string for Dynamic Resolver mode (default 
    uses system resolvers from `/etc/resolv.conf`)
  - `log` -- optional Object, a `bunyan`-style logger to use
  - `maxDNSConcurrency` -- optional Number, max number of DNS queries to issue 
    at once (default 5)

### `Resolver#start()`

Starts the resolver's normal operation (by beginning the process of looking up
the names given).

### `Resolver#stop()`

Stops the resolver. No further events will be emitted unless `start()` is
called again.

### Event `Resolver->added(key, backend)`

Emitted when a new backend has been found in DNS.

Parameters
 - `key` -- String, a unique key for this backend (will be referenced by any
   subsequent events about this backend)
 - `backend` -- Object, with keys:
   - `name` -- String, the DNS name for this backend
   - `address` -- String, an IPv4 or IPv6 address
   - `port` -- Number

### Event `Resolver->removed(key)`

Emitted when an existing backend has been removed from DNS.

Parameters
 - `key` -- String, unique key for this backend

## Errors

### `ClaimTimeoutError`

Passed as first argument to `ConnectionPool#claim()`'s callback when the given
timeout in `options` has been exceeded.

Properties
 - `pool` -- ConnectionPool

### `NoBackendsError`

Passed as first argument to `ConnectionPool#claim()`'s callback when there are
no known backends for the pool and the `errorOnEmpty` flag is set.

Properties
 - `pool` -- ConnectionPool

## Kang support

### `mod_cueball.poolMonitor.toKangOptions()`

Returns an options object that can be passed to `mod_kang.knStartServer`. The
kang options set up snapshots containing a list of all `Pool` objects in the
system and their associated backends and state.

The returned object is missing the `port` property, which should be added
before using.

## Recovery objects

To specify the retry and timeout behaviour of Cueball DNS and pooled 
connections, the "recovery spec object" is a required argument to most
constructors in the API.

A recovery spec object should always have at least one key, named `"default"`,
which gives the default settings for any operation.

More specific per-operation settings can also be given as additional keys.

For example:

```js
{
  default: {
    timeout: 2000,
    retries: 3,
    delay: 100
  },
  dns: {
    timeout: 5000,
    retries: 3,
    delay: 200
  }
}
```

This specifies that DNS-related operations should have a timeout of 5 seconds,
3 retries, and initial delay of 200ms, while all other operations (e.g.
`connect()` while connecting to a new backend) should have a timeout of 2
seconds, 3 retries and initial delay of 100ms.

The `delay` field indicates a time to wait between retry attempts. After each 
failure, it will be doubled until it exceeds the value of `maxDelay`.

The possible fields in one operation are:
 - `retries` finite Number >= 0, number of retry attempts
 - `timeout` finite Number > 0, milliseconds to wait before declaring an 
   attempt a failure
 - `maxTimeout` Number > `timeout` (can be `Infinity`), maximum value of
   `timeout` to be reached with exponential timeout increase
 - `delay` finite Number >= 0, milliseconds to delay between retry attempts
 - `maxDelay` Number > `delay` (can be `Infinity`), maximum value of `delay`
   to be reached with exponential delay increase

And the available operations:
 - `dns` (all DNS-related operations, lookups etc)
 - `dns_srv` (specifically lookups on SRV records, this is separate in case you
   need to deal with certain old buggy DNS servers that have trouble with SRV)
 - `connect` (connections to backends in a `ConnectionPool`)
 - `initial` (the very first attempt to connect to a new backend, will fall
   back to `connect` if not given)

If a given operation has no specification given, it will use `default` instead.

Dynamic Resolver mode
---------------------

`Resolver` instances can operate in a so-called "Dynamic Resolver" mode, where
as well as tracking their particular target service in DNS, they also track the
correct nameservers to ask about it.

This is useful in systems where the nameservers are listed in DNS as a service
just like your ordinary target service (e.g. HTTP). An example is the Joyent
SDC `binder`. `binder` acts as a DNS server, listing addresses of all SDC
service instances. This includes listing its own address, and if multiple
`binder`s are deployed, all other `binder`s in the DC.

We can look up the list of currently available `binder` instances in DNS, and
use this to perform our name resolution. We can also then use the `binder`s to
update our original list of `binder` instances.

This mode requires a "bootstrap" to begin with, however -- we cannot resolve
the name that the `binder` instances are listed under until we already know the
address of one of the `binder`s. In Dynamic Resolver mode, `cueball` will
bootstrap using the system resolvers from `/etc/resolv.conf`.

### Example

```js
const mod_cueball = require('cueball');
const mod_restify = require('restify-clients');

var client = mod_restify.createJsonClient({
    url: 'http://napi.coal.joyent.us',
    agent: new mod_cueball.HttpAgent({
        resolvers: ['binder.coal.joyent.us'],
        spares: 4, maximum: 8
    })
});

client.get('/networks/' + uuid, function (err, req, res, data) {
    ...
});
```

This example code will start by using the system resolvers to resolve
`binder.coal.joyent.us`. Then, the records found via this lookup will be used
as nameservers to look up `napi.coal.joyent.us`.

When the TTL expires on the records for `binder.coal.joyent.us`, we will use
the records from the previous lookup as the list of nameservers to query in
order to find out what the new records should be. Then, we will use any new
nameservers we find for the next `napi.coal.joyent.us` lookup as well.
