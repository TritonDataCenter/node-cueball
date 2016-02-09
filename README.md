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
    agent: new mod_cueball.HttpsAgent({ spares: 4, maximum: 10 })
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
  - `resolvers` -- optional Array of String, either containing IP addresses to
    use as nameservers, or a single string for Dynamic Resolver mode
  - `log` -- optional Object, a `bunyan`-style logger to use
  - `spares` -- optional Number, number of spares wanted in the pool per host
  - `maximum` -- optional Number, maximum number of connections per host

## Pool

### `new mod_cueball.ConnectionPool(options)`

Creates a new pool of connections.

Parameters

- `options` -- Object, with keys:
  - `constructor` -- Function(backend) -> object, must open a new connection 
    to the given backend and return it
  - `domain` -- String, name to look up to find backends
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

### `ConnectionPool#stop()`

Stops the connection pool and its `Resolver`, then destroys all connections.

### `ConnectionPool#claim([options, ]callback)`

Claims a connection from the pool ready for use.

Parameters

- `options` -- optional Object, with keys:
  - `timeout` -- optional Number, timeout for request in ms
    (default `Infinity`)
- `callback` -- Function(err[, handle, connection]), parameters:
  - `err` -- an Error object, if the request could not be fulfilled or timed
    out
  - `handle` -- Object, handle to be used to release the connection back to 
    the pool when work is complete
  - `connection` -- Object, the actual connection (as returned by the 
    `constructor` given to `new ConnectionPool()`)

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
  - `service` -- optional String, name of SRV service (e.g. `_http._tcp`)
  - `defaultPort` -- optional Number, port to use for plain A/AAAA records
  - `resolvers` -- optional Array of String, either containing IP addresses to
    use as nameservers, or a single string for Dynamic Resolver mode (default 
    uses system resolvers from `/etc/resolv.conf`)
  - `log` -- optional Object, a `bunyan`-style logger to use
  - `timeout` -- optional Number, timeout for DNS queries in ms (default 1000)
  - `delay` -- optional Number, base delay between failed queries in ms 
    (default 100)
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

## Kang support

### `mod_cueball.poolMonitor.toKangOptions()`

Returns an options object that can be passed to `mod_kang.knStartServer`. The
kang options set up snapshots containing a list of all `Pool` objects in the
system and their associated backends and state.

The returned object is missing the `port` property, which should be added
before using.

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
    resolvers: ['binder.coal.joyent.us']
    url: 'http://napi.coal.joyent.us',
    agent: new mod_cueball.HttpAgent({ spares: 4, maximum: 10 })
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
