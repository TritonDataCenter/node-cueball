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
  - `tcpKeepAliveInitialDelay` -- optional Number, if supplied, enable TCP
    level keep-alives with the given initial delay (in milliseconds)
  - `ping` -- optional String, URL path to use for health checking. Connection
    is considered still viable if this URL returns a non-5xx response code.
  - `pingInterval` -- optional Number, interval between health check pings
  - `errorOnEmpty` -- optional Boolean

## Pool

### `new mod_cueball.ConnectionPool(options)`

Creates a new pool of connections.  There are two ways of using a
ConnectionPool.  You can either provide your own resolver directly, or provide
parameters with which to create the default, DNS-based resolver.

Parameters

- `options` -- Object, with keys:
  - `constructor` -- Function(backend) -> object, must open a new connection
    to the given backend and return it
  - `domain` -- String, name to look up to find backends.
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
  - `resolver` -- optional instance of an object meeting the Resolver interface
    below.  You would typically obtain this object by either creating your own
    Resolver directly or using the `resolverForIpOrDomain` function.

Do not confuse `resolvers` (the list of IP addresses for the DNS resolvers to
contact) with `resolver` (a custom object meeting the Resolver interface below).

If you want to use a custom resolver, then you must specify the `resolver`
property.  In that case, the `resolvers`, `maxDNSConcurrency`, `defaultPort`,
and `recovery` options are ignored, and the `domain` and `service` properties
are used only for logging.

Otherwise, if want to use the default DNS-based resolver, do not specify the
`resolver` property.  A resolver instance will be created based on the other
configuration properties.

### Pool states

ConnectionPool exposes the `mooremachine` FSM interface, with the following
state graph:

                                                           | (from failed)
                                                .stop()    v
             +--------+   connect ok   +-------+       +--------+
    init --> |starting| +------------> |running| +---> |stopping|
             +--------+                +-------+       +--------+
                 +                      ^     +            +
        resolver |                      |     |            |
          failed |                      |     |            |
              OR |       +------+       |     |            v
         retries +---->  |failed| +-----+     |        +-------+
       exhausted         +------+ connect ok  |        |stopped|
                          +  ^                |        +-------+
                          |  |                |
                   .stop()|  +----------------+
                          |   all retries exhausted

Pools begin their life in the "starting" state. Once they have successfully made
one connection to any backend, they proceed to the "running" state. Otherwise,
if their underlying Resolver enters the "failed" state, or they exhaust their
retry policy attempting to connect to all their backends, they enter the
"failed" state.

A "running" pool can then either be stopped by calling the `.stop()` method, at
which point it enters the "stopping" state and begins tearing down its
connections; or all of its connections become disconnected and it exhausts its
retry policy, in which case it enters the "failed" state.

Failed pools can re-enter the "running" state at any time if they make a
successful connection to a backend and their underlying Resolver is no longer
"failed". A "failed" pool can also have the `.stop()` method called, in which
case it proceeds much as from "running".

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

Returns a "waiter handle", which is an Object having a `cancel()` method. The
`cancel()` method may be called at any time up to when the `callback` is run, to
cancel the request to the pool and relinquish any queue positions held.

When a client is done with a connection, they must call `handle.release()` to
return it to the pool. All event handlers should be disconnected from the
`connection` prior to calling `release()`.

Calling `claim()` on a Pool that is in the "stopping", "stopped" or "failed"
states will result in the callback being called with an error on the next run of
the event loop.

### `ConnectionPool#claimSync()`

Claims a connection from the pool, only if an idle one is immediately
available. Otherwise, throws an Error. Always throws an Error if called on a
Pool that is "stopping", "stopped" or "failed".

Returns an Object with keys:
 - `handle` -- Object, handle to be used to release the connection
 - `connection` -- Object, actual connection

## Resolver

### `mod_cueball.Resolver` interface

An interface for all "resolvers", objects which take in some kind of
configuration (e.g. a DNS name) and track a list of "backends" for that
name. A "backend" is an IP/port pair that describes an endpoint that can
be connected to to reach a given service.

Resolver exposes the `mooremachine` FSM interface, with the following state
graph:

                    .start()          error
            +-------+       +--------+       +------+
    init -> |stopped| +---> |starting| +---> |failed|
            +---+---+       +---+----+       +------+
                ^               |               +
                |               | ok            |
                |               v               |
            +---+----+      +---+---+           |
            |stopping| <--+ |running|  <--------+
            +--------+      +-------+       retry success
                     .stop()

Resolvers begin their life "stopped". When the user calls `.start()`, they
begin the process of resolving the name/configuration they were given into
backends.

If the initial attempt to resolve the name/configuration fails, the Resolver
enters the "failed" state, but continues retrying. If it succeeds, or if any
later retry succeeds, it moves to the "running" state. The reason why the
"failed" state exists is so that commandline tools and other short-lived
processes can make use of it to decide when to "give up" on a name resolution.

Once an attempt has succeeded, the Resolver will begin emitting `added` and
`removed` events (see below) describing the backends that it has found.

In the "running" state, the Resolver continues to monitor the source of its
backends (e.g. in DNS by retrying once the TTL expires) and emitting these
events when changes occur.

Finally, when the `.stop()` method is called, the Resolver transitions to
"stopping", stops monitoring and emitting events, and comes to rest in the
"stopped" state where it started.

### `Resolver#start()`

Starts the resolver's normal operation (by beginning the process of looking up
the names given).

### `Resolver#stop()`

Stops the resolver. No further events will be emitted unless `start()` is
called again.

### `Resolver#getLastError()`

Returns the last error experienced by the Resolver. This is particularly useful
when the Resolver is in the "failed" state, to produce a log message or user
interface text.

### `Resolver#getState()`

Returns the current state of the Resolver as a string (see diagram above).

Inherited from `mod_mooremachine.FSM`.

### `Resolver#onState(state, cb)`

Registers an event handler to run when the Resolver enters the given state.

Inherited from `mod_mooremachine.FSM`.

### Event `Resolver->added(key, backend)`

Emitted when a new backend has been found.

Parameters
 - `key` -- String, a unique key for this backend (will be referenced by any
   subsequent events about this backend)
 - `backend` -- Object, with keys:
   - `name` -- String, the DNS name for this backend
   - `address` -- String, an IPv4 or IPv6 address
   - `port` -- Number

### Event `Resolver->removed(key)`

Emitted when an existing backend has been removed.

Parameters
 - `key` -- String, unique key for this backend

## DNS-based name resolver

### `new mod_cueball.DNSResolver(options)`

Creates a Resolver that looks up a name in DNS. This Resolver prefers SRV
records if they are available, and falls back to A/AAAA records if they cannot
be found.

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

## Static IP resolver

### `new mod_cueball.StaticIpResolver(options)`

Creates a new static IP resolver.  This object matches the Resolver interface
above, but emits a fixed list of IP addresses when started.  This list never
changes.  This is intended for development environments and debugging tools,
where a user may have provided an explicit IP address rather than a DNS name to
contact.  See also: `resolverForIpOrDomain()`.

Parameters

- `options` -- Object, with keys:
  - `backends` -- Array of objects, each having properties:
    - `address` -- String, an IP address to emit as a backend
    - `port` -- Number, a port number for this backend

This object provides the same `start()` and `stop()` methods as the Resolver
class, as well as the same `added` and `removed` events.

## Picking the right resolver

### `resolverForIpOrDomain(options)`

Services that use DNS for service discovery would typically use a DNS-based
resolver.  But in development environments or with debugging tools, it's useful
to be able to point a cueball-using program at an instance located at a specific
IP address and port.  That's what the Static IP resolver is for.

To make this easy for programs that want to support connecting to either
hostnames or IP addresses, this function is provided to take configuration
(expected to come from a user, via an environment variable, command-line
option, or other configuration source), determine whether an IP address or DNS
name was specified, and return either a DNS-based or static resolver.  If the
input appears to be neither a valid IPv4 nor IPv6 address nor DNS name, or the
port number is not valid, then an Error is returned (not thrown).  (If the
input is missing or has the wrong type, an Error object is thrown, since this
is a programmer error.)

Parameters

- `options` -- Object, with keys:
  - `input` -- String, either an IP address or DNS name, with optional port
    suffix
  - `resolverConfig` -- Object, a set of additional properties to pass to
    the resolver constructor.

The `input` string has the form `HOSTNAME[:PORT]`, where the `[:PORT]` suffix is
optional, and `HOSTNAME` may be either an IP address or DNS name.

**Example:** create a resolver that will emit one backend for an instance at IP
127.0.0.1 port 2020:

    var resolver = mod_cueball.resolverForIpOrDomain({
        'input': '127.0.0.1:2020',
        'resolverConfig': {
            'recovery': {
                'default': {
                    'retries': 1,
                    'timeout': 1000,
                    'delay': 1000,
                    'maxDelay': 1000
                }
            }
        }
    })
    /* check whether resolver is an Error */

**Example:** create a resolver that will track instances associated with DNS
name `mydomain.example.com`:

    var resolver = mod_cueball.resolverForIpOrDomain({
        'input': 'mydomain.example.com',
        'resolverConfig': {
            'recovery': {
                'default': {
                    'retries': 1,
                    'timeout': 1000,
                    'delay': 1000,
                    'maxDelay': 1000
                }
            }
        }
    });
    /* check whether resolver is an Error */

In these examples, the `input` string is assumed to come from a user
cueball does the expected thing when given an IP address or DNS name.

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


Tools
-----

The `cbresolve` tool is provided to show how cueball would resolve a given
configuration.  The output format is not committed.  It may change in the
future.

    usage: cbresolve HOSTNAME[:PORT]                # for DNS-based lookup
           cbresolve -S | --static IP[:PORT]...     # for static IPs
    Locate services in DNS using Cueball resolver.

    The following options are available for DNS-based lookups:
    
        -f, --follow                periodically re-resolve and report changes
        -p, --port PORT             default backend port
        -r, --resolvers IP[,IP...]  list of DNS resolvers
        -s, --service SERVICE       "service" name (for SRV)
        -t, --timeout TIMEOUT       timeout for lookups
    
**Example:** resolve DNS name "1.moray.us-east.joyent.us":

    $ cbresolve 1.moray.emy-10.joyent.us
    domain: 1.moray.emy-10.joyent.us
    timeout: 5000 milliseconds
    172.27.10.218       80 lLbminikNKjfy+iwDobYBuod7Hs=
    172.27.10.219       80 iJMaVRehJ2zKfiS55H/lUUFPb9o=

**Example:** resolve IP/port "127.0.0.1:2020".  This is only useful for seeing
how cueball would parse your input:

    $ cbresolve --static 127.0.0.1:2020
    using static IP resolver
    127.0.0.1         2020 xBut/f1D52k1TpDN/miW82qXw6k=

**Example: resolve DNS name "1.moray.us-east.joyent.us" and watch for changes:

    $ cbresolve --follow 1.moray.emy-10.joyent.us
    domain: 1.moray.emy-10.joyent.us
    timeout: 5000 milliseconds
    2016-06-23T00:45:00.312Z added      172.27.10.218:80    (lLbminikNKjfy+iwDobYBuod7Hs=)
    2016-06-23T00:45:00.314Z added      172.27.10.219:80    (iJMaVRehJ2zKfiS55H/lUUFPb9o=)
    2016-06-23T00:49:00.478Z removed    172.27.10.218:80    (lLbminikNKjfy+iwDobYBuod7Hs=)

In this example, one of the DNS entries was removed a few minutes after the
program was started.
