# hyperjoin

Replicates groups of hypercores across many topics using pooled resources

## Usage

#### Example

Automatic topic selection by application context.

```js
const b4a = require('b4a')
const Corestore = require('corestore')
const Hyperjoin = require('hyperjoin')

const store = new Corestore()
const keyPair = await store.createKeyPair('hyperjoin')

const groups = {
  A: [
    store.get({ name: 'A1', group: b4a.alloc(32, 'A') }),
    store.get({ name: 'A2', group: b4a.alloc(32, 'A') })
  ],
  B: [
    store.get({ name: 'B1', group: b4a.alloc(32, 'B') }),
    store.get({ name: 'B2', group: b4a.alloc(32, 'B') })
  ]
}

const hyperjoin = new Hyperjoin(onreplicate, topicOf, { keyPair })

for (const name in groups) {
  hyperjoin.join(toTopic(name))

  // auto-prio on group wakeup
  const handle = store.notifyGroup(b4a.alloc(32, name))

  handle.on('update', () => {
    hyperjoin.bump(toTopic(name), Date.now())
  })

  hyperjoin.on('close', () => handle.destroy())
}

// local writes re-prioritizes replication
await groups.A[0].append('Hello')

function onreplicate(stream) {
  store.replicate(stream)
}

function topicOf(coreDiscoveryKey) {
  for (const name in groups) {
    for (const core of groups[name]) {
      if (b4a.equals(coreDiscoveryKey, core.discoveryKey)) {
        return toTopic(name)
      }
    }
  }

  return null
}

function toTopic(input) {
  const { discoveryKey } = require('hypercore-crypto')
  return discoveryKey(b4a.from(input))
}
```

#### Pool control

```js
// add / remove from pool
hyperjoin.join(topic)
hyperjoin.leave(topic)

// update priority
hyperjoin.bump(topic, priority, dhtBumpValue)

// force replication
hyperjoin.pin(topic)
hyperjoin.unpin(topic)

// unpool
hyperjoin.leave(topic)

// Lookup bump value
const dhtBumpValue = await hyperjoin.lookup(topic)

// batch apply changes
hyperjoin.refresh()

// wait for known work to complete
await hyperjoin.flush()
```

#### Scanning

A scan does not announce on topic, \
contrary to `lookup` operation - `scan` discovers _and connects_ to peers.
It completes when scan window (`timeout`) ends.

```js
const op = hyperjoin.scan(topic, { delay: 3000, timeout: 60000 })

op.connected.then(() => {
  op.expire(10000) // reduce & linger upto 10s
})

op.complete.then(() => {
  console.log('scan stopped & removed')
})
```

## API

### `new Hyperjoin(onreplicate, topicOf, opts)`

`onreplicate(stream)` forward the stream to corestore

`topicOf(discoveryKey)` return the topic owning an open core, or `null` when unknown or closed.

Options:

```js
const opts = {
  // initialization

  interval: 5000, // auto-refresh interval
  suspended: false, // start in suspended state

  // Replication

  slots: 8, // max active topics
  maxPeers: 40, // drop connections when above
  minPeers: 20, // minimum connections to keep
  maxParallel: 3, // max parallel outbound dials
  minActiveTime: 30000, // prevent rotation after activation
  gcIdleTime: 30000, // leave topics when idle time exceeded (`evicted` event)

  // Transport

  keyPair, // use keypair
  bootstrap: null, // DHT bootstrap nodes
  nodes: null, // explicit DHT nodes
  dht: null, // external HyperDHT instance
  port: null, // bind the DHT socket to a fixed port
  firewall: () => false, // allow all inbound and outbound peers
  relayThrough: null, // relay selector for outbound connections

  // Debug

  manual: false, // disable auto-refresh interval
  gcDisabled: false, // prevent garbage collection on `refresh()`
  pool: true // enable topic rotation
}
```

### Events

#### `hyperjoin.on('join', (session) => {})`

Fired when topic is added to pool

#### `hyperjoin.on('leave', (session) => {})`

Fired when topic is unannounced and removed from pool

#### `hyperjoin.on('activate', (session) => {})`

Fired when a topic becomes active.

#### `hyperjoin.on('deactivate', (session) => {})`

Fired when a topic becomes inactive a.k.a idle.

#### `hyperjoin.on('evicted', (count) => {})`

Fired after GC evicts idle topics. `count` is the number removed.

Can be used as signal to add more sessions through `join()`

```
for (const [topic, session] of hyperjoin.sessions) {
  session.priority = appTimestampOf(topic)
}
```

#### `hyperjoin.on('dropped', (count) => {})`

Fired after GC drops connections. `count` is the number dropped.

#### `hyperjoin.on('ban', (peer, reason) => {})`

Fired when a peer is banned.

### Getters

#### `hyperjoin.pool`

Enable|disable topic rotation pool, is writable and applied on `refresh()`

#### `hyperjoin.connectionCount`

Total amount of active connections.

#### `hyperjoin.connecting`

Amount of outbound connections in-flight.

#### `hyperjoin.collected`

Renewed after each `refresh()` and resolves on GC flush

#### `hyperjoin.connections`

Returns iterator over all established connections.

#### `hyperjoin.stats`

Connection and DHT activity counters:

```js
{
  updates: 0,
  bannedPeers: 0,
  connects: {
    client: { attempted: 0, opened: 0, closed: 0 },
    server: { opened: 0, closed: 0 }
  },
  dht: { lookup: 0, announce: 0, unannounce: 0 }
}
```

#### `hyperjoin.activeSessions`

Returns list of active sessions
A session is an internal representation of topic.

#### `hyperjoin.announcedSessions`

Returns list of currently announced sessions

### Functions

#### `await hyperjoin.ready()`

Resolves after dht.server port is bound

#### `const rs = hyperjoin.join(topic, { pin: false, delay: 5000, priority: 0 })`

Join a topic announcing presence.
Joined topics participate in autoswarm pool.

`opts`:

- `priority`: pool priority, high number equals higher priority.
- `pin`: same as `session.pin()`
- `delay`: delay before autoswarm activation

#### `await hyperjoin.leave(topic)`

Remove a joined topic from the pool

#### `hyperjoin.bump(topic, priority, bump = -1)`

Update a topic's pool priority, higher number means higher priority.

The `bump` value if postive is included in dht-announcement and queryable
using `lookup` or `scan`

#### `const bump = await hyperjoin.lookup(topic)`

Query the DHT bump value for a topic without joining it or connecting to peers.

#### `hyperjoin.pin(topic)`

Pin a joined topic to keep it active bypassing `priority`

#### `hyperjoin.unpin(topic)`

Unpins a topic and returns it to the pool

#### `const op = hyperjoin.scan(topic, { limit: 1, delay: 3000, timeout: 60000, force: false })`

Scan a topic for peers without announcing.

Promise `op.connected` does not throw and resolves when first peer is connected on the scanned topic or scan removed.
Promise `op.complete` does not throw and resolves when a scan is removed

`op.expire(timeout)` shortens the scan lifetime.

`op.timeout` read-only, current scan window

`opts`:

- `limit`: maximum peers emitted per DHT query
- `delay`: delay before the DHT query
- `timeout`: initial scan lifetime
- `force`: force new DHT query if existing topic scan is scheduled

emits events `scan` on scan start and `scanned` on scan complete

#### `hyperjoin.refresh()`

Rotates pool, recalculates dialer work and applies new state then finally runs garbage-collector.

#### `await hyperjoin.gc()`

Schedules a garbage-collection pass on the next event-loop turn and resolves when it completes.
Calls made while a pass is pending return the same promise. Resolves immediately during teardown.

#### `await hyperjoin.flush()`

Wait until current scan and dial, and announcement work settles.

#### `hyperjoin.presuspend()`

Begins unannouncing all topics - completion can be awaited with immediate call to `flush()`.

No topics will be announced when hyperjoin is in `presuspended` state,
use `resume()` to exit presuspension.

#### `await hyperjoin.suspend()` / `await hyperjoin.resume()`

Suspend and resume session, server, and connection activity.

#### `await hyperjoin.close()`

Close the hyperjoin and all joined sessions.

<details>
<summary>Sessions are private since `2.x` - docs kept for reference only</summary>

### ReplicatorSession

#### `rs.on('state', (next, previous) => {})`

Fired when a session changes state.

States:

- `inactive`
- `pinned`
- `autostarted`
- `scanning`
- `destroyed`

#### `rs.on('activate', (session) => {})`

Fired when the session becomes active.

#### `rs.on('deactivate', (session) => {})`

Fired when the session becomes inactive a.k.a idle.

#### `rs.on('bump', (version) => {})`

Fired when a remote discovery bump is observed.

### `rs.pin()` / `rs.unpin()`

Pin topic to announce itself on dht - bypasses `priority`, auto-rotation and
forces the session to be active until manual call to `unpin()`.

### `rs.bump(version = -1)`

Announce local version on topic.

### `rs.priority = number`

Priority used by autoswarm auto-selection.

### `await rs.flush()`

Wait until this topic's current dht query settles

</details>

## License

Apache-2.0
