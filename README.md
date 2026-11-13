# hyperreplicator

Replicates many groups of cores across many topics using pooled resources

## Usage

#### Autoswarming

Automatic pooling by application context

```js
const b4a = require('b4a')
const Corestore = require('corestore')
const Replicator = require('hyperreplicator)

const store = new Corestore()
const keyPair = await store.createKeyPair('hyperreplicator')

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

const replicator = new Replicator(onreplicate, whoHas, { keyPair })

for (const name in groups) {
  replicator.join(toTopic(name))

  // auto-prio on group wakeup
  const handle = store.notifyGroup(b4a.alloc(32, name))

  handle.on('update', () => {
    replicator.bump(toTopic(name), Date.now())
  })

  replicator.on('close', () => handle.destroy())
}

// local writes re-prioritizes replication
await groups.A[0].append('Hello')

function onreplicate(stream) {
  store.replicate(stream)
}

function whoHas(coreDiscoveryKey) {
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
replicator.join(topic)
replicator.leave(topic)

// update priority
replicator.bump(topic, priority, dhtBumpValue)

// force replication
replicator.pin(topic)
replicator.unpin(topic)

// unpool
replicator.leave(topic)

// Lookup bump value
const dhtBumpValue = await replicator.lookup(topic)

// batch apply changes
replicator.refresh()

// wait for known work to complete
await replicator.flush()
```

#### Scanning

A scan does not announce on topic, \
contrary to `lookup` operation - `scan` discovers _and connects_ to peers.
It completes when scan window (`timeout`) ends.

```js
const op = replicator.scan(topic, { delay: 3000, timeout: 60000 })

op.connected.then(() => {
  op.expire(10000) // reduce & linger upto 10s
})

op.complete.then(() => {
  console.log('scan stopped & removed')
})
```

## API

### `new Replicator(onreplicate, whoHas, opts)`

`onreplicate(stream)` forward the stream to corestore

`whoHas(discoveryKey)` return the topic owning an open core, or `null` when unknown or closed.

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
  pooling: true // enable topic rotation
}
```

### Events

#### `rep.on('join', (session) => {})`

Fired when topic is added to pool

#### `rep.on('leave, (session) => {})`

Fired when topic is unannounced and removed from pool

#### `rep.on('activate', (session) => {})`

Fired when a topic becomes active.

#### `rep.on('deactivate', (session) => {})`

Fired when a topic becomes inactive a.k.a idle.

#### `rep.on('evicted', (count) => {})`

Fired after GC evicts idle topics. `count` is the number removed.

Can be used as signal to add more sessions through `join()`

```
for (const [topic, session] of rep.sessions) {
  session.priority = appTimestampOf(topic)
}
```

#### `rep.on('dropped', (count) => {})`

Fired after GC drops connections. `count` is the number dropped.

### Getters

#### `rep.pooling`

Enable|disable topic rotation pool, is writable and applied on `refresh()`

#### `rep.connectionCount`

Total amount of active connections.

#### `rep.connecting`

Amount of outbound connections in-flight.

#### `rep.collected`

Renewed after each `refresh()` and resolves on GC flush

#### `rep.connections`

Returns iterator over all established connections.

#### `rep.stats`

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

#### `rep.activeSessions`

Returns list of active sessions
A session is an internal representation of topic.

#### `rep.announcedSessions`

Returns list of currently announced sessions

### Functions

#### `await rep.ready()`

Resolves after dht.server port is bound

#### `const rs = rep.join(topic, { pin: false, delay: 5000, priority: 0 })`

Join a topic announcing presence.
Joined topics participate in autoswarm pool.

`opts`:

- `priority`: pool priority, high number equals higher priority.
- `pin`: same as `session.pin()`
- `delay`: delay before autoswarm activation

#### `await rep.leave(topic)`

Remove a joined topic from the pool

#### `rep.bump(topic, priority, bump = -1)`

Update a topic's pool priority, higher number means higher priority.

The `bump` value if postive is included in dht-announcement and queryable
using `lookup` or `scan`

#### `const bump = await rep.lookup(topic)`

Query the DHT bump value for a topic without joining it or connecting to peers.

#### `rep.pin(topic)`

Pin a joined topic to keep it active bypassing `priority`

#### `rep.unpin(topic)`

Unpins a topic and returns it to the pool

#### `const op = rep.scan(topic, { limit: 1, delay: 3000, timeout: 60000, force: false })`

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

#### `rep.refresh()`

Rotates pool, recalculates dialer work and applies new state then finally runs garbage-collector.

#### `await rep.gc()`

Schedules a garbage-collection pass on the next event-loop turn and resolves when it completes.
Calls made while a pass is pending return the same promise. Resolves immediately during teardown.

#### `await rep.flush()`

Wait until current scan and dial, and announcement work settles.

#### `rep.presuspend()`

Begins unannouncing all topics - completion can be awaited with immediate call to `flush()`.

No topics will be announced when replicator is in `presuspended` state,
use `resume()` to exit presuspension.

#### `await rep.suspend()` / `await rep.resume()`

Suspend and resume session, server, and connection activity.

#### `await rep.close()`

Close the replicator and all joined sessions.

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
