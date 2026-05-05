# Replicator

A `hyperswarm` replacement modeled for `corestore` that allows
resource tracking and garbage collection.

## Usage

```js
const Corestore = require('corestore')
const Replicator = require('replicator')

const store = new Corestore('./storage')

const rep = new Replicator(store, { slots: 8, maxPeers: 40 })
await rep.ready()

const session = store.session() // or namespace()

const core = session.get({ name: 'example' })
await core.ready()

const topic = Buffer.alloc(32, 0x1)

// ReplicatorSession
const rs = rep.join(session, topic)

await rep.refresh() // apply auto-rotation & launch topics

await store.close() // leaves topic
await rep.close()
```

## API

### `new Replicator(store, opts)`

Create a topic replicator for a root corestore.

`opts`:

General

- `autoswarm`: enables active topic rotation (default `true`)
- `suspended`: start suspended
- `seed`: derive the replicator keypair from a seed
- `maxPeers`: drop connections above
- `minPeers`: minimum connections to keep
- `slots`: auto-rotation slots
- `gcIdleTime`: Evict idle sessions exceeding time
- `defaultDiscoveryTimeout`: initial discovery lifetime
- `updatedDiscoveryTimeout`: shortened discovery lifetime after `session.bump()`
- `manual`: don't launch internal `refresh()` interval.
- `gcDisabled`: prevent garbage collection on `refresh()`

Transport

- `keyPair`: provide an explicit keypair (default: store.createKepair)
- `bootstrap`: DHT bootstrap nodes
- `nodes`: explicit DHT nodes
- `dht`: reuse an existing HyperDHT instance
- `port`: bind the DHT socket to a fixed port
- `firewall`: filter inbound and outbound peers
- `relayThrough`: relay selector for outbound connections

Dialer

- `maxParallel`: max parallel outbound dials
- `maxClientConnections`: max concurrent outbound connections
- `maxServerConnections`: max concurrent inbound connections

### Events

#### `rep.on('activate', (session) => {})`

Fired when a session becomes active.

#### `rep.on('deactivate', (session) => {})`

Fired when a session becomes inactive a.k.a idle.

#### `rep.on('remove', (session) => {})`

Fired when a session is removed from the replicator.

#### `rep.on('evicted', (count) => {})`

Fired after GC evicts idle sessions. `count` is the number removed.

Can be used as signal to add more sessions through `rep.join()`
and update all priorities:

```
for (const [topic, session] of rep.sessions) {
  session.priority = appTimestampOf(topic)
}
```

#### `rep.on('dropped', (count) => {})`

Fired after GC drops connections. `count` is the number dropped.

### `await rep.ready()`

Resolves after dht.server port is bound and store is ready.

### `const rs = rep.join(storeSession, topic)`

Join topic, returns `ReplicatorSession`.

### `await rep.refresh()`

Invokes topic-rotation, applies state transitions, then finally runs garbage-collector.

Replicator takes no actions unless refresh is called.

### `await rep.flush()`

Wait until current discovery and dial work settles.

### `await rep.suspend()` / `await rep.resume()`

Suspend and resume discovery, server, and connection activity.

### `await rep.close()`

Close the replicator and all joined sessions.

### ReplicatorSession

#### `rs.on('state', (next, previous) => {})`

Fired when a session changes state.

States:

- `inactive`
- `pinned`
- `autostarted`
- `discovering`
- `destroyed`

#### `rs.on('activate', (session) => {})`

Fired when the session becomes active.

#### `rs.on('deactivate', (session) => {})`

Fired when the session becomes inactive a.k.a idle.

#### `rs.on('bump', (version) => {})`

Fired when a remote discovery bump is observed.

#### `rs.on('discovered', () => {})`

Fired when the session exits discovery mode.

### `rs.pin()` / `rs.unpin()`

Pin topic to announce itself on dht - bypasses `priority`, auto-rotation and
forces the session to be active until manual call to `unpin()`.

### `rs.discover()`

Launch a dht lookup for the topic to discover peers and update version `seen`. (announce: false / client only)

### `rs.bump(version = -1)`

Announce local version on topic.
If a disovery is in progress, then it will be prolonged with a shorter disovery window.

### `rs.priority = number`

Priority used by autoswarm auto-selection.

### `await rs.flush()`

Wait until this topic's current dht query settles

## License

Apache-2.0
