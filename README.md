# Replicator

A `hyperswarm` replacement modeled for `corestore` that allows
resource tracking and garbage collection.

## Usage

```js
const Corestore = require('corestore')
const Replicator = require('hyperjoin')

const store = new Corestore('./storage')

const rep = new Replicator(store, { slots: 8, maxPeers: 40 })
await rep.ready()

const session = store.session() // or namespace()

const core = session.get({ name: 'example' })
await core.ready()

const topic = Buffer.alloc(32, 0x1)

// ReplicatorSession
const rs = rep.join(session, topic, { pin: true })

await rep.refresh() // apply auto-rotation & launch topics

await store.close() // leaves topic
await rep.close()
```

## API

### `new Replicator(store, opts)`

Create a topic replicator for a root corestore.

`opts`:

General

- `autoswarm`: enables active topic rotation (default: `true`)
- `suspended`: start suspended
- `seed`: derive the replicator keypair from a seed
- `maxPeers`: drop connections above
- `minPeers`: minimum connections to keep
- `maxParallel`: max parallel outbound dials
- `slots`: auto-rotation slots
- `gcIdleTime`: Evict idle sessions exceeding time
- `defaultDiscoveryTimeout`: initial discovery lifetime
- `updatedDiscoveryTimeout`: shortened discovery lifetime after `session.bump()`
- `discoveryStartupDelay`: delay before discovery lookups
- `autoStartupDelay`: delay before autoswarm announcements
- `minActiveTime`: minimum time an autoswarmed session stays active
- `manual`: don't launch internal `refresh()` interval (default: `false`)
- `interval`: Auto-refresh interval (default: `5000`)
- `gcDisabled`: prevent garbage collection on `refresh()`

Transport

- `keyPair`: provide an explicit keypair (default: store.createKeypair)
- `bootstrap`: DHT bootstrap nodes
- `nodes`: explicit DHT nodes
- `dht`: reuse an existing HyperDHT instance, _note:_ replicator does not close/suspend/resume borrowed dht instances.
- `port`: bind the DHT socket to a fixed port
- `firewall`: filter inbound and outbound peers
- `relayThrough`: relay selector for outbound connections

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

### Getters

#### `rep.connectionCount`

Total amount of active connections.

#### `rep.connecting`

Amount of outbound connections in-flight.

#### `rep.connections`

Returns iterator over all established connections.

#### `rep.activeSessions`

Returns list of active sessions.

#### `rep.announcedSessions`

Returns list of announced sessions that are required to be un-announced on teardown.

### Methods

### `await rep.ready()`

Resolves after dht.server port is bound and store is ready.

### `const rs = rep.join(storeSession, topic, opts)`

Join topic, returns `ReplicatorSession`.

`opts`:

- `priority`: autoswarm priority `number`
- `pin`: same as `session.pin()`
- `discover`: same as `session.discover()`
- `limit`: maximum peers emitted per discovery query (default: `Infinity`)
- `server`: whether the session can announce on the dht (default: `true`).

### `await rep.refresh()`

Invokes topic-rotation, applies state transitions, then finally runs garbage-collector.

Manual replicators take no actions unless refresh is called.

### `await rep.flush()`

Wait until current discovery and dial work settles.

### `await rep.setAutoswarm(enabled)`

Enable or disable autoswarm topic rotation.
Disabling un-announces all autostarted topics

### `rep.presuspend()`

Begins unannouncing all topics.

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
On pinned sessions, forces a discovery refresh without unpinning.

### `rs.bump(version = -1)`

Announce local version on topic.

If bump is called while in discovery mode, the discovery timeout is reset to
`updatedDiscoveryTimeout` as the discovery operation can be considered 'sucessful'

### `rs.priority = number`

Priority used by autoswarm auto-selection.

### `await rs.flush()`

Wait until this topic's current dht query settles

## License

Apache-2.0
