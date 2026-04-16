# Replicator

A `hyperswarm` replacement modeled for `corestore` that allows
resource tracking and garbage collection.

## Usage

```js
const Corestore = require('corestore')
const Replicator = require('replicator')

const store = new Corestore('./storage')

const rep = new Replicator(store)
await rep.ready()

const session = store.session() // or namespace()

const core = session.get({ name: 'example' })
await core.ready()

const topic = Buffer.alloc(32, 0x1)

// ReplicatorSession
const rs = rep.join(session, topic, { active: false, discover: false })

await store.close() // leaves topic
await rep.close()
```

## API

### `new Replicator(store, opts)`

Create a topic replicator for a root corestore.

`opts`:

- `autoswarm`: enable active/discovery session heuristics
- `suspended`: start discovery/server in suspended mode
- `seed`: derive the replicator keypair from a seed
- `keyPair`: provide an explicit keypair (default: store.createKepair)
- `bootstrap`: DHT bootstrap nodes
- `dht`: reuse an existing HyperDHT instance
- `port`: bind the DHT socket to a fixed port
- `minPeers`: lower GC connection watermark
- `maxPeers`: upper GC connection watermark
- `maxParallel`: max parallel outbound dials
- `relayThrough`: relay selector for outbound connections
- `firewall`: filter inbound and outbound peers

### `await rep.ready()`

Resolves after dht.server port is bound and store is ready.

### `const rs = rep.join(storeSession, topic, { active, discover })`

Join topic, returns `ReplicatorSession`.

`opts`:

- `active` Same as `rs.setActive()`
- `discover` Same as `rs.discover()`

### `await rep.refresh()`

TBD

### `await rep.flush()`

Wait until current discovery and dial work settles.

### `await rep.close()`

Close the replicator and all joined sessions.

### `rs.setActive(bool)`

Pin topic into `active` mode; bypasses auto-rotation and
forces session to be active until manually turned off.

### `rs.discover()`

TBD

### `rs.bumpDiscovery()`

TBD

### `rs.setPriority(number)`

TBD

### `await rs.refresh()`

Refresh this topic's discovery state now.

### `await rs.flush()`

Wait until this topic's current dht query settles

### `await rs.close()`

Close this topic session.

## License

Apache-2.0
