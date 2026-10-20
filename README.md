# Replicator

A `hyperswarm` replacement modeled for `corestore`. Every connection replicates
the root corestore. The API mirrors hyperswarm's. What the replicator adds is
connection management: an idle connection GC driven by real traffic, a
connection cap that drops the right connections, and dial backoff and bans
that never punish a peer for disconnects we caused.

## Usage

```js
const Corestore = require('corestore')
const Replicator = require('replicator')

const store = new Corestore('./storage')

const rep = new Replicator(store, { maxPeers: 40 })
await rep.ready()

const session = store.session() // or namespace()

// like hyperswarm: announce the topic and connect to its peers
const discovery = rep.join(session, topic)
await discovery.flushed()

// only look it up, connect to at most one peer
const lookup = rep.join(session, otherTopic, { server: false, client: true, limit: 1 })

// leave
await lookup.destroy()
await discovery.destroy()

await session.close() // also leaves every topic joined with it

await rep.close()
```

## API

### `new Replicator(store, opts)`

`opts`:

General

- `suspended`: start suspended
- `seed`: derive the replicator keypair from a seed
- `maxPeers`: connection cap
- `minPeers`: minimum connections to keep
- `maxParallel`: max parallel outbound dials
- `connectionIdleTimeout`: a connection without protocol traffic for this long is idle (default: 3 minutes, keep-alives do not count)
- `connectionGc`: drop idle connections nobody wants even below `maxPeers` (default: `true`)
- `dhtFastRetry`: retry an empty first lookup after up to this long (default: `3s`)
- `manual`: don't run the internal `refresh()` interval (default: `false`)
- `interval`: auto-refresh interval (default: `5000`)
- `gcDisabled`: never drop connections in `refresh()`

Transport

- `keyPair`: provide an explicit keypair (default: store.createKeypair)
- `bootstrap`: DHT bootstrap nodes
- `nodes`: explicit DHT nodes
- `dht`: reuse an existing HyperDHT instance, _note:_ replicator does not close/suspend/resume borrowed dht instances.
- `port`: bind the DHT socket to a fixed port
- `firewall`: filter inbound and outbound peers
- `relayThrough`: relay selector for outbound connections
- `wakeup`: a `protomux-wakeup` instance, attached to every connection

### `const discovery = rep.join(storeSession, topic, { server = true, client = true, limit = Infinity })`

Join a topic, like hyperswarm. Closing `storeSession` leaves the topic, the
session of the first join of a topic owns it.

- `server`: announce the topic so its peers can find us;
- `client`: look the topic up and connect to its peers;
- `limit`: connect to at most this many of them, counting connected peers.

Every call returns its own handle. A topic combines its joins: it is a server
if any join is, a client if any join is, and its limit is the highest of the
client joins (unlimited if any client join is). The topic is left (and
un-announced) when its last join is destroyed.

- `await discovery.flushed()`: wait for the current announce or lookup
- `await discovery.refresh({ server, client, limit })`: change this join's flags
- `await discovery.destroy()`: leave

### `rep.joinPeer(publicKey)` / `rep.leavePeer(publicKey)`

Stay connected to a peer by public key, without a topic. Refcounted. A joined
peer is never dropped by the connection GC.

### `await rep.background()` / `await rep.foreground()`

In the background nothing is announced, looked up or dialed. Existing
connections stay up and inbound connections are still accepted.
`presuspend()` is a deprecated alias of `background()`.

### `await rep.suspend()` / `await rep.resume()`

Suspend drops every connection and stops the server. `resume()` brings the
server back first, then announces, looks up and dials again. It also leaves
the background.

### `await rep.refresh()`

Drops idle or surplus connections and reschedules dials. Manual replicators
take no actions unless refresh is called.

### `await rep.flush()`

Wait until the current announces, lookups and dials settle.

### `await rep.close()`

Close the replicator and leave every topic.

### Properties

- `rep.connectionCount`, `rep.connecting`, `rep.connections`, `rep.peers`, `rep.sessions`
- `rep.activeSessions`, `rep.announcedSessions`
- `rep.isBackgrounded`, `rep.discoveryRunning`, `rep.suspended`
- `rep.stats`

### Events

- `'join' (session)` / `'remove' (session)`: a topic was joined or left
- `'connected' (connection)` / `'disconnected' (connection)`
- `'dropped' (count)`: connections were dropped by the GC
- `'discovery-error' (err, session)`: a DHT query failed (also counted in `stats.dht.errors`)
- `'ban' (peer, reason)`

### Connections

A connection is _wanted_ when the dialer would connect to that peer: it is
joined (`joinPeer`) or was seen on a topic we are a client of.

- Idle connections that are not wanted are dropped, whatever the connection
  count. Wanted ones are kept, dropping them would only redial them.
- Over `maxPeers`, unwanted connections go first, then the quietest.
- At `maxPeers`, a wanted peer waiting to be dialed makes room by dropping an
  unwanted connection.
- Joined peers are never dropped.

Disconnects the replicator causes itself (suspend, dedupe, idle, cap) never
count against a peer. An inbound peer that drops within seconds 20 times in
10 minutes is banned for hammering.

## Migrating from 1.x

- `rep.join(storeSession, topic, opts)` takes `{ server, client, limit }` and returns a handle, like hyperswarm.
- The swarming modes are gone: `pin`/`unpin`, `discover`, `bump`, autoswarm
  (`autoswarm`, `slots`, `priority`, `setAutoswarm`), the discovery timeouts and
  `gcIdleTime`. Applications decide which topics to join and for how long.
- `presuspend()` no longer refuses inbound connections; it is `background()`.
- `limit` counts connected peers (self excluded), like hyperswarm.
- `joinPeer` / `leavePeer` connect by public key.

## License

Apache-2.0
