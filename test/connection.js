const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const { spawnSwarm } = require('./helpers')
const {
  joinRemote,
  onceEvent,
  seed,
  seedA,
  seedB,
  seedC,
  spawnPeer
} = require('./helpers/backports')
const { toHex } = b4a

/*
 * Test retained for future, use-case is legit
 * but blocked by following issue:
 *
 * # Protomux channels closure depends on remote
 *
 * A topic cannot be departed unless both peers leave.
 *
 * https://github.com/holepunchto/corestore/pull/176
 *
 *
 * Note: Corestore session-utils proxy to root store
 *
 * const root = new Corestore()
 * root.session().cores // <-- hits root not session.
 * root.session().watch(fn) // <-- hits root not session.
 *
 * replicator.session.hasCore(coreDiscoveryKey) // <-- hits session
 */

test.skip('core-index: connections with no active session are evicted', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ pool: false })
  const p2 = await swarm.spawnPeer({ pool: false })
  const p3 = await swarm.spawnPeer({ pool: false })

  const topicA = b4a.alloc(32, 0xa)
  const topicB = b4a.alloc(32, 0xb)

  // topicA = P1 & P2
  await Promise.all([p1.join(topicA, { pin: true }), p2.join(topicA, { pin: true })])

  // topicB = P1 & P3
  const passive = await p1.join(topicB, { pin: true })
  await p3.join(topicB, { pin: true })

  await swarm.propagateWriters()

  swarm.refresh() // announce
  await swarm.flush() // wait for dials

  const { replicator } = p1

  t.is(replicator.activeSessions.length, 2, 'active sessions')
  t.is(replicator.connectionCount, 2, 'P1 connected to topic A & B')

  // P1's connections
  let [c1, c2] = replicator.connections
  if (!b4a.equals(c1.peerId, p2.replicator.peerId)) {
    ;[c1, c2] = [c2, c1]
  }

  t.is(toHex(c1.peerId), toHex(p2.replicator.peerId), 'P2 @ C1')
  t.is(toHex(c2.peerId), toHex(p3.replicator.peerId), 'P3 @ C2')
  t.is(c1.isIdle(), false)
  t.is(c2.isIdle(), false)
  t.is(c2.stream.destroyed, false)

  // P1 leaves topicB
  await passive.store.close()
  swarm.refresh().catch(t.fail) // apply

  t.is(replicator.activeSessions.length, 1, 'session deactivated')
  t.is(replicator.connectionCount, 2, 'P1 connected to topic A & B')

  t.is(c1.isIdle(), false)
  t.is(c2.isIdle(), true, 'idle connection detected')

  await swarm.flush()

  // await swarm.refresh(Date.now() + CONNECTION_MAX_IDLE)
  // await swarm.flush()

  t.is(c2.stream.destroyed, true, 'evicted')
  t.is(replicator.connectionCount, 1, 'P1 connected only to topic A')

  await swarm.destroy()
})

test('connection becomes idle after three minutes without traffic', async (t) => {
  const IDLE_TIMEOUT = 3 * 60 * 1000 // 3 Minutes

  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  const connected = onceEvent(p1.replicator, 'connected')

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await connected

  const [connection] = p1.replicator.connections

  await connection.replicating
  connection.refresh()

  const now = Date.now()

  t.ok(connection.bytesRead !== 0, 'bytes read')
  t.ok(connection.bytesWritten !== 0, 'bytes written')

  connection.bytesRead = 0
  connection.bytesWritten = 0

  connection.refresh(now) // samples traffic

  t.is(connection.isIdle(now + IDLE_TIMEOUT - 1), false, 'recent traffic keeps connection active')
  t.is(connection.isIdle(now + IDLE_TIMEOUT + 1), true, 'stale traffic makes connection idle')

  await swarm.destroy()
})

test('connections are unconditionally evicted at max capacity', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ minConnections: 1, maxConnections: 2, gcDisabled: true })
  const p2 = await swarm.spawnPeer()
  const p3 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  const p2Connected = onceEvent(p1.replicator, 'connected')

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p2Connected

  const p2Key = toHex(p2.replicator.peerId)
  t.is(p1.replicator.connectionCount, 1, 'one connection at capacity')

  await p1.replicator._gc()

  const p3Connected = onceEvent(p1.replicator, 'connected')

  await p3.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p3Connected
  p1.replicator._maxConnections = 1 // inbound at max peers is refused, so lower the cap once p3 is in

  t.is(p1.replicator.connectionCount, 2, 'connection exceeds capacity')

  await p1.replicator._gc()

  t.is(p1.replicator.connectionCount, 1, 'connection at capacity after gc')
  t.ok(p1.replicator.getConnection(p2Key), 'keeps established connection')

  await swarm.destroy()
})

test('connection becomes idle when no topic owns its core', async (t) => {
  const swarm = await spawnSwarm(t)
  const topic = b4a.alloc(32, 1)

  let owned = true
  const topicOf = () => (owned ? topic : null)

  const p1 = await swarm.spawnPeer({ topicOf })
  const p2 = await swarm.spawnPeer()

  const connected = onceEvent(p1.replicator, 'connected')

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await connected

  const [connection] = p1.replicator.connections
  await connection.replicating

  t.is(connection.isIdle(), false, 'has active core')

  owned = false

  t.is(connection.isIdle(), true, 'no active core')

  await swarm.destroy()
})

test('gc preserves stable connections over new', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ minConnections: 1, maxConnections: 2, gcDisabled: true })
  const p2 = await swarm.spawnPeer()
  const p3 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  const p2Connected = onceEvent(p1.replicator, 'connected')

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p2Connected

  const p2Key = toHex(p2.replicator.peerId)
  const p3Key = toHex(p3.replicator.peerId)
  const a = p1.replicator.getConnection(p2Key)

  t.ok(a, 'p2 connected')
  t.is(a._survived, 0, 'not yet swept')

  await p1.replicator._gc()
  t.is(a._survived, 1, 'passed gc')

  const p3Connected = onceEvent(p1.replicator, 'connected')

  await p3.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p3Connected
  p1.replicator._maxConnections = 1 // inbound at max peers is refused, so lower the cap once p3 is in

  const b = p1.replicator.getConnection(p3Key)

  t.is(p1.replicator.connectionCount, 2, 'over capacity')
  t.ok(b, 'p3 connected')
  t.ok(a._survived > b._survived, 'new connection will be dropped')

  await p1.replicator._gc()

  t.is(p1.replicator.connectionCount, 1, 'gc dropped overflow')
  t.ok(p1.replicator.getConnection(p2Key), 'p2 remains connected')
  t.is(a._survived, 2, 'passed gc twice')

  await swarm.destroy()
})

test('regression: gc does not count closing connections', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ minConnections: 1, maxConnections: 2, gcDisabled: true })
  const p2 = await swarm.spawnPeer()
  const p3 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  const p2Connected = onceEvent(p1.replicator, 'connected')

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p2Connected

  const p3Connected = onceEvent(p1.replicator, 'connected')

  await p3.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await p3Connected
  p1.replicator._maxConnections = 1 // inbound at max peers is refused, so lower the cap once p3 is in

  t.is(p1.replicator.connectionCount, 2, 'connection exceeds capacity')

  const [closing, survivor] = p1.replicator.connections

  closing.destroy()
  await p1.replicator._gc()

  t.is(survivor.destroying, false, 'at capacity, nothing to evict')

  await swarm.destroy()
})

test('regression: heuristic-GC does not evict below maxConnections', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ minConnections: 2, maxConnections: 4, gcDisabled: true })
  const topic = b4a.alloc(32, 1)

  await p1.join(topic, { pin: true })

  for (let i = 0; i < 4; i++) {
    const peer = await swarm.spawnPeer()
    const connected = onceEvent(p1.replicator, 'connected')

    await peer.join(topic, { pin: true })
    await swarm.propagateWriters()
    swarm.refresh()
    await swarm.flush()
    await connected
  }
  p1.replicator._maxConnections = 2 // inbound at max peers is refused, so lower the cap once all are in

  t.is(p1.replicator.connectionCount, 4, 'over capacity')

  const later = Date.now() + 4 * 60 * 1000 // all connections idle
  await Promise.all([p1.replicator._gc(later), p1.replicator._gc(later)])

  t.is(p1.replicator.connectionCount, 2, 'evicted 2 of 4')

  await swarm.destroy()
})

test('new inbound is refused at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p2, p1)
  await p2Connected

  let opened = 0
  p1.replicator.server.on('connection', () => opened++)

  const err = await p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))

  t.is(err.code, 'PEER_BUSY', 'p3 refused')
  t.is(opened, 0, 'p1 opened no connection')
  t.is(p1.replicator.stats.connects.server.refused, 1, 'p1 counted the refusal')
  t.absent(p1.replicator.dialer.lookup(p3.replicator.peerId).isBanned, 'p1 did not ban p3')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('a reconnect is accepted at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p2, p1)
  await p2Connected
  const existing = p1.replicator.getConnection(p2.peerId)
  await existing.replicating

  // p2 reconnects without its end of the old link, as after a network change
  const connected = onceEvent(p1.replicator, 'connected')
  const stream = p2.replicator.dht.connect(p1.replicator.peerId, { keyPair: p2.replicator.keyPair })
  stream.on('error', () => {})
  await connected

  t.ok(existing.stream.destroyed, 'p1 replaced the old connection')

  stream.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('a redial that gc dedupe keeps is accepted at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  // seedA's key is lower, so gc dedupe keeps p1's inbound from p2
  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p1, p2)
  await p2Connected
  const existing = p1.replicator.getConnection(p2.peerId)

  // p2 redials without its end of p1's link, as after a network change
  const connected = onceEvent(p1.replicator, 'connected')
  const stream = p2.replicator.dht.connect(p1.replicator.peerId, { keyPair: p2.replicator.keyPair })
  stream.on('error', () => {})
  await connected

  await p1.replicator._gc()
  t.ok(existing.stream.destroyed, 'gc replaced the old connection')
  t.ok(p1.replicator.inbound.has(p2.peerId), 'p1 kept the redial')

  stream.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('a redial that gc dedupe drops is refused at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  // seedB's key is higher, so gc dedupe would keep p1's outbound to p2
  const p1 = await spawnPeer(t, { bootstrap, seed: seedB, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedA })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p1, p2)
  await p2Connected

  t.is(p1.replicator._handleAccept(p2.replicator.peerId), false, 'p1 replies busy')

  const outbound = p1.replicator.getConnection(p2.peerId)
  await outbound.replicating
  outbound.lastActivity = Date.now() - 3 * 60 * 1000 - 1 // no traffic for 3 minutes
  t.is(p1.replicator._handleAccept(p2.replicator.peerId), false, 'p1 still replies busy when idle')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one inbound per idle connection is accepted at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const p4 = await spawnPeer(t, { bootstrap, seed: seed(0xd) })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p2, p1)
  await p2Connected
  const idle = p1.replicator.getConnection(p2.peerId)
  await idle.replicating
  idle.lastActivity = Date.now() - 3 * 60 * 1000 - 1 // no traffic for 3 minutes

  // p3 opens p1's cores so its own link never looks idle
  await Promise.all(p1.cores.map((core) => p3.session.get(core.key).ready()))
  const connected = onceEvent(p1.replicator, 'connected')
  await p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))
  await connected

  const err = await p4.replicator._connect(p4.replicator.dialer.lookup(p1.replicator.peerId))
  t.is(err.code, 'PEER_BUSY', 'p4 refused, p3 took the idle slot')

  await p1.replicator._gc()

  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p3.peerId],
    'gc evicted the idle connection and kept p3'
  )

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await p4.destroy()
  await testnet.destroy()
})

test('outbound dials are not refused at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  const p2Connected = onceEvent(p1.replicator, 'connected')
  await joinRemote(p2, p1)
  await p2Connected

  await p1.replicator._connect(p1.replicator.dialer.lookup(p3.replicator.peerId))
  t.ok(p1.replicator.outbound.has(p3.peerId), 'p1 dialed p3 at max peers')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('inbound is accepted above minConnections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 2 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const rep = p1.replicator

  const p2Connected = onceEvent(rep, 'connected')
  await p2.replicator._connect(p2.replicator.dialer.lookup(rep.peerId))
  await p2Connected

  const p3Connected = onceEvent(rep, 'connected')
  const err = await p3.replicator._connect(p3.replicator.dialer.lookup(rep.peerId))

  t.absent(err, 'p3 not refused')

  await p3Connected

  t.ok(rep.inbound.has(p3.peerId), 'p3 accepted')
  t.is(rep.stats.connects.server.refused, 0)

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})
