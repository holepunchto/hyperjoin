const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')
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

  const p1 = await swarm.spawnPeer({ pooling: false })
  const p2 = await swarm.spawnPeer({ pooling: false })
  const p3 = await swarm.spawnPeer({ pooling: false })

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

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

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
  const p1 = await swarm.spawnPeer({ maxPeers: 1, gcDisabled: true })
  const p2 = await swarm.spawnPeer()
  const p3 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  const p2Key = toHex(p2.replicator.peerId)
  t.is(p1.replicator.connectionCount, 1, 'one connection at capacity')

  await p1.replicator._gc()

  await p3.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

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
  const whoHas = () => (owned ? topic : null)

  const p1 = await swarm.spawnPeer({ whoHas })
  const p2 = await swarm.spawnPeer()

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  const [connection] = p1.replicator.connections
  await connection.replicating

  t.is(connection.isIdle(), false, 'has active core')

  owned = false

  t.is(connection.isIdle(), true, 'no active core')

  await swarm.destroy()
})

test('gc preserves stable connections over new', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ maxPeers: 1, gcDisabled: true })
  const p2 = await swarm.spawnPeer()
  const p3 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  const p2Key = toHex(p2.replicator.peerId)
  const p3Key = toHex(p3.replicator.peerId)
  const a = p1.replicator.getConnection(p2Key)

  t.ok(a, 'p2 connected')
  t.is(a._survived, 0, 'not yet swept')

  await p1.replicator._gc()
  t.is(a._survived, 1, 'passed gc')

  await p3.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

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
