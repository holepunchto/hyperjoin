const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')
const createTestnet = require('hyperdht/testnet')
const { joinRemote, onceEvent, seed, spawnPeer } = require('./helpers/backports')
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

  const p1 = await swarm.spawnPeer({ autoswarm: false })
  const p2 = await swarm.spawnPeer({ autoswarm: false })
  const p3 = await swarm.spawnPeer({ autoswarm: false })

  const topicA = b4a.alloc(32, 0xa)
  const topicB = b4a.alloc(32, 0xb)

  // topicA = P1 & P2
  await Promise.all([p1.join(topicA, { pin: true }), p2.join(topicA, { pin: true })])

  // topicB = P1 & P3
  const passive = await p1.join(topicB, { pin: true })
  await p3.join(topicB, { pin: true })

  await swarm.propagateWriters()

  await swarm.refresh() // announce
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

  const [session1, session2] = await Promise.all([
    p1.join(topic, { pin: true }),
    p2.join(topic, { pin: true })
  ])
  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()
  session1.discovery.refresh()
  session2.discovery.refresh()
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

test('gc does not count closing connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seed(1), maxPeers: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seed(2) })
  const p3 = await spawnPeer(t, { bootstrap, seed: seed(3) })

  const rep = p1.replicator

  for (const peer of [p2, p3]) {
    const connected = onceEvent(rep, 'connected')
    await joinRemote(peer, p1)
    await connected
  }

  const [closing, survivor] = rep.connections

  closing.destroy()
  await rep._gc()

  t.is(survivor.destroying, false, 'at capacity, nothing to evict')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('overlapping gc evicts down to maxPeers only', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seed(1), maxPeers: 2 })
  const rep = p1.replicator

  const peers = []
  for (let i = 2; i <= 5; i++) {
    const peer = await spawnPeer(t, { bootstrap, seed: seed(i) })
    const connected = onceEvent(rep, 'connected')
    await joinRemote(peer, p1)
    await connected
    peers.push(peer)
  }

  t.is(rep.connectionCount, 4, 'over cap')

  const later = Date.now() + 4 * 60 * 1000 // all connections idle
  await Promise.all([rep._gc(later), rep._gc(later)])

  t.is(rep.connectionCount, 2, 'evicted 2 of 4')

  await p1.destroy()
  for (const peer of peers) await peer.destroy()
  await testnet.destroy()
})
