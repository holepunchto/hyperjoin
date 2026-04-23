const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

// hyperswarm mode (deprecated)
test('autoswarm: disabled, all are active (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: false })
  const p2 = await swarm.spawnPeer({ autoswarm: false })
  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic), p2.join(topic)])

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()
  await swarm.gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

// replicator mode

test('autoswarm: enabled, two new instances do not swarm without intent (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic), p2.join(topic)])

  await swarm.propagateWriters()
  await swarm.refresh()

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test('autoswarm: manually activated instances', async (t) => {
  const swarm = await spawnSwarm(t)

  const topic = b4a.alloc(32, 0x1)

  const a = await swarm.spawnPeer()
  const b = await swarm.spawnPeer()
  const c = await swarm.spawnPeer()

  // A + B joins Active

  const session1 = await a.join(topic)
  session1.setActive(true)

  const session2 = await b.join(topic)
  session2.setActive(true)

  await swarm.propagateWriters()
  await swarm.refresh() // lookup then commit announce
  await swarm.refresh() // lookup
  await swarm.flush() // dht-query + dials
  await swarm.gc() // de-dupe

  t.is(a.replicator.connectionCount, 1)
  t.is(b.replicator.connectionCount, 1, 'active announces')

  // A + B de-activates, C validates

  session1.setActive(false)
  session2.setActive(false)
  await swarm.refresh() // unnanounce

  const session3 = await c.join(topic)
  session3.setActive(true) // TODO: use discover()

  await swarm.propagateWriters()
  await swarm.refresh() // lookup
  await swarm.flush() // dht-query + dials

  t.is(c.replicator.peers.size, 0, 'A, B unannounced')
  t.is(c.replicator.connectionCount, 0, 'C has no connections')

  // A re-activates, C validates

  session1.setActive(true)

  await swarm.refresh()
  await swarm.refresh()
  await swarm.flush() // dht-query + dials

  t.is(c.replicator.peers.size, 1, 'A re-activated')

  await swarm.gc() // de-dupe
  t.is(c.replicator.connectionCount, 1, 'C connected to A')
  t.is(a.replicator.connectionCount, 2, 'A connected to B, C')

  await swarm.destroy()
})

test('autoswarm: can enable discovery swarming', async (t) => {
  const swarm = await spawnSwarm(t)

  const defaultTimeout = 60 * 1000 // 1m
  const updatedDiscoveryTimeout = 500
  const gcIdleTime = 100
  const p1 = await swarm.spawnPeer({
    refreshInterval: 50, // TODO: not implemented
    gcIdleTime,
    updatedDiscoveryTimeout
  })
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  const session1 = await p1.join(topic)
  const session2 = await p2.join(topic)

  await swarm.propagateWriters()

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  let idle = 0
  session1.once('idle', () => idle++)

  session1.startDiscovery()
  await p1.replicator.refresh()

  t.is(session1.announce, false)
  t.is(session1.discoveryTimeout, defaultTimeout)

  session2.setActive(true)
  await p2.replicator.refresh()

  // discovery window is shortened on successful replication

  session1.bumpDiscovery()
  await p1.replicator.refresh()

  t.is(p1.replicator.sessions.get(toHex(topic)), session1, 'still open')
  t.is(session1.announce, false, 'still not listening')
  t.is(session1.discoveryTimeout, updatedDiscoveryTimeout, 'shortened')

  // discovery should deactivate

  const now = session1.discoveryStartTime + session1.discoveryTimeout + 1
  await p1.replicator.refresh(now)

  t.is(session1.isDiscovering, false)

  // Evicted on next refresh
  await p1.replicator.refresh(now + gcIdleTime + 1)
  t.is(idle, 1, '"idle" event fired')
  t.is(p1.replicator.sessions.size, 0, 'session ejected')

  await swarm.destroy()
})

test.skip('autoswarm: replicator activates fallbacks', (t) => {})

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
