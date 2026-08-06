const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('autoswarm: all topics pinned (hyperswarm mode)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.refresh()
  await swarm.flush()
  await swarm.gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

// replicator mode

test('autoswarm: two new instances do not swarm without intent (sanity test)', async (t) => {
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

// TODO Flakes
test('autoswarm: deferSwarming disabled', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic, { deferSwarming: false }), p2.join(topic, { deferSwarming: false })])

  await swarm.propagateWriters()
  await swarm.refresh() // lookup then commit announce
  await swarm.refresh() // lookup
  await swarm.flush() // dht-query + dials

  t.is(p1.replicator.announcedSessions.length, 1, 'autoswarm announce')
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

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
  session1.pin()

  const session2 = await b.join(topic)
  session2.pin()

  await swarm.propagateWriters()
  await swarm.refresh() // lookup then commit announce
  await swarm.refresh() // lookup
  await swarm.flush() // dht-query + dials
  await swarm.gc() // de-dupe

  t.is(a.replicator.connectionCount, 1)
  t.is(b.replicator.connectionCount, 1, 'active announces')

  // A + B de-activates, C validates

  session1.unpin()
  session2.unpin()
  await swarm.refresh() // unnanounce

  const session3 = await c.join(topic)
  session3.pin() // TODO: use discover()

  await swarm.propagateWriters()
  await swarm.refresh() // lookup
  await swarm.flush() // dht-query + dials

  t.is(c.replicator.peers.size, 0, 'A, B unannounced')
  t.is(c.replicator.connectionCount, 0, 'C has no connections')

  // A re-activates, C validates

  session1.pin()

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
    gcIdleTime,
    updatedDiscoveryTimeout,
    discoveryStartupDelay: 0
  })
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  const session1 = await p1.join(topic)
  const session2 = await p2.join(topic)
  await swarm.propagateWriters()

  session2.bump(99)
  session2.pin()
  await p2.replicator.refresh()
  await p2.replicator.refresh()

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  let nEvDiscovered = 0
  session1.on('discovered', () => nEvDiscovered++)

  const evSeen = new Promise((resolve) => session1.once('bump', resolve))
  session1.discover()

  t.is(session1.isDiscovering, true)
  await p1.replicator.refresh()

  t.is(session1.seen, 99, 'prop seen')
  t.is(await evSeen, 99, 'bump event')

  t.is(session1.announce, false)
  t.is(session1._discoveryTimeout, defaultTimeout)

  // discovery window is shortened on successful replication

  session1.bump()
  await p1.replicator.refresh()

  t.is(p1.replicator.sessions.get(toHex(topic)), session1, 'still open')
  t.is(session1.announce, false, 'still not listening')
  t.is(session1._discoveryTimeout, updatedDiscoveryTimeout, 'shortened')

  // discovery should deactivate

  const now = session1._startTime + session1._discoveryTimeout + 1
  await p1.replicator.refresh(now)

  t.is(session1.isDiscovering, false)
  t.is(nEvDiscovered, 1)

  await swarm.destroy()
})

test('autoswarm: replicator activates sessions', async (t) => {
  const swarm = await spawnSwarm(t)
  const gcIdleTime = 100
  const slots = 4

  const topics = new Array(10).fill(null).map((_, i) => b4a.alloc(32, i))

  const p1 = await swarm.spawnPeer({ slots, gcIdleTime, autoStartupDelay: 0 })

  const joins = []
  const nCores = 5
  for (const topic of topics) {
    joins.push(p1.join(topic, { nCores }))
  }
  await swarm.propagateWriters()
  await Promise.all(joins)

  t.is(p1.replicator.activeSessions.length, 0, 'nothing active')
  t.is(p1.replicator.announcedSessions.length, 0, 'nothing announced')

  await p1.replicator.refresh()
  t.is(p1.replicator.activeSessions.length, slots, 'topics auto activate')
  t.is(p1.replicator.announcedSessions.length, slots, 'topics announced')

  await p1.replicator.suspend()
  t.is(p1.replicator.activeSessions.length, 0, 'topics deactivated')
  t.is(p1.replicator.announcedSessions.length, 0, 'topics unannounced')

  await swarm.destroy()
})

test('autoswarm: can be disabled', async (t) => {
  const swarm = await spawnSwarm(t)
  const peer = await swarm.spawnPeer({ autoStartupDelay: 0 })

  const topic = b4a.alloc(32, 0x1)

  t.is(peer.replicator.announcedSessions.length, 0, 'nothing announced')
  await peer.join(topic)

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.refresh()
  await swarm.flush()

  t.is(peer.replicator.announcedSessions.length, 1, 'autoswarm announce')

  await peer.replicator.setAutoswarm(false)

  t.is(peer.replicator.announcedSessions.length, 0, 'unannounced')

  await peer.replicator.setAutoswarm(true)

  t.is(peer.replicator.announcedSessions.length, 1, 're-announced')

  await swarm.destroy()
})

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
