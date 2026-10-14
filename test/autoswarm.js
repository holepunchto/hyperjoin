const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')
const { toHex } = b4a

test('autoswarm: all topics pinned (hyperswarm mode)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()
  await swarm.gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

// (replicator mode)
test('autoswarm: two new instances do not swarm without intent (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic), p2.join(topic)])

  await swarm.propagateWriters()
  await swarm.refresh()

  // setTimeout sanctioned for sanity
  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test('autoswarm: deferSwarming disabled', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([
    p1.join(topic, { deferSwarming: false }),
    p2.join(topic, { deferSwarming: false })
  ])

  await swarm.propagateWriters()
  await swarm.refresh() // announce & lookup
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
  await swarm.refresh()
  await swarm.flush()
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
  await swarm.refresh()
  await swarm.flush()

  t.is(c.replicator.peers.size, 0, 'A, B unannounced')
  t.is(c.replicator.connectionCount, 0, 'C has no connections')

  // A re-activates, C validates

  session1.pin()
  const remoteConnected = new Promise((resolve) => c.replicator.on('connected', resolve))

  await swarm.refresh()
  await swarm.flush() // dht-query + dials
  await remoteConnected

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
  await p2.replicator.flush()

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  let nEvDiscovered = 0
  session1.on('discovered', () => nEvDiscovered++)

  const evSeen = new Promise((resolve) => session1.once('bump', resolve))
  session1.discover()

  t.is(session1.isDiscovering, true)
  await p1.replicator.refresh()
  await p1.replicator.flush()

  t.is(session1.seen, 99, 'prop seen')
  t.is(await evSeen, 99, 'bump event')

  t.is(session1.announce, false)
  t.is(session1._discoveryTimeout, defaultTimeout)

  // discovery window is shortened on successful replication

  session1.bump()
  await p1.replicator.refresh()
  await p1.replicator.flush()

  t.is(p1.replicator.sessions.get(toHex(topic)), session1, 'still open')
  t.is(session1.announce, false, 'still not listening')
  t.is(session1._discoveryTimeout, updatedDiscoveryTimeout, 'shortened')

  // discovery should deactivate

  const now = session1._startTime + session1._discoveryTimeout + 1
  await p1.replicator.refresh(now)
  await p1.replicator.flush()

  t.is(session1.isDiscovering, false)
  t.is(nEvDiscovered, 1)

  await swarm.destroy()
})

test('autoswarm: discover() -> pin() upgrades the session, keeping discovery intact', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ discoveryStartupDelay: 0 })
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  const session1 = await p1.join(topic)
  await p2.join(topic, { pin: true })
  await swarm.propagateWriters()

  t.is(session1.isDiscovering, false)
  t.is(session1.isPinned, false)

  session1.discover()
  t.is(session1.isDiscovering, true, 'discovering after discover()')

  const discovery = session1.discovery

  session1.pin()
  t.is(session1.isDiscovering, false, 'no longer discovering once pinned')
  t.is(session1.isPinned, true, 'pin() upgrades the session')
  t.is(session1.active, true)
  t.is(session1.discovery, discovery, 'discovery instance is retained across the upgrade, not recreated')

  await swarm.refresh()
  await swarm.flush()

  t.is(session1.isPinned, true, 'still pinned after refresh')
  t.is(session1.announce, true, 'pinned session announces itself')
  t.is(session1.isAnnounced, true, 'discovery actually announced on the dht')
  t.ok(p1.replicator.connectionCount > 0, 'connects to the other pinned peer')

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

test('autoswarm: replicator evicts sessions that idle above threshold', async (t) => {
  const swarm = await spawnSwarm(t)
  const gcIdleTime = 100
  const slots = 4

  const topics = new Array(10).fill(null).map((_, i) => b4a.alloc(32, i))

  const p1 = await swarm.spawnPeer({ slots, gcIdleTime, autoStartupDelay: 0 })

  const joins = []
  for (const topic of topics) {
    joins.push(p1.join(topic))
  }
  await swarm.propagateWriters()
  await Promise.all(joins)

  let nEvicted = 0
  p1.replicator.on('evicted', (n) => (nEvicted += n))

  await p1.replicator.refresh()
  t.is(p1.replicator.sessions.size, topics.length, 'all topics pooled')
  t.is(p1.replicator.activeSessions.length, slots, 'topics auto activate')

  await p1.replicator.refresh(Date.now() + gcIdleTime + 1)

  t.is(nEvicted, topics.length - slots, 'idle sessions evicted')
  t.is(p1.replicator.sessions.size, slots, 'active topics retained')
  t.is(p1.replicator.activeSessions.length, slots, 'topics still active')

  await swarm.destroy()
})

test('autoswarm: replicator evicts session on direct session.destroy()', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ autoStartupDelay: 0 })

  const topic = b4a.alloc(32, 0x1)
  const session = await p1.join(topic)

  await p1.replicator.refresh()
  t.is(p1.replicator.sessions.size, 1, 'topic pooled')
  t.is(p1.replicator.activeSessions.length, 1, 'topic active')

  const removed = new Promise((resolve) => p1.replicator.once('remove', resolve))

  await session.destroy()

  t.is(await removed, session, 'remove event')
  t.is(p1.replicator.sessions.size, 0, 'session evicted')

  await swarm.destroy()
})

test('autoswarm: replicator evicts session on indirect storeSession.closed()', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer({ autoStartupDelay: 0 })

  const topic = b4a.alloc(32, 0x1)
  const session = await p1.join(topic)

  await p1.replicator.refresh()
  t.is(p1.replicator.sessions.size, 1, 'topic pooled')
  t.is(p1.replicator.activeSessions.length, 1, 'topic active')

  const removed = new Promise((resolve) => p1.replicator.once('remove', resolve))

  await session.store.close()

  t.is(await removed, session, 'remove event')
  t.is(session.closed, true, 'session closed')
  t.is(p1.replicator.sessions.size, 0, 'session evicted')

  await swarm.destroy()
})

test('autoswarm: rotation refills freed slots', async (t) => {
  const swarm = await spawnSwarm(t)
  const peer = await swarm.spawnPeer({ slots: 2, autoStartupDelay: 0, minActiveTime: 0 })
  const now = Date.now()

  const high = await peer.join(b4a.alloc(32, 3), { priority: 3 })
  const middle = await peer.join(b4a.alloc(32, 2), { priority: 2 })
  const low = await peer.join(b4a.alloc(32, 1), { priority: 1 })

  high._autoStart(now)
  low._autoStart(now)
  peer.replicator._autoselect(now)

  t.is(peer.replicator.activeSessions.length, 2, 'rotation keeps all slots filled')
  t.is(middle.isAuto, true, 'middle-ranked inactive session fills the slot')

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
  await swarm.flush()

  t.is(peer.replicator.announcedSessions.length, 1, 'autoswarm announce')

  await peer.replicator.setAutoswarm(false)

  t.is(peer.replicator.announcedSessions.length, 0, 'unannounced')

  await peer.replicator.setAutoswarm(true)

  t.is(peer.replicator.announcedSessions.length, 1, 're-announced')

  await swarm.destroy()
})

test('Session close removes the store listener', async (t) => {
  const swarm = await spawnSwarm(t)
  t.teardown(() => swarm.close())
  const peer = await swarm.spawnPeer()
  const store = peer.store.session()
  const baseline = store.listenerCount('close')
  const topic = b4a.alloc(32, 1)

  for (let i = 0; i < 3; i++) {
    const session = peer.replicator.join(store, topic)
    await session.ready()
    t.is(store.listenerCount('close'), baseline + 1, 'session adds one store listener')
    await session.close()
    t.is(store.listenerCount('close'), baseline, 'session close removes its store listener')
  }

  const session = peer.replicator.join(store, topic)
  await session.ready()
  const closed = new Promise((resolve) => session.once('close', resolve))
  await store.close()
  await closed
  t.ok(session.closed, 'closing the store still closes the live session')
})

test('discovery: forwards the peer limit', async (t) => {
  const swarm = await spawnSwarm(t)
  t.teardown(() => swarm.close())
  const peer = await swarm.spawnPeer()
  const limited = await peer.join(b4a.alloc(32, 42), { discover: true, limit: 1 })
  const unlimited = await peer.join(b4a.alloc(32, 43), { discover: true })

  t.is(limited.discovery.limit, 1)
  t.is(unlimited.discovery.limit, Infinity)
})
