const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('pool: join() pinned (hyperswarm mode)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)

  let joined = false
  p1.replicator.once('join', () => {
    joined = true
  })

  await Promise.all([p1.join(topic, { pin: true }), p2.join(topic, { pin: true })])

  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await swarm.gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)
  t.ok(joined, 'join fired')

  await swarm.destroy()
})

test('pool: join() default delay is minimum 5sec', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic), p2.join(topic)])

  await swarm.propagateWriters()
  swarm.refresh()

  // setTimeout sanctioned for sanity
  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test('pool: join() delay=0 announces topics without jitter', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([p1.join(topic, { delay: 0 }), p2.join(topic, { delay: 0 })])

  await swarm.propagateWriters()
  swarm.refresh() // announce & lookup
  await swarm.flush() // dht-query + dials
  await swarm.gc() // de-dupe

  t.is(p1.replicator.announcedSessions.length, 1, 'autoswarm announce')
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

test('pool: pin()/unpin()', async (t) => {
  const swarm = await spawnSwarm(t)

  const topic = b4a.alloc(32, 0x1)

  const a = await swarm.spawnPeer()
  const b = await swarm.spawnPeer()
  const c = await swarm.spawnPeer()

  // A + B joins Active

  await a.join(topic)
  a.replicator.pin(topic)

  await b.join(topic)
  b.replicator.pin(topic)

  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()
  await swarm.gc() // de-dupe

  t.is(a.replicator.connectionCount, 1)
  t.is(b.replicator.connectionCount, 1, 'active announces')

  // A + B de-activates, C validates

  a.replicator.unpin(topic)
  b.replicator.unpin(topic)
  swarm.refresh() // unnanounce

  await c.join(topic)
  c.replicator.pin(topic) // TODO: use scan()

  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  t.is(c.replicator.peers.size, 0, 'A, B unannounced')
  t.is(c.replicator.connectionCount, 0, 'C has no connections')

  // A re-activates, C validates

  a.replicator.pin(topic)
  const remoteConnected = new Promise((resolve) => c.replicator.on('connected', resolve))

  swarm.refresh()
  await swarm.flush() // dht-query + dials
  await remoteConnected

  t.is(c.replicator.peers.size, 1, 'A re-activated')

  await swarm.gc() // de-dupe
  t.is(c.replicator.connectionCount, 1, 'C connected to A')
  t.is(a.replicator.connectionCount, 2, 'A connected to B, C')

  await swarm.destroy()
})

test('pool: refresh() populates slots', async (t) => {
  const swarm = await spawnSwarm(t)
  const gcIdleTime = 100
  const slots = 4

  const topics = new Array(10).fill(null).map((_, i) => b4a.alloc(32, i))

  const p1 = await swarm.spawnPeer({ slots, gcIdleTime })
  await p1.replicator._pendingListen

  const joins = []
  const nCores = 5
  for (const topic of topics) {
    joins.push(p1.join(topic, { nCores, delay: 0 }))
  }
  await swarm.propagateWriters()
  await Promise.all(joins)

  t.is(p1.replicator.activeSessions.length, 0, 'nothing active')
  t.is(p1.replicator.announcedSessions.length, 0, 'nothing announced')

  await p1.replicator.flush()
  p1.replicator.refresh()
  await p1.replicator.flush()
  t.is(p1.replicator.activeSessions.length, slots, 'topics auto activate')
  t.is(p1.replicator.announcedSessions.length, slots, 'topics announced')

  await p1.replicator.suspend()
  t.is(p1.replicator.activeSessions.length, 0, 'topics deactivated')
  t.is(p1.replicator.announcedSessions.length, 0, 'topics unannounced')

  await swarm.destroy()
})

test('pool: topics exceeding idle threshold are evicted', async (t) => {
  const swarm = await spawnSwarm(t)
  const gcIdleTime = 100
  const slots = 4

  const topics = new Array(10).fill(null).map((_, i) => b4a.alloc(32, i))

  const p1 = await swarm.spawnPeer({ slots, gcIdleTime })

  const joins = []
  for (const topic of topics) {
    joins.push(p1.join(topic, { delay: 0 }))
  }
  await swarm.propagateWriters()
  await Promise.all(joins)

  let nEvicted = 0
  p1.replicator.on('evicted', (n) => (nEvicted += n))

  p1.replicator.refresh()
  await p1.replicator.collected
  t.is(p1.replicator.sessions.size, topics.length, 'all topics pooled')
  t.is(p1.replicator.activeSessions.length, slots, 'topics auto activate')

  p1.replicator.refresh(Date.now() + gcIdleTime + 1)
  await p1.replicator.collected

  t.is(nEvicted, topics.length - slots, 'idle sessions evicted')
  t.is(p1.replicator.sessions.size, slots, 'active topics retained')
  t.is(p1.replicator.activeSessions.length, slots, 'topics still active')

  await swarm.destroy()
})

test('pool: session.destroy() removes session', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  const session = await p1.join(topic, { delay: 0 })

  p1.replicator.refresh()
  t.is(p1.replicator.sessions.size, 1, 'topic pooled')
  t.is(p1.replicator.activeSessions.length, 1, 'topic active')

  const removed = new Promise((resolve) => p1.replicator.once('leave', resolve))

  await session.destroy()

  t.is(await removed, session, 'remove event')
  t.is(p1.replicator.sessions.size, 0, 'session removed')

  await swarm.destroy()
})

test('pool: leave() removes topic from pool', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  const session = await p1.join(topic, { delay: 0 })

  p1.replicator.refresh()
  t.is(p1.replicator.sessions.size, 1, 'topic pooled')
  t.is(p1.replicator.activeSessions.length, 1, 'topic active')

  const removed = new Promise((resolve) => p1.replicator.once('leave', resolve))

  await p1.replicator.leave(topic)

  t.is(await removed, session, 'leave event')
  t.is(session.closed, true, 'session closed')
  t.is(p1.replicator.sessions.size, 0, 'session evicted')

  await swarm.destroy()
})

test('pool: refresh() rotates topics by priority', async (t) => {
  const swarm = await spawnSwarm(t)
  const peer = await swarm.spawnPeer({ slots: 2, minActiveTime: 0 })
  const now = Date.now()

  const high = await peer.join(b4a.alloc(32, 3), { priority: 3, delay: 0 })
  const middle = await peer.join(b4a.alloc(32, 2), { priority: 2, delay: 0 })
  const low = await peer.join(b4a.alloc(32, 1), { priority: 1, delay: 0 })

  high._autoStart(now)
  low._autoStart(now)
  peer.replicator._autoselect(now)

  t.is(peer.replicator.activeSessions.length, 2, 'rotation keeps all slots filled')
  t.is(middle.isAuto, true, 'middle-ranked inactive session fills the slot')

  await swarm.destroy()
})

test('pool: pooling can be disabled', async (t) => {
  const swarm = await spawnSwarm(t)
  const peer = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)

  t.is(peer.replicator.announcedSessions.length, 0, 'nothing announced')
  await peer.join(topic, { delay: 0 })

  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  t.is(peer.replicator.announcedSessions.length, 1, 'autoswarm announce')

  peer.replicator.pooling = false
  peer.replicator.refresh()
  await peer.replicator.flush()

  t.is(peer.replicator.announcedSessions.length, 0, 'unannounced')

  peer.replicator.pooling = true
  peer.replicator.refresh()
  await peer.replicator.flush()

  t.is(peer.replicator.announcedSessions.length, 1, 're-announced')

  await swarm.destroy()
})

test('bump value announced & visible on lookup', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)

  await p1.join(topic, { pin: true })
  p1.replicator.bump(topic, 0, 77)
  p1.replicator.refresh()
  await p1.replicator.flush()

  t.is(await p2.replicator.lookup(topic), 77)

  await swarm.destroy()
})

test('bump value announced & visible on scan', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)

  await p1.join(topic, { pin: true })
  p1.replicator.bump(topic, 0, 77)
  p1.replicator.refresh()
  await p1.replicator.flush()

  const op = p2.replicator.scan(topic, { delay: 0, timeout: 1000 })
  await op.complete

  t.is(op.discovery.bump.seen, 77)

  await swarm.destroy()
})
