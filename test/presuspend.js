const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')
const { toHex } = b4a

test.solo('presuspend() unannounces active topics', async (t) => {
  const swarm = await spawnSwarm(t)
  const opts = {
    discoveryStartupDelay: 0,
    autoStartupDelay: 0
  }

  const p1 = await swarm.spawnPeer(opts)
  const p2 = await swarm.spawnPeer(opts)
  const topic = b4a.alloc(32, 0x1)

  const rep1 = p1.replicator

  t.is(rep1.announcedSessions.length, 0)
  t.is(rep1.connectionCount, 0)

  const connected = new Promise((resolve) => p1.replicator.on('connected', resolve))

  const session1 = await p1.join(topic)
  await p2.join(topic)

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()

  await connected
  t.ok('connected')

  t.is(rep1.isPresuspended, false)
  t.is(rep1.announcedSessions.length, 1)
  t.ok(rep1.connectionCount > 0)
  t.is(session1.isAnnounced, true)

  rep1.presuspend()
  const presuspended = rep1.flush()

  t.is(rep1.isPresuspended, true)
  t.ok(rep1.connectionCount > 0)

  await presuspended

  t.is(session1.isAnnounced, false)
  t.is(rep1.announcedSessions.length, 0)
  t.ok(rep1.connectionCount > 0, 'connections linger')

  await swarm.close()
})

test('presuspend() -> resume(), resumes announcements', async (t) => {
  const swarm = await spawnSwarm(t)
  const opts = { discoveryStartupDelay: 0, autoStartupDelay: 0 }
  const p1 = await swarm.spawnPeer(opts)
  const p2 = await swarm.spawnPeer(opts)
  const topic = b4a.alloc(32, 1)
  const rep1 = p1.replicator

  const session1 = await p1.join(topic)
  await p2.join(topic)
  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()

  const [connection] = rep1.connections
  rep1.presuspend()
  await rep1.flush()

  await rep1.resume()

  await swarm.flush()
  await swarm.refresh()

  t.is(rep1.isPresuspended, false)
  t.is(session1.isAnnounced, true, 'resume reannounces the topic')

  t.is(toHex([...rep1.connections][0].peerId), toHex(connection.peerId), 'connection retained')

  await swarm.close()
})

test('presuspend() -> suspend() -> resume(), resumes networking', async (t) => {
  const swarm = await spawnSwarm(t)
  const opts = { discoveryStartupDelay: 0, autoStartupDelay: 0 }
  const p1 = await swarm.spawnPeer(opts)
  const p2 = await swarm.spawnPeer(opts)
  const topic = b4a.alloc(32, 1)
  const rep1 = p1.replicator

  const session1 = await p1.join(topic)
  await p2.join(topic)
  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()

  rep1.presuspend()
  await rep1.flush()

  await rep1.suspend()

  t.is(rep1.suspended, true)
  t.is(rep1.connectionCount, 0, 'suspend drops the connection')
  t.is(session1.isAnnounced, false)

  const connected = new Promise((resolve) => rep1.once('connected', resolve))
  await rep1.resume()
  await swarm.flush() // announce + dial

  await swarm.refresh() // de-dupe
  await connected

  t.is(rep1.suspended, false)
  t.is(rep1.isPresuspended, false)
  t.is(session1.isAnnounced, true, 'resume reannounces the topic')
  t.is(rep1.connectionCount, 1, 'resume reconnects')

  await swarm.close()
})
