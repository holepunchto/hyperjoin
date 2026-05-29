const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('presuspend() enters background mode', async (t) => {
  const swarm = await spawnSwarm(t)
  const opts = {
    discoveryStartupDelay: 0,
    autoStartupDelay: 0
  }

  const p1 = await swarm.spawnPeer(opts)
  const p2 = await swarm.spawnPeer(opts)
  const topic = b4a.alloc(32, 0x1)

  const rep1 = p1.replicator

  t.is(rep1.activeSessions.length, 0)
  t.is(rep1.connectionCount, 0)

  const connected = new Promise(resolve => p1.replicator.on('connected', resolve))

  const session1 = await p1.join(topic)
  await p2.join(topic)

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()
  await swarm.refresh()
  await swarm.flush()

  await connected
  t.ok('connected')

  t.is(rep1.isPresuspended, false)
  t.is(rep1.activeSessions.length, 1)
  t.is(rep1.connectionCount, 1)
  t.is(session1.isAnnounced, true)

  const presuspended = rep1.presuspend()

  t.is(rep1.isPresuspended, true)
  t.is(rep1.connectionCount, 1)

  await presuspended

  t.is(session1.isAnnounced, false)

  // test presuspend->resume

  await rep1.resume()
  await swarm.refresh()
  await swarm.flush()

  t.is(rep1.isPresuspended, false)
  t.is(session1.isAnnounced, true)

  // test presuspend->suspend

  t.is(rep1.connectionCount, 1) // maybe

  t.test('presuspended', (t) => {
    t.plan(1)
    rep1.presuspend().then(() => t.pass())
  })

  await rep1.suspend()

  t.is(rep1.connectionCount, 0)
  t.is(rep1.isPresuspended, true)
  t.is(session1.isAnnounced, false)
  t.is(rep1.activeSessions.length, 0)

  await swarm.close()
})
