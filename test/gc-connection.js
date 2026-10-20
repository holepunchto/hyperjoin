const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('connection becomes idle after three minutes without traffic', async (t) => {
  const IDLE_TIMEOUT = 3 * 60 * 1000 // 3 Minutes

  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)

  await Promise.all([p1.join(topic), p2.join(topic)])
  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()

  const [connection] = p1.replicator.connections

  await connection.replicating

  const now = connection.lastActivity // the last protocol message

  t.is(connection.isIdle(now + IDLE_TIMEOUT - 1), false, 'recent traffic keeps connection active')
  t.is(connection.isIdle(now + IDLE_TIMEOUT + 1), true, 'stale traffic makes connection idle')

  await swarm.destroy()
})
