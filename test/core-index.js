const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('core-index: maps connections to sessions', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: false })
  const p2 = await swarm.spawnPeer({ autoswarm: false })
  const p3 = await swarm.spawnPeer({ autoswarm: false })

  const topicA = b4a.alloc(32, 0xA)
  const topicB = b4a.alloc(32, 0xB)

  p1.replicator.slots = 1

  await Promise.all([
    p1.join(topicA, { pin: true }),
    p2.join(topicA, { pin: true }),
  ])

  await Promise.all([
    p1.join(topicB),
    p3.join(topicA, { pin: true })
  ])

  console.log('pre-propagate')
  await swarm.propagateWriters()
  console.log('propagate')

  await swarm.refresh()
  await swarm.refresh()
  await swarm.flush()

  const { replicator } = p1

  t.is(replicator.sessions.size, 2, '2 session')
  t.is(replicator.activeSessions.length, 1, '1 active')

  const activeSessions = Array.from(p1.replicator.sessions.keys()).length
  t.comment(activeSessions)
  t.is(p1.replicator.connectionCount, 2, 'p1 connected to topic A & B')

  const [c1, c2] = replicator.connections

  /*
  let sessions = p1.replicator.coreIndex._ident(c1)
  t.is(sessions.length, 2)
  t.is(sessions.filter(i => i.active).length, 1)

  sessions = p1.replicator.coreIndex._identify(c2)
  t.is(sessions.length, 1)
  t.is(sessions.filter(i => i.active).length, 0)
  */

  t.is(c1.isIdle(), false)
  t.is(c2.isIdle(), true)

  await swarm.destroy()
})
