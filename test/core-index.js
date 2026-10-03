const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')
const { toHex } = b4a

test('core-index: maps connections to sessions', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: false })
  const p2 = await swarm.spawnPeer({ autoswarm: false })
  const p3 = await swarm.spawnPeer({ autoswarm: false })

  const topicA = b4a.alloc(32, 0xA)
  const topicB = b4a.alloc(32, 0xB)


  await Promise.all([
    p1.join(topicA, { pin: true }),
    p2.join(topicA, { pin: true }),
  ])

  const passive = await p1.join(topicB, { pin: true })
  await p3.join(topicB, { pin: true })

  console.log('pre-propagate')
  await swarm.propagateWriters()
  console.log('propagate')

  await swarm.refresh() // announce
  await swarm.refresh() // connect
  await swarm.flush() // wait for dials
  await swarm.refresh() // dedupes

  const { replicator } = p1

  t.is(replicator.activeSessions.length, 2, 'active sessions')
  t.is(p1.replicator.connectionCount, 2, 'p1 connected to topic A & B')

  let [c1, c2] = replicator.connections
  if (!b4a.equals(c1.peerId, p2.replicator.peerId)) {
    [c1, c2] = [c2, c1]
  }

  t.is(toHex(c1.peerId), toHex(p2.replicator.peerId), 'P2 @ C1')
  t.is(toHex(c2.peerId), toHex(p3.replicator.peerId), 'P3 @ C2')
  t.is(c1.isIdle(), false)
  t.is(c2.isIdle(), false)
  t.is(c2.stream.destroyed, false)

  // de-prio topic B on first peer
  replicator.slots = 1
  passive.unpin()
  swarm.refresh().catch(t.fail) // apply

  t.is(p1.replicator.connectionCount, 2, 'p1 connected to topic A & B')

  t.is(c1.isIdle(), false)
  t.is(c2.isIdle(), true, 'idle connection detected')

  await swarm.flush()

  t.is(c2.stream.destroyed, true, 'garbage collected')

  await swarm.destroy()
})
