const test = require('brittle')
const b4a = require('b4a')
const { spawnSwarm } = require('./helpers')

test('DHT Stats', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)
  await p1.join(topic, { pin: true, deferSwarming: false })
  await p2.join(topic, { pin: false, discover: true, deferSwarming: false })

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.refresh()
  await swarm.flush()

  t.alike(p1.replicator.stats.dht, { lookup: 0, announce: 1, unannounce: 0 })
  t.alike(p2.replicator.stats.dht, { lookup: 1, announce: 0, unannounce: 0 })

  await swarm.destroy()

  t.alike(p1.replicator.stats.dht, { lookup: 0, announce: 1, unannounce: 1 })
  t.alike(p2.replicator.stats.dht, { lookup: 1, announce: 0, unannounce: 0 })
})
