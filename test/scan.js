const { test } = require('brittle')
const createTestnet = require('hyperdht/testnet')
const {
  seedA,
  seedB,
  spawnPeer
} = require('./helpers/backports')

test('scan() finds peers without announcing', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  // announces
  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })

  // scans
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const rep = p2.replicator

  // create cores on p2
  const cores = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(cores.map((core) => core.ready()))

  const topic = p1.cores[0].discoveryKey
  const { announce, lookup } = rep.stats.dht

  await rep.scan(p2.session, topic, { delay: 0, timeout: 1000 })

  t.ok(rep.peers.size > 0, 'scan discovers peers')

  const peer = rep.peers.get(p1.peerId)
  t.ok(peer._outboundAttempts > 0, 'scan dials p1')

  t.is(rep.connectionCount, 1)
  t.is(p1.replicator.connectionCount, 1)
  t.is(cores[0].length, 2, 'replicated')

  t.is(p2.replicator.stats.dht.announce, announce, 'scan does not announce')
  t.ok(p2.replicator.stats.dht.lookup > lookup, 'scan looks up the topic')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('scan() can be externally expired', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  // announces
  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })

  // scans
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const rep = p2.replicator

  // create cores on p2
  const cores = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(cores.map((core) => core.ready()))

  const topic = p1.cores[0].discoveryKey
  const { announce, lookup } = rep.stats.dht

  await rep.scan(p2.session, topic, { delay: 0, timeout: 10000, updatedTimeout: 50 })

  // TODO: intuitive expire

  t.ok(rep.peers.size > 0, 'scan discovers peers')

  const peer = rep.peers.get(p1.peerId)
  t.ok(peer._outboundAttempts > 0, 'scan dials p1')

  t.is(rep.connectionCount, 1)
  t.is(p1.replicator.connectionCount, 1)
  t.is(cores[0].length, 2, 'replicated')

  t.is(p2.replicator.stats.dht.announce, announce, 'scan does not announce')
  t.ok(p2.replicator.stats.dht.lookup > lookup, 'scan looks up the topic')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
