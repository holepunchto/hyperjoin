const { test } = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')
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

  const op = rep.scan(topic, { delay: 0, timeout: 1000 })
  await op.complete

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

test('scan().connected resolves when a peer is acquired', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const cores = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(cores.map((core) => core.ready()))

  const op = p2.replicator.scan(p1.cores[0].discoveryKey, { delay: 0 })

  t.ok(op.connected instanceof Promise, 'scan has a connected promise')

  if (op.connected instanceof Promise) {
    await op.connected
    t.is(p2.replicator.connectionCount, 1, 'scan connected to the topic peer')
  }

  op.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('scan() deduplicates active scans by topic', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(() => {}, () => null, { bootstrap, manual: true })
  const topic = b4a.alloc(32, 1)

  await replicator.ready()

  const first = replicator.scan(topic, { delay: 10000 })
  const second = replicator.scan(topic)

  t.is(second, first, 'returns the active scan')
  t.is(replicator.scans.size, 1, 'only one scan is active')

  first.destroy()
  await replicator.close()
  await testnet.destroy()
})

test('scan() forwards the peer limit', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const peer = await spawnPeer(t, { bootstrap, seed: seedA })

  const limited = peer.replicator.scan(b4a.alloc(32, 42), { limit: 1 })
  const unlimited = peer.replicator.scan(b4a.alloc(32, 43), { limit: Infinity })

  await Promise.all([limited.flush(), unlimited.flush()])

  t.is(limited.discovery.limit, 1)
  t.is(unlimited.discovery.limit, Infinity)

  await peer.destroy()
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

  let connected = false
  const op = rep.scan(topic, { delay: 0, timeout: 10000 })
  op.connected.then(() => {
    connected = true
    op.expire(50)
  })
  await op.complete

  t.ok(rep.peers.size > 0, 'scan discovers peers')

  const peer = rep.peers.get(p1.peerId)
  t.ok(peer._outboundAttempts > 0, 'scan dials p1')

  t.is(rep.connectionCount, 1)
  t.is(p1.replicator.connectionCount, 1)
  t.is(cores[0].length, 2, 'replicated')
  t.ok(connected, 'scan expires after connecting')

  t.is(p2.replicator.stats.dht.announce, announce, 'scan does not announce')
  t.ok(p2.replicator.stats.dht.lookup > lookup, 'scan looks up the topic')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('scan() resumes after suspension', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const rep = p2.replicator

  const cores = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(cores.map((core) => core.ready()))

  const topic = p1.cores[0].discoveryKey
  const op = rep.scan(topic, { delay: 0, timeout: 60000 })

  await rep.flush()

  const { lookup } = rep.stats.dht

  await rep.suspend()
  t.ok(rep.scans.size > 0, 'scan remains active while suspended')

  await rep.resume()
  await rep.flush()

  t.ok(rep.stats.dht.lookup > lookup, 'scan looks up after resume')

  await p1.destroy()
  await p2.destroy()
  await op.complete
  await testnet.destroy()
})
