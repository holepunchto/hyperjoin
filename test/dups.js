const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { seed, spawnPeer, timeout, onceServerConnection } = require('./helpers')
const b4a = require('b4a')

test('many peers on one discovery key converge on the source peer', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const peers = []
  const cnt = 10

  for (let i = 0; i < cnt; i++) {
    peers.push(await spawnPeer(t, { bootstrap, seed: seed(i + 1) }))
  }

  const discoveryKey = peers[0].cores[0].discoveryKey
  for (const peer of peers.slice(1)) {
    peer.replicator.join(peer.store.session(), discoveryKey)
  }

  await timeout(1000)

  t.is(peers[0].replicator.connections.size, cnt - 1)

  for (const peer of peers.slice(1)) {
    t.is(peer.replicator.connections.size, 1)
  }

  for (const peer of peers) {
    await peer.destroy()
  }

  await testnet.destroy()
})

test('GC dedupes connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xa) })
  const p2 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xb) })

  await Promise.all([...p1.replicator.sessions.values()].map(s => s.ready()))
  await Promise.all([...p2.replicator.sessions.values()].map(s => s.ready()))

  const s1 = [...p1.replicator.sessions.values()][0]
  const s2 = [...p2.replicator.sessions.values()][0]

  const in1 = onceServerConnection(p1.replicator.server, p2.replicator.keyPair.publicKey)
  const in2 = onceServerConnection(p2.replicator.server, p1.replicator.keyPair.publicKey)

  s1._connectPeer({ publicKey: p2.replicator.keyPair.publicKey }, {})
  s2._connectPeer({ publicKey: p1.replicator.keyPair.publicKey }, {})

  await Promise.all([in1, in2])

  // assert duplicate state
  t.is(p1.replicator.connectionCount, 2)
  t.is(p2.replicator.connectionCount, 2)
  t.is(p1.replicator.inbound.size, 1)
  t.is(p1.replicator.outbound.size, 1)
  t.is(p2.replicator.inbound.size, 1)
  t.is(p2.replicator.outbound.size, 1)

  const extractKeys = (connections) => Array.from(connections).map(c => b4a.toString(c.peerId, 'hex'))
  t.alike(extractKeys(p2.replicator.connections()), [p1.peerId, p1.peerId], 'duplicate p1')
  t.alike(extractKeys(p1.replicator.connections()), [p2.peerId, p2.peerId], 'duplicate p2')

  await p1.replicator._gc()
  await p2.replicator._gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  t.alike(extractKeys(p2.replicator.connections()), [p1.peerId], 'de-duped')
  t.alike(extractKeys(p1.replicator.connections()), [p2.peerId], 'de-duped')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
