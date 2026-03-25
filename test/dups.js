const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { seed, spawnPeer, timeout } = require('./helpers')

test('many peers on one discovery key converge on the source peer', async (t) => {
  t.timeout(15000)

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
