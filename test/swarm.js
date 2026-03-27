const test = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')

const Replicator = require('..')
const { joinRemote, onceEvent, seedA, seedB, spawnPeer } = require('./helpers')

test('one server, one client - first connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const connected = onceEvent(p1.replicator, 'connection')
  const remote = await joinRemote(p2, p1)
  const block = await Promise.all([remote[0].get(0), connected])

  t.alike(block[0], b4a.from('cells'))
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, two discovery keys - connection reuse', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  p1.replicator.join(p1.session, p1.cores[1].discoveryKey).addLocal(p1.cores[1])

  const remote = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  p2.replicator.join(p2.store.session(), p1.cores[0].discoveryKey)
  p2.replicator.join(p2.store.session(), p1.cores[1].discoveryKey)

  t.alike(await remote[0].get(0), b4a.from('cells'))
  t.alike(await remote[1].get(0), b4a.from('within'))
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('port opt gets passed on to hyperdht', async (t) => {
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { manual: true, port: [10000, 10100] })

  t.alike(replicator.dht.io.portRange, [10000, 10100])

  await replicator.close()
  await store.close()
})
