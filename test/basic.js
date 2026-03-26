const { test } = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')
const { joinRemote, onceEvent, seedA, seedB, seedC, spawnPeer, toHex } = require('./helpers')

test('Default keypair from corestore', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })

  await replicator.ready()

  t.is(
    toHex(replicator.peerId),
    '7dda4c84e7061170c5100be5d4c5cce9bb05ff2e1bd20d80d83408a10a6e73cc'
  )

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('Basic replication', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const remote = await joinRemote(p2, p1)
  const val = await remote[0].get(0)

  t.is(val.toString(), 'cells')
  t.is(remote[0].length, 2)
  t.is(remote[1].length, 2)
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)
  t.alike(Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)), [p1.peerId], 'p2 sees p1')
  t.alike(Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)), [p2.peerId], 'p1 sees p2')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('GC connections with no active mux sessions', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, lowWatermark: 1, highWatermark: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  const remote = await joinRemote(p2, p1)
  await remote[1].get(1)

  const p2Key = toHex(p2.replicator.peerId)
  const p3Key = toHex(p3.replicator.peerId)

  const p3Connected = onceEvent(p1.replicator, 'connection')
  p3.replicator.join(p3.store.session(), p1.cores[0].discoveryKey)
  await p3Connected

  t.is(p1.replicator.connectionCount, 2, 'p1 connected to both peers')
  t.ok(
    p1.replicator.getConnection(p2Key)._protomuxSessions() > 0,
    'p2 has open core session to p1'
  )
  t.is(p1.replicator.hasConnection(p3Key), true, 'p3 should connect to p1')
  t.is(
    p1.replicator.getConnection(p3Key)._protomuxSessions(),
    0,
    'p3 connection has no protomux sessions'
  )

  await p1.replicator._gc(() => Date.now())

  t.is(
    p1.replicator.connectionCount,
    1,
    'gc should evict the connection without active protmux sessions'
  )
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2Key],
    'gc keeps the connection carrying opened local cores'
  )

  await p1.cores[0].append(b4a.from('gc-kept'))
  t.alike(await remote[0].get(2), b4a.from('gc-kept'), 'replication survives gc on the kept peer')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test.skip('Sessions are garbage collected', async () => {})
test.skip('sessions are garbage collected & reactivated', async () => {})
