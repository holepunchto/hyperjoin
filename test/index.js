const { test } = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')

const seedA = Buffer.alloc(32, 0xa)
const seedB = Buffer.alloc(32, 0xb)
const seedC = Buffer.alloc(32, 0xc)

test('Default keypair from corestore', async (t) => {
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const rep = new Replicator(store)
  await rep.ready()
  t.is(
    rep.keyPair.publicKey.toString('hex'),
    '7dda4c84e7061170c5100be5d4c5cce9bb05ff2e1bd20d80d83408a10a6e73cc'
  )
  await store.close()
  await rep.close()
})

test('Basic replication', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const remote = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  const rstore = p2.store.session()
  const discoveryKey = p1.cores[0].discoveryKey
  p2.replicator.join(rstore, discoveryKey)

  const val = await remote[0].get(0)
  t.is(val.toString(), 'cells')

  t.is(remote[0].length, 2)
  t.is(remote[1].length, 2)

  t.is(p1.replicator.connections.size, 1)
  t.is(p2.replicator.connections.size, 1)

  t.alike(Array.from(p2.replicator.connections.keys()), [p1.peerId], 'p2 sees p1')
  t.alike(Array.from(p1.replicator.connections.keys()), [p2.peerId], 'p1 sees p2')

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

  const remote = p1.cores.map((core) => p2.session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  const discoveryKey = p1.cores[0].discoveryKey
  p2.replicator.join(p2.store.session(), discoveryKey)

  await remote[1].get(1)

  const p2Key = b4a.toString(p2.replicator.keyPair.publicKey, 'hex')
  const p3Key = b4a.toString(p3.replicator.keyPair.publicKey, 'hex')

  const p3Connected = serverConnection(p1.replicator.server, p3.replicator.keyPair.publicKey)
  p3.replicator.join(p3.store.session(), discoveryKey)
  await p3Connected

  t.is(p1.replicator.connections.size, 2, 'p1 connected to both peers')
  t.ok(
    p1.replicator.connections.get(p2Key)._protomuxSessions() > 0,
    'p2 has open core session to p1'
  )
  t.is(p1.replicator.connections.has(p3Key), true, 'p3 should connect to p1')

  t.is(
    p1.replicator.connections.get(p3Key)._protomuxSessions(),
    0,
    'p3 connection has no protomux sessions'
  )

  await p1.replicator._gc(() => Date.now())

  t.is(
    p1.replicator.connections.size,
    1,
    'gc should evict the connection without active protmux sessions'
  )
  t.alike(
    Array.from(p1.replicator.connections.keys()),
    [p2Key],
    'gc keeps the connection carrying opened local cores'
  )

  await p1.cores[0].append(b4a.from('gc-kept'))
  t.alike(await remote[0].get(2), b4a.from('gc-kept'), 'replication survives gc on the kept peer')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()

  function serverConnection(server, publicKey) {
    return new Promise((resolve) => {
      const onconnection = (socket) => {
        if (!b4a.equals(socket.remotePublicKey, publicKey)) return
        server.removeListener('connection', onconnection)
        resolve()
      }

      server.on('connection', onconnection)
    })
  }
})

// V1
test.skip('Sessions are garbage collected', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store1 = new Corestore(await t.tmp())
  const store2 = new Corestore(await t.tmp())
  const rep1 = new Replicator(store1, { bootstrap, seed: seedA })
  const rep2 = new Replicator(store2, { bootstrap, seed: seedB })

  await rep1.ready()
  await rep2.ready()

  const core1 = store1.get({ name: 'test' })
  await core1.ready()

  await core1.append([b4a.from('hello'), b4a.from('world')])

  // wait for rep1 to announce
  await new Promise((res) => setTimeout(res, 1000))

  const core2 = store2.get({ key: core1.key })
  await core2.ready()
  core2.on('append', () => t.comment('core2 len:', core2.length))

  await core2.get(1)

  t.is(rep2.connections, 1, 'one connection')
  await rep2._gc(() => Date.now() + 30_001)
  t.is(rep2.connections, 0, 'no-connections')

  await rep1.close()
  await rep2.close()
  await store1.close()
  await store2.close()
  await testnet.destroy()
})

// V2
test.skip('sessions are garbage collected & reactivated', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store1 = new Corestore(await t.tmp())
  const store2 = new Corestore(await t.tmp())
  const rep1 = new Replicator(store1, { bootstrap, seed: seedA })
  const rep2 = new Replicator(store2, { bootstrap, seed: seedB })

  await rep1.ready()
  await rep2.ready()

  const core1 = store1.get({ name: 'test' })
  await core1.ready()

  await core1.append([b4a.from('hello'), b4a.from('world')])

  // wait for rep1 to announce
  await new Promise((res) => setTimeout(res, 1000))

  const core2 = store2.get({ key: core1.key })
  await core2.ready()
  core2.on('append', () => t.comment('core2 len:', core2.length))

  await core2.get(1)

  t.is(rep2.connections, 1, 'one connection')
  await rep2._gc(() => Date.now() + 30_001)
  t.is(rep2.connections, 0, 'no-connections')

  await core1.append(b4a.from('restart'))

  // wait for rep1 to reannounce
  await new Promise((res) => setTimeout(res, 1000))

  // trigger dht.lookup()
  await rep2._gc(() => Date.now() + 10_000)

  t.is(rep2.connections, 1, 'reactivated from dht')
  t.is(core2.length, 3)

  await rep1.close()
  await rep2.close()
  await store1.close()
  await store2.close()
  await testnet.destroy()
})

async function spawnPeer(t, { seed, bootstrap, lowWatermark, highWatermark } = {}) {
  let storeOpts = {}
  if (seed) storeOpts = { primaryKey: seed, unsafe: true }

  const store = new Corestore(await t.tmp(), storeOpts)

  const replicator = new Replicator(store, { bootstrap, lowWatermark, highWatermark, manual: true })
  await replicator.ready()

  const session = store.session()

  const one = session.get({ name: 'one' })
  await one.ready()
  one.append([b4a.from('cells'), b4a.from('interlinked')])

  replicator.join(session, one.discoveryKey).addLocal(one)

  const two = session.get({ name: 'two' })
  await two.ready()
  two.append([b4a.from('within'), b4a.from('Cells')])

  const destroy = async () => {
    await replicator.close()
    await store.close()
  }

  const peerId = b4a.toString(replicator.keyPair.publicKey, 'hex')

  return { store, replicator, session, peerId, destroy, cores: [one, two] }
}
