const { test } = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')

const seedA = Buffer.alloc(32, 0xa)
const seedB = Buffer.alloc(32, 0xb)

test('basic replication', async (t) => {
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
  core2.on('append', () => t.comment('blocks replicated', core2.length))
  await core2.ready()

  await core2.get(1)

  t.is(core2.length, 2)
  t.alike(await core2.get(0), b4a.from('hello'))
  t.alike(await core2.get(1), b4a.from('world'))

  t.is(rep1.connections, 1)
  t.is(rep2.connections, 1)

  t.alike(
    // rep1.keyPair.publicKey
    ['43a72e714401762df66b68c26dfbdf2682aaec9f2474eca4613e424a0fbafd3c'],
    Array.from(rep2.sessions.keys()),
    'Rep1 sees public key of Rep2'
  )
  t.alike(
    // rep2.keyPair.publicKey
    ['66be7e332c7a453332bd9d0a7f7db055f5c5ef1a06ada66d98b39fb6810c473a'],
    Array.from(rep1.sessions.keys()),
    'Rep2 sees public key of Rep1'
  )

  await rep1.close()
  await rep2.close()
  await store1.close()
  await store2.close()
  await testnet.destroy()
})

test('sessions are garbage collected & reactivated', async (t) => {
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
