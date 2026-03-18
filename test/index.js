const { test } = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')

test('basic replication', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store1 = new Corestore(await t.tmp())
  const store2 = new Corestore(await t.tmp())
  const rep1 = new Replicator(store1, { bootstrap })
  const rep2 = new Replicator(store2, { bootstrap })

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

  await rep1.close()
  await rep2.close()
  await store1.close()
  await store2.close()
  await testnet.destroy()
})
