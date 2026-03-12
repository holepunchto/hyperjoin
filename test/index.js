const { test } = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')

test('basic replication', async (t) => {
  const { bootstrap } = await createTestnet()

  const store1 = new Corestore(await t.tmp())
  const store2 = new Corestore(await t.tmp())

  const rep1 = new Replicator({ bootstrap })
  const rep2 = new Replicator({ bootstrap })

  const core1 = store1.get({ name: 'test' })
  await core1.ready()

  await core1.append([b4a.from('hello'), b4a.from('world')])

  const core2 = store2.get({ key: core1.key })
  await core2.ready()

  await rep1.replicate(core1)

  // @todo wait to be fully announced
  await new Promise((res) => setTimeout(res, 1000))

  await rep2.replicate(core2)

  await new Promise((res) => setTimeout(res, 3000))

  console.log(core1, core2)
  await core2.update({ wait: true })

  t.is(core2.length, 2)
  t.alike(await core2.get(0), b4a.from('hello'))
  t.alike(await core2.get(1), b4a.from('world'))

  await rep1.close()
  await rep2.close()
})
