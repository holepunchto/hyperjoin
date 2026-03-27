const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const { joinRemote, onceEvent, seedA, seedB, spawnPeer } = require('./helpers')

test('suspend + resume', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const remote = await joinRemote(p2, p1)
  await remote[0].get(0)

  const p1Closed = onceEvent(p1.replicator, '_disconnect')
  const p2Closed = onceEvent(p2.replicator, '_disconnect')

  await p2.replicator.suspend()
  await Promise.all([p1Closed, p2Closed])

  t.is(p2.replicator.suspended, true)
  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  const reconnected = onceEvent(p1.replicator, '_connection')
  await p2.replicator.resume()
  await reconnected

  t.is(p2.replicator.suspended, false)

  await p1.cores[0].append(b4a.from('resume'))
  t.alike(await remote[0].get(2), b4a.from('resume'))

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
