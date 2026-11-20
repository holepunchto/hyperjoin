const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { onceEvent, spawnPeer, seedA, seedB } = require('../helpers/backports')

test('connectionsOpened and connectionsClosed stats', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  const tOpen = t.test('Open connection')
  tOpen.plan(3)
  const tClose = t.test('Close connection')
  tClose.plan(4)

  const p1Connected = onceEvent(p1.hyperjoin, 'connected')
  const p2Connected = onceEvent(p2.hyperjoin, 'connected')

  p2.hyperjoin.once('connected', (connection) => {
    tOpen.is(p2.hyperjoin.stats.connects.client.opened, 1, 'opened connection is in stats')
    tOpen.is(p2.hyperjoin.stats.connects.client.attempted, 1, 'attemped connection is in stats')
    tClose.is(p2.hyperjoin.stats.connects.client.closed, 0, 'sanity check')

    connection.stream.on('close', () => {
      tClose.is(p2.hyperjoin.stats.connects.client.closed, 1, 'closed connection is in stats')
    })

    connection.stream.on('error', noop)
    connection.stream.end()
  })

  p1.hyperjoin.once('connected', (connection) => {
    tOpen.is(p1.hyperjoin.stats.connects.server.opened, 1, 'opened server connection is in stats')
    tClose.is(p1.hyperjoin.stats.connects.server.closed, 0, 'sanity check')

    connection.stream.on('close', () => {
      tClose.is(p1.hyperjoin.stats.connects.server.closed, 1, 'closed connections is in stats')
    })

    connection.stream.on('error', noop)
  })

  await p1.session.close()
  const session = p2.hyperjoin.join(p1.cores[0].discoveryKey, { pin: true })
  await session.ready()
  p2.hyperjoin.refresh()
  await p2.hyperjoin.flush()
  await Promise.all([p1Connected, p2Connected])

  await tClose
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

function noop() {}
