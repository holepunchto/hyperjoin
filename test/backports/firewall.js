const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { timeout, spawnPeer, seedA, seedB } = require('../helpers/backports')

test('firewalled server - bad client is rejected', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: (remotePublicKey) => remotePublicKey.equals(p1.hyperjoin.keyPair.publicKey)
  })

  let serverConnections = 0
  p2.hyperjoin.on('connected', () => serverConnections++)

  const server = [...p2.hyperjoin.sessions.values()][0]
  await server.flush()

  const client = p1.hyperjoin.scan(p2.cores[0].discoveryKey, { delay: 0 })
  await client.flush()
  await p1.hyperjoin.flush()
  await new Promise((resolve) => setImmediate(resolve))

  t.alike(serverConnections, 0, 'server did not receive an incoming connection')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('firewalled client - bad server is rejected', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  t.plan(2)

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: (remotePublicKey) => {
      const firewalled = remotePublicKey.equals(p1.hyperjoin.keyPair.publicKey)
      t.ok(firewalled, 'The peer got firewalled')
      return firewalled
    }
  })

  let clientConnections = 0
  p2.hyperjoin.on('connected', () => clientConnections++)

  const client = p2.hyperjoin.scan(p1.cores[0].discoveryKey, { delay: 0 })
  await client.flush()
  await p2.hyperjoin.flush()
  await new Promise((resolve) => setImmediate(resolve))

  t.alike(clientConnections, 0, 'client did not receive an incoming connection')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('async firewall - client allows good server', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: () => false
  })

  const connected = new Promise((resolve) => p2.hyperjoin.once('connected', resolve))

  const client = p2.hyperjoin.scan(p1.cores[0].discoveryKey, { delay: 0 })
  await client.flush()
  await p2.hyperjoin.flush()
  await connected

  t.alike(p2.hyperjoin.connectionCount, 1, 'client connected')
  t.alike(p2.hyperjoin.stats.bannedPeers, 0, 'client did not ban good server')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('async firewall - server allows good client', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: () => false
  })

  const server = [...p2.hyperjoin.sessions.values()][0]
  await server.flush()

  const accepted = new Promise((resolve) => p2.hyperjoin.once('connected', resolve))
  const client = p1.hyperjoin.scan(p2.cores[0].discoveryKey, { delay: 0 })
  await client.flush()
  await p1.hyperjoin.flush()
  const connected = await Promise.race([accepted, timeout(1000).then(() => false)])

  t.ok(connected, 'server accepted client')
  t.alike(p2.hyperjoin.stats.bannedPeers, 0, 'server did not ban good client')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('firewalled server - rejection does not trigger retry cascade', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })

  let firewallCalls = 0
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: (remotePublicKey) => {
      firewallCalls++
      return remotePublicKey.equals(p1.hyperjoin.keyPair.publicKey)
    }
  })

  let serverConnections = 0
  p2.hyperjoin.on('connected', () => serverConnections++)

  const server = [...p2.hyperjoin.sessions.values()][0]
  await server.flush()

  const client = p1.hyperjoin.scan(p2.cores[0].discoveryKey, { delay: 0 })
  await client.flush()

  await timeout(1500)

  t.alike(serverConnections, 0, 'server did not receive an incoming connection')
  t.alike(firewallCalls, 1, 'client retried mulitple times but server cached it')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
