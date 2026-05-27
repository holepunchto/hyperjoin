const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { timeout, spawnPeer, seedA, seedB } = require('../helpers/backports')

const BACKOFFS = [100, 200, 300, 400]

test('firewalled server - bad client is rejected', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, backoffs: BACKOFFS, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    backoffs: BACKOFFS,
    jitter: 0,
    firewall: (remotePublicKey) => remotePublicKey.equals(p1.replicator.keyPair.publicKey)
  })

  let serverConnections = 0
  p2.replicator.on('connected', () => serverConnections++)

  const server = [...p2.replicator.sessions.values()][0]
  await server.flush()

  const client = p1.replicator.join(p1.store.session(), p2.cores[0].discoveryKey, {
    discover: true
  })
  await client.flush()
  await p1.replicator.flush()
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

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, backoffs: BACKOFFS, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    backoffs: BACKOFFS,
    jitter: 0,
    firewall: (remotePublicKey) => {
      const firewalled = remotePublicKey.equals(p1.replicator.keyPair.publicKey)
      t.ok(firewalled, 'The peer got firewalled')
      return firewalled
    }
  })

  let clientConnections = 0
  p2.replicator.on('connected', () => clientConnections++)

  const client = p2.replicator.join(p2.store.session(), p1.cores[0].discoveryKey, {
    discover: true
  })
  await client.flush()
  await p2.replicator.flush()
  await new Promise((resolve) => setImmediate(resolve))

  t.alike(clientConnections, 0, 'client did not receive an incoming connection')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('async firewall - client allows good server', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, backoffs: BACKOFFS, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    backoffs: BACKOFFS,
    jitter: 0,
    firewall: async () => false
  })

  const connected = new Promise((resolve) => p2.replicator.once('connected', resolve))

  p2.replicator.join(p2.store.session(), p1.cores[0].discoveryKey, {
    discover: true
  })

  await p2.replicator.flush()
  await connected

  t.alike(p2.replicator.connectionCount, 1, 'client connected')
  t.alike(p2.replicator.stats.bannedPeers, 0, 'client did not ban good server')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('async firewall - server allows good client', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, backoffs: BACKOFFS, jitter: 0 })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    backoffs: BACKOFFS,
    jitter: 0,
    firewall: async () => false
  })

  const server = [...p2.replicator.sessions.values()][0]
  await server.flush()

  const accepted = new Promise((resolve) => p2.replicator.once('connected', resolve))
  const client = p1.replicator.join(p1.store.session(), p2.cores[0].discoveryKey, {
    discover: true
  })

  await client.flush()
  await p1.replicator.flush()
  const connected = await Promise.race([accepted, timeout(1000).then(() => false)])

  t.ok(connected, 'server accepted client')
  t.alike(p2.replicator.stats.bannedPeers, 0, 'server did not ban good client')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('firewalled server - rejection does not trigger retry cascade', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, backoffs: BACKOFFS, jitter: 0 })

  let firewallCalls = 0
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    backoffs: BACKOFFS,
    jitter: 0,
    firewall: (remotePublicKey) => {
      firewallCalls++
      return remotePublicKey.equals(p1.replicator.keyPair.publicKey)
    }
  })

  let serverConnections = 0
  p2.replicator.on('connected', () => serverConnections++)

  const server = [...p2.replicator.sessions.values()][0]
  await server.flush()

  p1.replicator.join(p1.store.session(), p2.cores[0].discoveryKey, { discover: true })

  await timeout(BACKOFFS[2] * 5)

  t.alike(serverConnections, 0, 'server did not receive an incoming connection')
  t.alike(firewallCalls, 1, 'client retried mulitple times but server cached it')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
