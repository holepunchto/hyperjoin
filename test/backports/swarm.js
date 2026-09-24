const test = require('brittle')
const Corestore = require('corestore')
const b4a = require('b4a')
const {
  joinRemote,
  onceEvent,
  seed,
  spawnPeer,
  seedA,
  seedB,
  seedC,
  timeout,
  toHex,
  waitForEvents
} = require('../helpers/backports')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('../..')
const Peer = require('../../lib/peer')

async function joinPinned(replicator, store, topic) {
  const session = replicator.join(store, topic, { pin: true })
  await session.ready()
  await session.refresh()
  return session
}

async function joinDiscover(replicator, store, topic) {
  const session = replicator.join(store, topic, { discover: true })
  await session.ready()
  await session.refresh()
  return session
}

test('one server, one client - first connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const connected = onceEvent(p2.replicator, 'connected')

  p2.replicator.on('connected', (connection) => {
    t.pass('p2')
    connection.stream.end()
  })

  p1.replicator.on('connected', (connection) => {
    connection.stream.end()
  })

  await joinDiscover(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await connected

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('two servers - first connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const connection1Test = t.test('connection1')
  const connection2Test = t.test('connection2')

  connection1Test.plan(1)
  connection2Test.plan(1)

  p1.replicator.on('connected', (connection) => {
    connection1Test.pass('p1')
    connection.stream.end()
  })

  p2.replicator.on('connected', (connection) => {
    connection2Test.pass('p2')
    connection.stream.end()
  })

  const session = await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await session.ready()
  await p2.replicator.flush()

  await connection1Test
  await connection2Test

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, one client - single reconnect', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const serverReconnects = t.test('server reconnects')
  const clientReconnects = t.test('client reconnects')
  const firstClientConnect = new Promise((resolve) => p2.replicator.once('connected', resolve))

  serverReconnects.plan(1)
  clientReconnects.plan(1)

  let clientConnected = false
  let serverDisconnected = false

  p2.replicator.on('connected', () => {
    if (!clientConnected) {
      clientConnected = true
      return
    }

    clientReconnects.pass('client reconnected')
  })

  p1.replicator.on('connected', async (connection) => {
    if (!serverDisconnected) {
      serverDisconnected = true

      await firstClientConnect // 'connected' event in replicator has different semantics.
      t.ok(clientConnected)

      connection.stream.destroy()
      return
    }

    serverReconnects.pass('server reconnected')
  })

  const remote = await Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  await Promise.all(remote.map((core) => core.ready()))

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await remote[0].get(0)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, one client - maximum reconnects', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  let connections = 0
  p2.replicator.on('connected', (connection) => {
    connections++
    connection.stream.destroy()
  })

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)

  await timeout(Peer.backoffDuration(1) + 500)

  t.ok(connections > 1)
  t.ok(connections < 5)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, one client - banned peer does not reconnect', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  let connections = 0
  p2.replicator.on('connected', (connection) => {
    connections++
    connection.info.setBanned()
    connection.stream.destroy()
  })

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)

  await timeout(600)

  t.is(connections, 1)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('many peers on one discovery key converge on the source peer', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const peers = []
  const cnt = 10

  for (let i = 0; i < cnt; i++) {
    peers.push(await spawnPeer(t, { bootstrap, seed: seed(i + 1) }))
  }

  const discoveryKey = peers[0].cores[0].discoveryKey
  for (const peer of peers.slice(1)) {
    await joinDiscover(peer.replicator, peer.store.session(), discoveryKey)
  }

  await timeout(1000)

  t.is(peers[0].replicator.connectionCount, cnt - 1)

  for (const peer of peers.slice(1)) {
    t.is(peer.replicator.connectionCount, 1)
  }

  for (const peer of peers) {
    await peer.destroy()
  }

  await testnet.destroy()
})

test('two servers, two clients - simple deduplication', async (t) => {
  const connection1Test = t.test('connection1')
  const connection2Test = t.test('connection2')

  connection1Test.plan(1)
  connection2Test.plan(1)

  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  p1.replicator.on('connected', () => {
    connection1Test.pass('p1 connection')
  })

  p2.replicator.on('connected', () => {
    connection2Test.pass('p2 connection')
  })

  const session = await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await session.ready()
  await p1.replicator.flush()
  await p2.replicator.flush()

  await connection1Test
  await connection2Test

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, two clients - topic multiplexing', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  let clientConnections = 0
  let peerInfo = null

  p2.replicator.on('connected', (connection) => {
    clientConnections++
    peerInfo = connection.info
  })

  await joinPinned(p1.replicator, p1.session, p1.cores[1].discoveryKey)
  await p1.replicator.flush()

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await joinPinned(p2.replicator, p2.store.session(), p1.cores[1].discoveryKey)

  await p2.replicator.flush()
  await p1.replicator.flush()

  t.is(clientConnections, 1)
  t.is(peerInfo.topics.length, 2)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, two clients - first connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  const connection1To2Test = t.test('connection 1 to 2')
  const connection1To3Test = t.test('connection 1 to 3')
  const connection2Test = t.test('connection2')
  const connection3Test = t.test('connection3')

  connection1To2Test.plan(1)
  connection1To3Test.plan(1)
  connection2Test.plan(1)
  connection3Test.plan(1)

  p1.replicator.on('connected', (connection) => {
    const peerId = toHex(connection.peerId)
    if (peerId === p2.peerId) {
      connection1To2Test.pass('p1 connected with p2')
    } else if (peerId === p3.peerId) {
      connection1To3Test.pass('p1 connected with p3')
    } else {
      t.fail('Unexpected connection')
    }
  })

  p2.replicator.on('connected', (connection) => {
    connection2Test.ok(toHex(connection.peerId) === p1.peerId, 'p2 connected with p1')
  })

  p3.replicator.on('connected', (connection) => {
    connection3Test.ok(toHex(connection.peerId) === p1.peerId, 'p3 connected with p1')
  })

  await joinDiscover(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await joinDiscover(p3.replicator, p3.store.session(), p1.cores[0].discoveryKey)

  await connection1To2Test
  await connection1To3Test
  await connection2Test
  await connection3Test

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('GC dedupes connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xa) })
  const p2 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xb) })

  await Promise.all([...p1.replicator.sessions.values()].map((s) => s.ready()))
  await Promise.all([...p2.replicator.sessions.values()].map((s) => s.ready()))

  const s1 = [...p1.replicator.sessions.values()][0]
  const s2 = [...p2.replicator.sessions.values()][0]

  const p1Connections = waitForEvents(p1.replicator, 'connected', 2)
  const p2Connections = waitForEvents(p2.replicator, 'connected', 2)

  let peerInfo = p1.replicator.dialer.lookup(p2.replicator.peerId)
  p1.replicator._connect(peerInfo, s1.discoveryKey)
  peerInfo = p2.replicator.dialer.lookup(p1.replicator.peerId)
  p2.replicator._connect(peerInfo, s2.discoveryKey)

  await Promise.all([p1Connections, p2Connections])

  t.is(p1.replicator.connectionCount, 2)
  t.is(p2.replicator.connectionCount, 2)
  t.is(p1.replicator.inbound.size, 1)
  t.is(p1.replicator.outbound.size, 1)
  t.is(p2.replicator.inbound.size, 1)
  t.is(p2.replicator.outbound.size, 1)

  t.alike(
    Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId, p1.peerId],
    'duplicate p1'
  )
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId, p2.peerId],
    'duplicate p2'
  )

  await p1.replicator._gc()
  await p2.replicator._gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  t.alike(
    Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId],
    'de-duped'
  )
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId],
    'de-duped'
  )

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('duplicate incoming swaps', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xa) })
  const p2 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xb) })

  const remote = await joinRemote(p2, p1)
  await remote[0].get(0)

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  const incoming = new Promise((resolve) => {
    const onconnection = (socket) => {
      if (toHex(socket.remotePublicKey) !== p2.peerId) return
      p1.replicator.server.removeListener('connection', onconnection)
      resolve(socket)
    }

    p1.replicator.server.on('connection', onconnection)
  })

  const duplicate = p2.replicator.dht.connect(p1.replicator.peerId, {
    keyPair: p2.replicator.keyPair
  })

  const duplicateClosed = onceEvent(duplicate, 'close')

  await incoming

  t.pass('duplicate inbound reached p1 server')

  t.is(duplicate.destroyed, false, 'new connection swapped')
  duplicate.destroy()

  await duplicateClosed

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('triple in-bound connections inflight', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xa) })
  const p2 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xb) })

  const remote = await joinRemote(p2, p1)
  await remote[0].get(0)

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  const incoming = new Promise((resolve) => {
    const sockets = []

    const onconnection = (socket) => {
      if (toHex(socket.remotePublicKey) !== p2.peerId) return

      sockets.push(socket)
      if (sockets.length < 2) return

      p1.replicator.server.removeListener('connection', onconnection)
      resolve(sockets)
    }

    p1.replicator.server.on('connection', onconnection)
  })

  const second = p2.replicator.dht.connect(p1.replicator.peerId, {
    keyPair: p2.replicator.keyPair
  })

  const third = p2.replicator.dht.connect(p1.replicator.peerId, {
    keyPair: p2.replicator.keyPair
  })

  const secondClosed = onceEvent(second, 'close')
  const thirdClosed = onceEvent(third, 'close')

  await incoming

  t.pass('two duplicate inbounds reached p1 server')

  second.destroy()
  third.destroy()

  await Promise.all([secondClosed, thirdClosed])

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('one server, two clients - if a second client joins after the server leaves, they will not connect', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const p1RepSession = [...p1.replicator.sessions.values()][0]

  const remote2 = Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  const remote3 = Promise.all(p1.cores.map((core) => p3.session.get(core.key)))

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  const r2 = await remote2
  await Promise.all(r2.map((core) => core.ready()))
  await r2[0].get(0)

  await p1.session.close()
  await p1RepSession.close()

  await joinDiscover(p3.replicator, p3.store.session(), p1.cores[0].discoveryKey)
  const r3 = await remote3
  await Promise.all(r3.map((core) => core.ready()))

  t.is(p2.replicator.connectionCount, 1)
  t.is(p3.replicator.connectionCount, 0)

  await p2.destroy()
  await p3.destroy()
  await p1.destroy()
  await testnet.destroy()
})

test('two servers, one client - refreshing a peer discovery instance discovers new server', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC, jitter: 0 })

  let clientConnections = 0
  p3.replicator.on('connected', () => {
    clientConnections++
  })

  const session = p3.replicator.join(p3.store.session(), p1.cores[0].discoveryKey, {
    discover: true
  })
  await session.ready()
  await session.refresh()
  await p3.replicator.flush()

  t.is(clientConnections, 1)

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await p2.replicator.flush()
  t.is(clientConnections, 1)

  await session.discovery.refresh()
  await p3.replicator.flush()
  t.is(clientConnections, 2)

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('one server, one client - correct deduplication when a client connection is destroyed', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  let clientConnections = 0
  let serverConnections = 0
  let resolveClientReconnect = null
  let resolveServerReconnect = null

  const clientReconnect = new Promise((resolve) => {
    resolveClientReconnect = resolve
  })
  const serverReconnect = new Promise((resolve) => {
    resolveServerReconnect = resolve
  })

  p1.replicator.on('connected', () => {
    serverConnections++
    if (serverConnections === 2) resolveServerReconnect()
  })

  p2.replicator.on('connected', (connection) => {
    clientConnections++
    if (clientConnections === 2) resolveClientReconnect()
    if (clientConnections === 1) setTimeout(() => connection.stream.destroy(), 50)
  })

  const remote = await Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  await Promise.all(remote.map((core) => core.ready()))

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await remote[0].get(0)
  await Promise.all([clientReconnect, serverReconnect])

  t.is(clientConnections, 2)
  t.is(serverConnections, 2)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('flush when max connections reached', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, maxPeers: 1 })
  const p3 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xc), maxPeers: 1 })

  const remote2 = p1.cores.map((core) => p2.session.get(core.key))
  const remote3 = p1.cores.map((core) => p3.session.get(core.key))

  await Promise.all(remote2.map((core) => core.ready()))
  await Promise.all(remote3.map((core) => core.ready()))

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await joinPinned(p3.replicator, p3.store.session(), p1.cores[0].discoveryKey)

  await Promise.all([remote2[0].get(0), remote3[0].get(0)])
  await p2.replicator.flush()
  await p3.replicator.flush()

  t.pass('flush resolved')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('rejoining with different client/server opts refreshes', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const local = store.session()
  const one = local.get({ name: 'one' })
  await one.ready()
  await one.append([b4a.from('cells'), b4a.from('interlinked')])

  const session1 = replicator.join(local, one.discoveryKey, { discover: true })
  await session1.ready()
  await session1.refresh()
  await session1.flush()
  await session1.close()

  const session2 = replicator.join(local, one.discoveryKey, { pin: true })
  await session2.ready()
  await session2.refresh()

  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  p2.replicator.on('connected', () => {})
  const client = await joinPinned(p2.replicator, p2.store.session(), one.discoveryKey)
  await client.flush()
  await p2.replicator.flush()

  t.is(p2.replicator.connectionCount, 1)

  await replicator.close()
  await store.close()
  await p2.destroy()
  await testnet.destroy()
})

test('closing all discovery sessions clears topic state', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minPeers: 0, maxPeers: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const store2 = p1.store.session()
  const session1 = p1.replicator.sessions.get(toHex(p1.cores[0].discoveryKey))
  const session2 = await joinPinned(p1.replicator, store2, p1.cores[1].discoveryKey)

  const remote1 = p2.session.get(p1.cores[0].key)
  const remote2 = p2.session.get(p1.cores[1].key)

  await Promise.all([session1.ready(), session2.ready(), remote1.ready(), remote2.ready()])

  const client1 = await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  const client2 = await joinPinned(p2.replicator, p2.store.session(), p1.cores[1].discoveryKey)

  await Promise.all([client1.ready(), client2.ready(), remote1.get(0), remote2.get(0)])

  t.is(p1.replicator.sessions.size, 2)
  // TODO: Post v0.5 - orphan status should accellerate decay
  // t.is(p1.replicator.dialer.peers.size, 1)

  const closed1 = onceEvent(session1, 'close')
  const closed2 = onceEvent(session2, 'close')
  const disconnected = onceEvent(p1.replicator, 'disconnected')

  await Promise.all([p1.session.close(), store2.close(), closed1, closed2])
  await p1.replicator._gc()
  await disconnected

  t.is(p1.replicator.sessions.size, 0)
  // TODO: Post v0.5 - orphan status should accellerate decay
  // t.is(p1.replicator.dialer.peers.size, 0)

  await p2.destroy()
  await p1.destroy()
  await testnet.destroy()
})

// @deprecated v0.5
test.skip('peer-discovery object deleted when corresponding connection closes (server)', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const connected = t.test('connection')
  connected.plan(1)

  const otherConnected = t.test('connection')
  otherConnected.plan(1)

  p2.replicator.on('connected', () => {
    connected.pass('p2')
  })

  const connClosed = onceEvent(p1.replicator, 'disconnected')
  p1.replicator.on('connected', () => {
    otherConnected.pass('p1')
  })

  await p1.session.close()

  const client = await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await client.ready()
  await p2.replicator.flush()

  await connected
  await otherConnected

  t.is(p1.replicator.dialer.peers.size, 1)
  await p2.destroy()

  await connClosed

  t.is(p1.replicator.dialer.peers.size, 0, 'No peerInfo memory leak')

  await p1.destroy()
  await testnet.destroy()
})

test('no default error handler set when connection event is emitted', async (t) => {
  t.plan(2)

  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p1Connected = onceEvent(p1.replicator, 'connected')
  const p2Connected = onceEvent(p2.replicator, 'connected')

  p2.replicator.on('connected', (connection) => {
    // protomux/index:333 'we handle this in "close"'
    t.is(connection.stream.listeners('error').length, 1, 'no upstream error listeners')
    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  p1.replicator.on('connected', (connection) => {
    // protomux/index:333 'we handle this in "close"'
    t.is(connection.stream.listeners('error').length, 1, 'no upstream error listeners')
    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await p2.replicator.flush()
  await Promise.all([p1Connected, p2Connected])

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('topic and peer get unslabbed in PeerInfo', async (t) => {
  t.plan(2)

  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p1Connected = onceEvent(p1.replicator, 'connected')
  const p2Connected = onceEvent(p2.replicator, 'connected')
  p2.replicator.on('connected', (connection) => {
    const peer = Array.from(p2.replicator.dialer.peers.values())[0]
    const { publicKey } = peer

    t.is(publicKey.buffer.byteLength, 32, 'unslabbed publicKey in peerInfo')

    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  p1.replicator.on('connected', (connection) => {
    t.is(
      [...p1.replicator.dialer.peers.values()][0].publicKey.buffer.byteLength,
      32,
      'unslabbed publicKey in peerInfo'
    )

    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await p2.replicator.flush()
  await Promise.all([p1Connected, p2Connected])

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('port opt gets passed on to hyperdht', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, port: [10000, 10100] })

  t.alike(replicator.dht.io.portRange, [10000, 10100])

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('firewall for server connections results in peer ban', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  let firewallRan = false
  let disableFirewall = false

  function firewall() {
    firewallRan = true
    return !disableFirewall
  }

  const p1 = await spawnPeer(t, {
    bootstrap,
    seed: seedA,
    jitter: 0,
    firewall
  })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0
  })

  p2.replicator.on('connected', () => {
    t.fail('should not connect due to firewall and ban')
  })
  p1.replicator.on('connected', () => {
    t.fail('should not connect due to firewall and ban')
  })

  await p1.session.close()
  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)
  await timeout(500)

  t.is(firewallRan, true, 'sanity check')

  disableFirewall = true

  await timeout(2000)

  const topic2 = p2.cores[0].discoveryKey
  await p2.session.close()
  await joinPinned(p1.replicator, p1.store.session(), topic2)

  const topic3 = p1.cores[1].discoveryKey
  await p1.store.session().close()
  await joinPinned(p2.replicator, p2.store.session(), topic3)

  await timeout(500)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('ban stat and event', async (t) => {
  t.plan(3)

  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const firewall = () => true

  const p1 = await spawnPeer(t, {
    bootstrap,
    seed: seedA,
    jitter: 0
  })
  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    firewall,
    jitter: 0
  })

  p2.replicator.on('ban', (peerInfo, reason) => {
    t.alike(peerInfo.publicKey, p1.replicator.keyPair.publicKey)
    t.alike(reason, 'firewalled')
  })

  p2.replicator.on('connected', (connection) => {
    connection.stream.on('error', () => {})
  })
  p1.replicator.on('connected', (connection) => {
    connection.stream.on('error', () => {})
  })

  await p1.session.close()
  await joinPinned(p2.replicator, p2.store.session(), p1.cores[0].discoveryKey)

  await timeout(500)

  t.is(p2.replicator.stats.bannedPeers, 1)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
