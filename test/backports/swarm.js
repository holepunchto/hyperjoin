const test = require('brittle')
const b4a = require('b4a')
const Hyperjoin = require('../..')
const {
  joinRemote,
  onceEvent,
  seed,
  spawnPeer,
  seedA,
  seedB,
  seedC,
  timeout,
  waitForEvents
} = require('../helpers/backports')
const createTestnet = require('hyperdht/testnet')
const Peer = require('../../lib/peer')
const { toHex } = b4a

async function joinPinned(hyperjoin, topic) {
  const session = hyperjoin.join(topic, { pin: true })
  await session.ready()
  hyperjoin.refresh()
  await hyperjoin.flush()
  return session
}

async function joinDiscover(hyperjoin, topic) {
  const session = hyperjoin.scan(topic, { delay: 0 })
  await session.flush()
  await hyperjoin.flush()
  return session
}

test('one server, one client - first connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const connected = onceEvent(p2.hyperjoin, 'connected')

  p2.hyperjoin.once('connected', (connection) => {
    t.pass('p2')
    connection.stream.end()
  })

  p1.hyperjoin.once('connected', (connection) => {
    connection.stream.end()
  })

  await joinDiscover(p2.hyperjoin, p1.cores[0].discoveryKey)
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

  p1.hyperjoin.once('connected', (connection) => {
    connection1Test.pass('p1')
    connection.stream.end()
  })

  p2.hyperjoin.once('connected', (connection) => {
    connection2Test.pass('p2')
    connection.stream.end()
  })

  const session = await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await session.ready()
  await p2.hyperjoin.flush()

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
  const firstClientConnect = new Promise((resolve) => p2.hyperjoin.once('connected', resolve))

  serverReconnects.plan(1)
  clientReconnects.plan(1)

  let clientConnected = false
  let serverDisconnected = false

  p2.hyperjoin.on('connected', () => {
    if (!clientConnected) {
      clientConnected = true
      return
    }

    clientReconnects.pass('client reconnected')
  })

  p1.hyperjoin.on('connected', async (connection) => {
    if (!serverDisconnected) {
      serverDisconnected = true

      await firstClientConnect // 'connected' event in hyperjoin has different semantics.
      t.ok(clientConnected)

      connection.stream.destroy()
      return
    }

    serverReconnects.pass('server reconnected')
  })

  const remote = await Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  await Promise.all(remote.map((core) => core.ready()))

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
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
  p2.hyperjoin.on('connected', (connection) => {
    connections++
    connection.stream.destroy()
  })

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)

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
  p2.hyperjoin.on('connected', (connection) => {
    connections++
    connection.info.setBanned()
    connection.stream.destroy()
  })

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)

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
    await joinDiscover(peer.hyperjoin, discoveryKey)
  }

  await timeout(1000)

  t.is(peers[0].hyperjoin.connectionCount, cnt - 1)

  for (const peer of peers.slice(1)) {
    t.is(peer.hyperjoin.connectionCount, 1)
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

  p1.hyperjoin.on('connected', () => {
    connection1Test.pass('p1 connection')
  })

  p2.hyperjoin.on('connected', () => {
    connection2Test.pass('p2 connection')
  })

  const session = await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await session.ready()
  await p1.hyperjoin.flush()
  await p2.hyperjoin.flush()

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

  p2.hyperjoin.on('connected', (connection) => {
    clientConnections++
    peerInfo = connection.info
  })

  await joinPinned(p1.hyperjoin, p1.cores[1].discoveryKey)
  await p1.hyperjoin.flush()

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await joinPinned(p2.hyperjoin, p1.cores[1].discoveryKey)

  await p2.hyperjoin.flush()
  await p1.hyperjoin.flush()

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

  p1.hyperjoin.on('connected', (connection) => {
    const peerId = toHex(connection.peerId)
    if (peerId === p2.peerId) {
      connection1To2Test.pass('p1 connected with p2')
    } else if (peerId === p3.peerId) {
      connection1To3Test.pass('p1 connected with p3')
    } else {
      t.fail('Unexpected connection')
    }
  })

  p2.hyperjoin.on('connected', (connection) => {
    connection2Test.ok(toHex(connection.peerId) === p1.peerId, 'p2 connected with p1')
  })

  p3.hyperjoin.on('connected', (connection) => {
    connection3Test.ok(toHex(connection.peerId) === p1.peerId, 'p3 connected with p1')
  })

  await joinDiscover(p2.hyperjoin, p1.cores[0].discoveryKey)
  await joinDiscover(p3.hyperjoin, p1.cores[0].discoveryKey)

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

  await Promise.all([...p1.hyperjoin.sessions.values()].map((s) => s.ready()))
  await Promise.all([...p2.hyperjoin.sessions.values()].map((s) => s.ready()))

  const s1 = [...p1.hyperjoin.sessions.values()][0]
  const s2 = [...p2.hyperjoin.sessions.values()][0]

  const p1Connections = waitForEvents(p1.hyperjoin, 'connected', 2)
  const p2Connections = waitForEvents(p2.hyperjoin, 'connected', 2)

  let peerInfo = p1.hyperjoin.dialer.lookup(p2.hyperjoin.peerId)
  p1.hyperjoin._connect(peerInfo, s1.discoveryKey)
  peerInfo = p2.hyperjoin.dialer.lookup(p1.hyperjoin.peerId)
  p2.hyperjoin._connect(peerInfo, s2.discoveryKey)

  await Promise.all([p1Connections, p2Connections])

  t.is(p1.hyperjoin.connectionCount, 2)
  t.is(p2.hyperjoin.connectionCount, 2)
  t.is(p1.hyperjoin.inbound.size, 1)
  t.is(p1.hyperjoin.outbound.size, 1)
  t.is(p2.hyperjoin.inbound.size, 1)
  t.is(p2.hyperjoin.outbound.size, 1)

  t.alike(
    Array.from(p2.hyperjoin.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId, p1.peerId],
    'duplicate p1'
  )
  t.alike(
    Array.from(p1.hyperjoin.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId, p2.peerId],
    'duplicate p2'
  )

  await p1.hyperjoin._gc()
  await p2.hyperjoin._gc()

  t.is(p1.hyperjoin.connectionCount, 1)
  t.is(p2.hyperjoin.connectionCount, 1)

  t.alike(
    Array.from(p2.hyperjoin.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId],
    'de-duped'
  )
  t.alike(
    Array.from(p1.hyperjoin.connections, (connection) => toHex(connection.peerId)),
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

  t.is(p1.hyperjoin.connectionCount, 1)
  t.is(p2.hyperjoin.connectionCount, 1)

  const incoming = new Promise((resolve) => {
    const onconnection = (socket) => {
      if (toHex(socket.remotePublicKey) !== p2.peerId) return
      p1.hyperjoin.server.removeListener('connection', onconnection)
      resolve(socket)
    }

    p1.hyperjoin.server.on('connection', onconnection)
  })

  const duplicate = p2.hyperjoin.dht.connect(p1.hyperjoin.peerId, {
    keyPair: p2.hyperjoin.keyPair
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

  t.is(p1.hyperjoin.connectionCount, 1)
  t.is(p2.hyperjoin.connectionCount, 1)

  const incoming = new Promise((resolve) => {
    const sockets = []

    const onconnection = (socket) => {
      if (toHex(socket.remotePublicKey) !== p2.peerId) return

      sockets.push(socket)
      if (sockets.length < 2) return

      p1.hyperjoin.server.removeListener('connection', onconnection)
      resolve(sockets)
    }

    p1.hyperjoin.server.on('connection', onconnection)
  })

  const second = p2.hyperjoin.dht.connect(p1.hyperjoin.peerId, {
    keyPair: p2.hyperjoin.keyPair
  })

  const third = p2.hyperjoin.dht.connect(p1.hyperjoin.peerId, {
    keyPair: p2.hyperjoin.keyPair
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
  const p1RepSession = [...p1.hyperjoin.sessions.values()][0]

  const remote2 = Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  const remote3 = Promise.all(p1.cores.map((core) => p3.session.get(core.key)))

  await joinDiscover(p2.hyperjoin, p1.cores[0].discoveryKey)
  const r2 = await remote2
  await Promise.all(r2.map((core) => core.ready()))
  await r2[0].get(0)

  await p1.session.close()
  await p1RepSession.close()

  await joinDiscover(p3.hyperjoin, p1.cores[0].discoveryKey)
  const r3 = await remote3
  await Promise.all(r3.map((core) => core.ready()))

  t.is(p2.hyperjoin.connectionCount, 1)
  t.is(p3.hyperjoin.connectionCount, 0)

  await p2.destroy()
  await p3.destroy()
  await p1.destroy()
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

  p1.hyperjoin.on('connected', () => {
    serverConnections++
    if (serverConnections === 2) resolveServerReconnect()
  })

  p2.hyperjoin.on('connected', (connection) => {
    clientConnections++
    if (clientConnections === 2) resolveClientReconnect()
    if (clientConnections === 1) setTimeout(() => connection.stream.destroy(), 50)
  })

  const remote = await Promise.all(p1.cores.map((core) => p2.session.get(core.key)))
  await Promise.all(remote.map((core) => core.ready()))

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
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

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await joinPinned(p3.hyperjoin, p1.cores[0].discoveryKey)

  await Promise.all([remote2[0].get(0), remote3[0].get(0)])
  await p2.hyperjoin.flush()
  await p3.hyperjoin.flush()

  t.pass('flush resolved')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('no default error handler set when connection event is emitted', async (t) => {
  t.plan(2)

  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p1Connected = onceEvent(p1.hyperjoin, 'connected')
  const p2Connected = onceEvent(p2.hyperjoin, 'connected')

  p2.hyperjoin.on('connected', (connection) => {
    // protomux/index:333 'we handle this in "close"'
    t.is(connection.stream.listeners('error').length, 1, 'no upstream error listeners')
    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  p1.hyperjoin.on('connected', (connection) => {
    // protomux/index:333 'we handle this in "close"'
    t.is(connection.stream.listeners('error').length, 1, 'no upstream error listeners')
    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await p2.hyperjoin.flush()
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
  const p1Connected = onceEvent(p1.hyperjoin, 'connected')
  const p2Connected = onceEvent(p2.hyperjoin, 'connected')
  p2.hyperjoin.on('connected', (connection) => {
    const peer = Array.from(p2.hyperjoin.dialer.peers.values())[0]
    const { publicKey } = peer

    t.is(publicKey.buffer.byteLength, 32, 'unslabbed publicKey in peerInfo')

    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  p1.hyperjoin.on('connected', (connection) => {
    t.is(
      [...p1.hyperjoin.dialer.peers.values()][0].publicKey.buffer.byteLength,
      32,
      'unslabbed publicKey in peerInfo'
    )

    connection.stream.on('error', () => {})
    connection.stream.end()
  })

  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)
  await p2.hyperjoin.flush()
  await Promise.all([p1Connected, p2Connected])

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('port opt gets passed on to hyperdht', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    {
      bootstrap,
      port: [10000, 10100]
    }
  )
  await hyperjoin.ready()

  t.alike(hyperjoin.dht.io.portRange, [10000, 10100])

  await hyperjoin.close()
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

  p2.hyperjoin.on('connected', () => {
    t.fail('should not connect due to firewall and ban')
  })
  p1.hyperjoin.on('connected', () => {
    t.fail('should not connect due to firewall and ban')
  })

  await p1.session.close()
  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)

  await timeout(500)

  t.is(firewallRan, true, 'sanity check')

  disableFirewall = true

  await timeout(2000)

  const topic2 = p2.cores[0].discoveryKey
  await p2.session.close()
  await joinPinned(p1.hyperjoin, topic2)

  const topic3 = p1.cores[1].discoveryKey
  await p1.store.session().close()
  await joinPinned(p2.hyperjoin, topic3)

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

  p2.hyperjoin.on('ban', (peerInfo, reason) => {
    t.alike(peerInfo.publicKey, p1.hyperjoin.keyPair.publicKey)
    t.alike(reason, 'firewalled')
  })

  p2.hyperjoin.on('connected', (connection) => {
    connection.stream.on('error', () => {})
  })
  p1.hyperjoin.on('connected', (connection) => {
    connection.stream.on('error', () => {})
  })

  await p1.session.close()
  await joinPinned(p2.hyperjoin, p1.cores[0].discoveryKey)

  await timeout(500)

  t.is(p2.hyperjoin.stats.bannedPeers, 1)

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
