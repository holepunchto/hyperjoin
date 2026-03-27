const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { joinRemote, onceEvent, seed, spawnPeer, timeout, toHex, waitForEvents } = require('./helpers')

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
    peer.replicator.join(peer.store.session(), discoveryKey)
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

test('GC dedupes connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xa) })
  const p2 = await spawnPeer(t, { bootstrap, seed: Buffer.alloc(32, 0xb) })

  await Promise.all([...p1.replicator.sessions.values()].map(s => s.ready()))
  await Promise.all([...p2.replicator.sessions.values()].map(s => s.ready()))

  const s1 = [...p1.replicator.sessions.values()][0]
  const s2 = [...p2.replicator.sessions.values()][0]

  const p1Connections = waitForEvents(p1.replicator, 'connection', 2)
  const p2Connections = waitForEvents(p2.replicator, 'connection', 2)

  // Force inflight cross-connection
  p1.replicator._connectPeer(s1.discoveryKey, { publicKey: p2.replicator.peerId }, {})
  p2.replicator._connectPeer(s2.discoveryKey, { publicKey: p1.replicator.peerId }, {})

  await Promise.all([p1Connections, p2Connections])

  // assert duplicate state
  t.is(p1.replicator.connectionCount, 2)
  t.is(p2.replicator.connectionCount, 2)
  t.is(p1.replicator.inbound.size, 1)
  t.is(p1.replicator.outbound.size, 1)
  t.is(p2.replicator.inbound.size, 1)
  t.is(p2.replicator.outbound.size, 1)

  t.alike(Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)), [p1.peerId, p1.peerId], 'duplicate p1')
  t.alike(Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)), [p2.peerId, p2.peerId], 'duplicate p2')

  await p1.replicator._gc()
  await p2.replicator._gc()

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  t.alike(Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)), [p1.peerId], 'de-duped')
  t.alike(Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)), [p2.peerId], 'de-duped')

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

  duplicate.on('error', console.error.bind(null, 'BOKB'))
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

function noop() {}
