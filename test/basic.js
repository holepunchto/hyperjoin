const { test } = require('brittle')
const EventEmitter = require('bare-events')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const DHT = require('hyperdht')
const Replicator = require('..')
const {
  joinRemote,
  onceEvent,
  seed,
  seedA,
  seedB,
  seedC,
  spawnPeer,
  timeout
} = require('./helpers/backports')

const { toHex } = b4a

test('default keypair from corestore', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })

  await replicator.ready()

  t.is(toHex(replicator.peerId), '7dda4c84e7061170c5100be5d4c5cce9bb05ff2e1bd20d80d83408a10a6e73cc')

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('basic replication', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const remote = await joinRemote(p2, p1)
  const val = await remote[0].get(0)

  t.is(val.toString(), 'cells')
  t.is(remote[0].length, 2)
  t.is(remote[1].length, 2)
  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)
  t.alike(
    Array.from(p2.replicator.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId],
    'p2 sees p1'
  )
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId],
    'p1 sees p2'
  )

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('gc connections with no active mux sessions', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minPeers: 1, maxPeers: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  const remote = await joinRemote(p2, p1)
  await remote[1].get(1)

  const p2Key = toHex(p2.replicator.peerId)
  const p3Key = toHex(p3.replicator.peerId)

  const p3Connected = onceEvent(p1.replicator, 'connected')
  const p3Session = p3.replicator.join(p3.store.session(), p1.cores[0].discoveryKey, {
    discover: true
  })
  await p3Session.ready()
  await p3Session.refresh()
  await p3Connected

  t.is(p1.replicator.connectionCount, 2, 'p1 connected to both peers')
  t.ok(p1.replicator.getConnection(p2Key)._protomuxSessions() > 0, 'p2 has open core session to p1')
  t.is(p1.replicator.hasConnection(p3Key), true, 'p3 should connect to p1')
  t.is(
    p1.replicator.getConnection(p3Key)._protomuxSessions(),
    0,
    'p3 connection has no protomux sessions'
  )

  await p1.replicator._gc()

  t.is(
    p1.replicator.connectionCount,
    1,
    'gc should evict the connection without active protmux sessions'
  )
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2Key],
    'gc keeps the connection carrying opened local cores'
  )

  await p1.cores[0].append(b4a.from('gc-kept'))
  t.alike(await remote[0].get(2), b4a.from('gc-kept'), 'replication survives gc on the kept peer')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('parent corestore doesnt activate core on namespace for session', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const session = p1.replicator.activeSessions[0]
  const dkey = toHex(p1.cores[0].discoveryKey)
  t.ok(session.hasCore(dkey))

  const otherNamespace = p1.store.namespace('beep')
  const core = otherNamespace.get({ name: 'other' })
  await core.ready()
  t.absent(session.hasCore(toHex(core.discoveryKey)))

  const sessionNewCore = session.store.get({ name: 'three' })
  await sessionNewCore.ready()
  t.ok(session.hasCore(toHex(sessionNewCore.discoveryKey)))

  await p1.destroy()
  await testnet.destroy()
})

test('gc preserves stable connections over rookies', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, {
    bootstrap,
    seed: seedA,
    minPeers: 0,
    maxPeers: 1
  })
  const p2Connected = onceEvent(p1.replicator, 'connected')

  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const p2Key = toHex(p2.replicator.peerId)
  const p3Key = toHex(p3.replicator.peerId)

  await joinRemote(p2, p1)
  await p2Connected

  let active = Array.from(p1.replicator.connections)
  t.is(active.length, 1)

  const a = active[0]
  t.is(toHex(a.peerId), p2Key, 'p2 connected')

  t.is(a._survived, 0, 'not yet sweeped')

  await p1.replicator._gc()
  t.is(a._survived, 1, 'passed gc')

  const p3Connected = onceEvent(p1.replicator, 'connected').then(
    (connection) => connection.replicating
  )

  await joinRemote(p3, p1)
  await p3Connected

  active = Array.from(p1.replicator.connections)
  t.is(active.length, 2, 'over capacity')
  const b = active[1]

  t.is(toHex(a.peerId), p2Key, 'connection a = p2')
  t.is(toHex(b.peerId), p3Key, 'connection b = p3')
  t.ok(a._survived > b._survived, 'b will be dropped')

  await p1.replicator._gc()

  active = Array.from(p1.replicator.connections)
  t.is(active.length, 1, 'GC dropped overflow')
  t.is(toHex(a.peerId), p2Key, 'p2 remains connected')
  t.is(a._survived, 2, 'passed gc twice')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers firewalls new inbound at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1, refuseAtMaxPeers: true })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  await joinRemote(p2, p1)
  t.is(p1.replicator.connectionCount, 1, 'p1 at max peers')

  let opened = 0
  p1.replicator.server.on('connection', () => opened++)

  let replicated = 0
  const replicate = p1.store.replicate
  p1.store.replicate = function (...args) {
    replicated++
    return replicate.apply(this, args)
  }

  const err = await p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))

  t.is(err.code, 'PEER_CONNECTION_FAILED', 'p3 dial failed')
  t.is(p3.replicator.connectionCount, 0, 'p3 has no connection')
  t.is(opened, 0, 'p1 opened no connection')
  t.is(replicated, 0, 'p1 attached no cores')
  t.is(p1.replicator.stats.bannedPeers, 0, 'p1 banned no one')
  t.absent(p1.replicator.dialer.peers.has(p3.peerId), 'p1 did not track p3')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers accepts a reconnect at max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1, refuseAtMaxPeers: true })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  await joinRemote(p2, p1)
  const existing = p1.replicator.getConnection(p2.peerId)
  await existing.replicating
  t.ok(p1.replicator.inbound.has(p2.peerId), 'p2 dialed p1')

  // p2 reconnects without its end of the old link, as after a network change
  const connected = onceEvent(p1.replicator, 'connected')
  const stream = p2.replicator.dht.connect(p1.replicator.peerId, { keyPair: p2.replicator.keyPair })
  stream.on('error', () => {})

  const res = await Promise.race([connected.then(() => 'connected'), timeout(5000)])
  t.is(res, 'connected', 'p1 accepted the reconnect')
  t.is(existing.stream.destroyed, true, 'p1 closed the old connection')
  t.is(p1.replicator.connectionCount, 1, 'p1 at max peers')

  stream.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers accepts an inbound from a peer we dialed', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1, refuseAtMaxPeers: true })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  await joinRemote(p1, p2)
  t.ok(p1.replicator.outbound.has(p2.peerId), 'p1 dialed p2')

  // p2 reconnects without its end of the old link, as after a network change
  const connected = onceEvent(p1.replicator, 'connected')
  const stream = p2.replicator.dht.connect(p1.replicator.peerId, { keyPair: p2.replicator.keyPair })
  stream.on('error', () => {})

  const res = await Promise.race([connected.then(() => 'connected'), timeout(5000)])
  t.is(res, 'connected', 'p1 accepted the inbound')

  await p1.replicator._gc()
  t.is(p1.replicator.connectionCount, 1, 'gc kept one connection to p2')

  stream.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers accepts one inbound per idle connection', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1, refuseAtMaxPeers: true })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const p4 = await spawnPeer(t, { bootstrap, seed: seed(0xd) })

  await joinRemote(p2, p1)
  const idle = p1.replicator.getConnection(p2.peerId)
  await idle.replicating
  idle.lastActivity = Date.now() - 3 * 60 * 1000 - 1 // no traffic for 3 minutes

  const connected = onceEvent(p1.replicator, 'connected')
  await p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))
  await Promise.race([connected, timeout(5000)])

  t.is(p1.replicator.connectionCount, 2, 'p3 accepted over the cap')

  const err = await p4.replicator._connect(p4.replicator.dialer.lookup(p1.replicator.peerId))
  t.is(err.code, 'PEER_CONNECTION_FAILED', 'p4 refused, p3 took the idle slot')

  await p1.replicator._gc()

  t.is(idle.stream.destroyed, true, 'gc evicted the idle connection')
  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p3.peerId],
    'p3 remains connected'
  )

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await p4.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers closes inbound that passed the firewall over max peers', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  let release = null
  const released = new Promise((resolve) => {
    release = resolve
  })

  let pending = 0
  const p1 = await spawnPeer(t, {
    bootstrap,
    seed: seedA,
    maxPeers: 1,
    refuseAtMaxPeers: true,
    firewall: async () => {
      if (++pending === 2) release() // both handshakes are past the max peers check
      await released
      return false
    }
  })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  let opened = 0
  p1.replicator.server.on('connection', () => opened++)

  let accepted = 0
  p1.replicator.on('connected', () => accepted++)

  await Promise.all([
    p2.replicator._connect(p2.replicator.dialer.lookup(p1.replicator.peerId)),
    p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))
  ])

  t.is(opened, 2, 'both passed the firewall')
  t.is(accepted, 1, 'p1 replicated one')
  t.is(p1.replicator.connectionCount, 1, 'p1 at max peers')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('refuseAtMaxPeers does not firewall outbound dials', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1, refuseAtMaxPeers: true })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  await joinRemote(p2, p1)
  t.is(p1.replicator.connectionCount, 1, 'p1 at max peers')

  const failed = await p1.replicator._connect(p1.replicator.dialer.lookup(p3.replicator.peerId))

  t.is(failed, false, 'p1 dialed p3')
  t.ok(p1.replicator.outbound.has(p3.peerId), 'p1 connected to p3')

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('inbound at max peers is accepted and evicted by default', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, maxPeers: 1 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })

  await joinRemote(p2, p1)

  const connected = onceEvent(p1.replicator, 'connected')
  await p3.replicator._connect(p3.replicator.dialer.lookup(p1.replicator.peerId))
  await Promise.race([connected, timeout(5000)])

  t.is(p1.replicator.connectionCount, 2, 'p3 accepted over the cap')

  await p1.replicator._gc()

  t.alike(
    Array.from(p1.replicator.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId],
    'gc evicted p3'
  )

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('root close() closes replicator', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp())
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const closed = onceEvent(replicator, 'close')
  await store.close()
  await closed

  t.is(replicator.destroyed, true)

  await testnet.destroy()
})

test('inbound peer gets banned for hammering', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const p1Key = p1.replicator.peerId
  const target = p2.replicator.dialer.lookup(p1Key)
  const attempts = 21

  for (let n = 0; n < attempts; n++) {
    await p2.replicator._connect(target)

    const connection = p2.replicator.getConnection(toHex(p1Key))
    if (!connection) break

    connection.stream.destroy()
    await onceEvent(connection.stream, 'close')
  }

  const peer = p1.replicator.dialer.peers.get(toHex(p2.replicator.peerId))

  t.ok(peer, 'p1 knows p2')
  t.ok(peer.isBanned, 'p2 is banned')
  t.is(peer.banReason, 'hammering')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('regression: suspend() during async firewall leaked _inflight', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p1Hex = toHex(p1.replicator.keyPair.publicKey)

  let releaseTrap = null
  const trapped = new Promise((resolve) => {
    releaseTrap = resolve
  })

  let resolveFirewall = null
  const firewall = new Promise((resolve) => {
    resolveFirewall = resolve
  })

  const p2 = await spawnPeer(t, {
    bootstrap,
    seed: seedB,
    jitter: 0,
    firewall: () => {
      releaseTrap()
      return firewall
    }
  })

  const p2session = p2.replicator.join(p2.store.session(), p1.cores[0].discoveryKey, {
    discover: true
  })

  await p2session.ready()
  await p2session.refresh()

  await trapped // pending async firewall

  const suspended = p2.replicator.suspend()
  t.ok(p2.replicator.suspended, 'suspending')

  resolveFirewall(false) // release

  // TODO: rep.on('dial-start') & rep.on('dial-end')
  await new Promise((resolve) => setImmediate(resolve))
  await new Promise((resolve) => setImmediate(resolve))

  await suspended
  t.ok('suspended')
  await p2.replicator.resume()

  t.is(p2.replicator._inflight.has(p1Hex), false, '_inflight cleared')
  t.is(p2.replicator.hasConnection(p1Hex), false, 'hasConnection(p1) is cleared')
  t.is(p2.replicator.connecting, 0, 'zero connections')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('dialer is resumed after suspend', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  const topic = p1.cores[0].discoveryKey
  const remote = p1.replicator.peerId

  p2.replicator.dialer.topicEnable(topic)
  const suspended = p2.replicator.suspend()
  p2.replicator.dialer.seen({ publicKey: remote }, topic)

  const peer = p2.replicator.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0, 'no dials')

  await suspended

  await p2.replicator.resume()
  await p2.replicator.flush()

  t.ok(peer._outboundAttempts > 0, 'dialed after resume')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('regression: suspend() holds dials until resume()', async (t) => {
  const store = new Corestore(await t.tmp())
  const replicator = new Replicator(store, { bootstrap: [], manual: true, jitter: 0 })
  await replicator.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  replicator.dialer.topicEnable(topic)

  const suspended = replicator.suspend()
  replicator.dialer.seen({ publicKey: remote }, topic)
  await suspended

  const peer = replicator.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0, 'nothing dialed while suspending')

  await replicator.resume()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await replicator.close()
  await store.close()
})

test('suspend resets dial attempts of dropped connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  const topic = p1.cores[0].discoveryKey
  const remote = p1.replicator.peerId

  p2.replicator.dialer.topicEnable(topic)
  p2.replicator.dialer.seen({ publicKey: remote }, topic)
  await p2.replicator.flush()

  const peer = p2.replicator.dialer.lookup(remote)
  peer._outboundAttempts = 4 // took a few dials to connect

  await p2.replicator.suspend()

  t.is(peer._outboundAttempts, 0, 'attempts reset')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('replicator enables dialer throttle', async (t) => {
  const store = new Corestore(await t.tmp())
  const replicator = new Replicator(store, { bootstrap: [], maxPeers: 2 })

  t.is(replicator.dialer._want(), 2)

  await replicator.close()
  await store.close()
})

test('holepunch failure switches the peer to relaying', async (t) => {
  const store = new Corestore(await t.tmp())
  const replicator = new Replicator(store, {
    bootstrap: [],
    manual: true,
    jitter: 0,
    relayThrough: b4a.alloc(32, 7)
  })
  await replicator.ready()

  replicator.dht.connect = () => {
    const stream = new EventEmitter()
    stream.destroy = () => {}
    setImmediate(() => {
      stream.emit('error', Object.assign(new Error('stub'), { code: 'HOLEPUNCH_ABORTED' }))
      stream.emit('close')
    })
    return stream
  }

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  replicator.dialer.topicEnable(topic)
  replicator.dialer.seen({ publicKey: remote }, topic)
  await replicator.dialer.flush()

  const peer = replicator.dialer.lookup(remote)
  t.is(peer._outboundSuccess, 0, 'failed dial is not a success')
  t.ok(peer.forceRelaying, 'switched to relaying')

  await replicator.close()
  await store.close()
})

test('regression: Dialer.flush() resolves when replicator suspends', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, jitter: 0 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  let releaseConnect = null
  const connectReleased = new Promise((resolve) => {
    releaseConnect = resolve
  })

  let connectEntered = null
  const connectStarted = new Promise((resolve) => {
    connectEntered = resolve
  })

  const connect = p2.replicator._connect
  p2.replicator._connect = async function (peer) {
    connectEntered()
    await connectReleased
    return connect.call(this, peer)
  }

  const topic = p1.cores[0].discoveryKey
  p2.replicator.dialer.topicEnable(topic)
  p2.replicator.dialer.seen({ publicKey: p1.replicator.peerId }, topic)

  await connectStarted

  const suspending = p2.replicator.suspend()
  releaseConnect()

  const suspended = await Promise.race([
    suspending.then(() => true),
    timeout(100).then(() => false)
  ])
  t.ok(suspended, 'suspension settles after the cancelled dial')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('network coming back online resets dial backoff', async (t) => {
  const store = new Corestore(await t.tmp())
  const replicator = new Replicator(store, { bootstrap: [], manual: true })
  await replicator.ready()

  const peer = replicator.dialer.lookup(b4a.alloc(32, 2))
  peer._outboundAttempts = 5 // failed while we were offline

  replicator.dht.online = false
  replicator.dht.emit('network-update')
  replicator.dht.online = true
  replicator.dht.emit('network-update')

  t.is(peer._outboundAttempts, 0)

  await replicator.close()
  await store.close()
})

test('ready does not wait for the server to listen', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap })

  await replicator.ready()

  t.is(replicator.listening, false)
  t.ok(replicator.peerId)

  await replicator.flush()

  t.is(replicator.listening, true)

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('pinned session announces only once the server is listening', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const core = store.get({ name: 'one' })
  await core.ready()

  const replicator = new Replicator(store, { bootstrap, manual: true })

  let release = null
  const gate = new Promise((resolve) => {
    release = resolve
  })

  const listen = replicator.server.listen.bind(replicator.server)
  replicator.server.listen = async (keyPair) => {
    await gate
    return listen(keyPair)
  }

  await replicator.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true })
  await session.refresh()

  t.is(replicator.listening, false)
  t.is(session.discovery.announce, false, 'not announced before listening')

  release()

  await replicator.flush()

  t.is(replicator.listening, true)

  await replicator.refresh()

  t.is(session.discovery.announce, true, 'announced after listening')

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('failed listen ignores error and nothing announces', async (t) => {
  const dht = new DHT({ bootstrap: [] })

  // TODO: creates a defunct replicator (see condition of warn)
  // add tests that cover invalid states of injected dependencies
  await dht.destroy()

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { dht })

  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true })
  await session.refresh()
  await replicator.flush()

  t.is(replicator.listening, false)
  t.is(session.discovery.announce, false, 'not announced')

  await replicator.close()
  await store.close()
})

test('presuspend accepts inbound connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  p2.replicator.presuspend()
  await p2.replicator.flush()

  const connected = onceEvent(p2.replicator, 'connected')
  const stream = p1.replicator.dht.connect(p2.replicator.peerId, {
    keyPair: p1.replicator.keyPair
  })

  await connected

  t.ok(p2.replicator.inbound.has(p1.peerId), 'accepted inbound connection')

  stream.destroy()
  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('presuspend allows outbound connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  p2.replicator.presuspend()
  await p2.replicator.flush()

  const topic = b4a.alloc(32, 1)
  p2.replicator.dialer.topicEnable(topic)
  p2.replicator.dialer.seen({ publicKey: p1.replicator.peerId }, topic)
  await p2.replicator.flush()

  t.ok(p2.replicator.outbound.has(p1.peerId), 'opened outbound connection')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
