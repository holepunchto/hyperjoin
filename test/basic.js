const { test } = require('brittle')
const EventEmitter = require('bare-events')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const DHT = require('hyperdht')
const {
  joinRemote,
  onceEvent,
  seedA,
  seedB,
  seedC,
  spawnPeer,
  timeout
} = require('./helpers/backports')
const Replicator = require('..')

const { toHex } = b4a

test('default keypair from corestore', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const keyPair = await store.createKeyPair('replicator')
  const replicator = await new Replicator(
    (stream) => store.replicate(stream),
    () => null,
    { keyPair, bootstrap, manual: true }
  )

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

test('inbound hammer bans, increasing stats & emits event', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  let banEvents = 0
  p1.replicator.on('ban', () => banEvents++)

  t.is(p1.replicator.stats.bannedPeers, 0)

  const p1Key = p1.replicator.peerId
  const target = p2.replicator.dialer.lookup(p1Key)
  const attempts = 21

  let firewalled = false

  for (let n = 0; n < attempts; n++) {
    await p2.replicator._connect(target)

    const connection = p2.replicator.getConnection(toHex(p1Key))
    if (!connection) {
      firewalled = true
      break
    }

    connection.stream.destroy()

    await onceEvent(connection.stream, 'close')
  }

  t.is(firewalled, true, 'last attempt hit firewall')
  t.is(p1.replicator.stats.bannedPeers, 1, 'stats increased')
  t.is(banEvents, 1, 'ban fired')

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

  p2.replicator.scan(p1.cores[0].discoveryKey, { delay: 0 })

  await trapped // pending async firewall

  const suspended = p2.replicator.suspend()
  t.ok(p2.replicator.suspended, 'suspending')

  resolveFirewall(false) // release

  // TODO: rep.on('dial-start') & rep.on('dial-end')
  await new Promise((resolve) => setImmediate(resolve))
  await new Promise((resolve) => setImmediate(resolve))

  await suspended
  t.ok('suspended')

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

  p2.replicator.dialer.refTopic(topic)
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
  const keyPair = await store.createKeyPair('replicator')
  const replicator = new Replicator(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 0
    }
  )
  await replicator.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  replicator.dialer.refTopic(topic)

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

  p2.replicator.dialer.refTopic(topic)
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
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap: [], minConnections: 2, maxConnections: 2 }
  )

  t.is(replicator.dialer._want(), 2)

  await replicator.close()
})

test('holepunch failure switches the peer to relaying', async (t) => {
  const replicator = new Replicator(
    () => {},
    () => null,
    {
      bootstrap: [],
      manual: true,
      jitter: 0,
      relayThrough: b4a.alloc(32, 7)
    }
  )
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

  replicator.dialer.refTopic(topic)
  replicator.dialer.seen({ publicKey: remote }, topic)
  await replicator.dialer.flush()

  const peer = replicator.dialer.lookup(remote)
  t.is(peer._outboundSuccess, 0, 'failed dial is not a success')
  t.ok(peer.forceRelaying, 'switched to relaying')

  await replicator.close()
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
  p2.replicator.dialer.refTopic(topic)
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
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap: [], manual: true }
  )
  await replicator.ready()

  const before = replicator.dialer.lookup(b4a.alloc(32, 1))
  before._outboundAttempts = 5 // failed before we went offline
  before.attemptedAt = Date.now() - 1000

  replicator.dht.emit('network-update') // still online
  t.is(before._outboundAttempts, 5)

  replicator.dht.online = false
  replicator.dht.emit('network-update')

  const failed = replicator.dialer.lookup(b4a.alloc(32, 2))
  failed._outboundAttempts = 5 // failed while we were offline
  failed.attemptedAt = Date.now()

  const dropped = replicator.dialer.lookup(b4a.alloc(32, 3))
  dropped._outboundAttempts = 5 // dropped while we were offline
  dropped.attemptedAt = Date.now() - 1000
  dropped.disconnectedAt = Date.now()

  replicator.dht.online = true
  replicator.dht.emit('network-update')

  t.is(before._outboundAttempts, 5)
  t.is(failed._outboundAttempts, 0)
  t.is(dropped._outboundAttempts, 0)

  await replicator.close()
})

test('ready does not wait for the server to listen', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap }
  )

  await replicator.ready()

  t.is(replicator.listening, false)
  t.ok(replicator.peerId)

  await replicator.flush()

  t.is(replicator.listening, true)

  await replicator.close()
  await testnet.destroy()
})

test('pinned session announces only once the server is listening', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

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

  const session = replicator.join(topic, { pin: true })
  session.refresh()

  t.is(replicator.listening, false)
  t.is(session.discovery.announce, false, 'not announced before listening')

  release()

  await replicator.flush()

  t.is(replicator.listening, true)

  replicator.refresh()

  t.is(session.discovery.announce, true, 'announced after listening')

  await replicator.close()
  await testnet.destroy()
})

test('failed listen ignores error and nothing announces', async (t) => {
  const dht = new DHT({ bootstrap: [] })

  // creates a defunct replicator
  await dht.destroy()

  const replicator = new Replicator(
    () => {},
    () => null,
    { dht }
  )
  const topic = b4a.alloc(32, 1)

  // This test is not entierly valid;
  // It produces a replicator instance that silently does not replicate.
  // TODO: harden conditions against defunct dependencies.
  let error = null
  replicator.once('error', (err) => {
    error = err
  })

  await replicator.ready()

  const session = replicator.join(topic, { pin: true })
  session.refresh()
  await replicator.flush()

  t.is(error.code, 'NODE_DESTROYED')
  t.is(replicator.listening, false)
  t.is(session.discovery.announce, false, 'not announced')

  await replicator.close()
})

test('regression: suspend does not count a dial still waiting to start', async (t) => {
  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const replicator = new Replicator(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 100
    }
  )
  await replicator.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  replicator.dialer.refTopic(topic)
  replicator.dialer.seen({ publicKey: remote }, topic)

  await replicator.suspend()

  t.is(replicator.dialer.lookup(remote)._outboundAttempts, 0)

  await replicator.close()
  await store.close()
})

test('regression: suspend does not count a dial waiting on the firewall', async (t) => {
  let release = null
  const gate = new Promise((resolve) => {
    release = resolve
  })

  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const replicator = new Replicator(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 0,
      firewall: async () => {
        await gate
        return false
      }
    }
  )
  await replicator.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  replicator.dialer.refTopic(topic)
  replicator.dialer.seen({ publicKey: remote }, topic)
  await new Promise((resolve) => setImmediate(resolve))

  const suspended = replicator.suspend()
  release()
  await suspended

  const peer = replicator.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0)

  await replicator.resume()
  await replicator.flush()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await replicator.close()
  await store.close()
})

test('regression: suspend does not count a dial it cancels', async (t) => {
  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const replicator = new Replicator(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 0
    }
  )
  await replicator.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  const suspended = new Promise((resolve) => {
    replicator.once('update', () => resolve(replicator.suspend()))
  })

  replicator.dialer.refTopic(topic)
  replicator.dialer.seen({ publicKey: remote }, topic)
  await suspended

  const peer = replicator.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0)

  await replicator.resume()
  await replicator.flush()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await replicator.close()
  await store.close()
})

test('dialer stops at minConnections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA, minConnections: 1, maxConnections: 2 })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })
  const p3 = await spawnPeer(t, { bootstrap, seed: seedC })
  const rep = p1.replicator

  const topic = b4a.alloc(32, 1)
  rep.dialer.refTopic(topic)

  rep.dialer.seen({ publicKey: p2.replicator.peerId }, topic)
  await rep.flush()

  t.ok(rep.outbound.has(p2.peerId), 'p2 dialed')

  rep.dialer.seen({ publicKey: p3.replicator.peerId }, topic)
  await rep.flush()

  t.absent(rep.outbound.has(p3.peerId), 'p3 not dialed')
  t.is(rep.stats.connects.client.attempted, 1)

  await p1.destroy()
  await p2.destroy()
  await p3.destroy()
  await testnet.destroy()
})

test('maxConnections lower than minConnections throws', (t) => {
  t.exception(
    () =>
      new Replicator(
        () => {},
        () => null,
        { minConnections: 2, maxConnections: 1 }
      )
  )
})
