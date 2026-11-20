const { test } = require('brittle')
const EventEmitter = require('bare-events')
const Corestore = require('corestore')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const DHT = require('hyperdht')
const { joinRemote, onceEvent, seedA, seedB, spawnPeer, timeout } = require('./helpers/backports')
const Hyperjoin = require('..')

const { toHex } = b4a

test('default keypair from corestore', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const keyPair = await store.createKeyPair('replicator')
  const hyperjoin = await new Hyperjoin(
    (stream) => store.replicate(stream),
    () => null,
    { keyPair, bootstrap, manual: true }
  )

  await hyperjoin.ready()

  t.is(toHex(hyperjoin.peerId), '7dda4c84e7061170c5100be5d4c5cce9bb05ff2e1bd20d80d83408a10a6e73cc')

  await hyperjoin.close()
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
  t.is(p1.hyperjoin.connectionCount, 1)
  t.is(p2.hyperjoin.connectionCount, 1)
  t.alike(
    Array.from(p2.hyperjoin.connections, (connection) => toHex(connection.peerId)),
    [p1.peerId],
    'p2 sees p1'
  )
  t.alike(
    Array.from(p1.hyperjoin.connections, (connection) => toHex(connection.peerId)),
    [p2.peerId],
    'p1 sees p2'
  )

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('inbound peer gets banned for hammering', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB })

  const p1Key = p1.hyperjoin.peerId
  const target = p2.hyperjoin.dialer.lookup(p1Key)
  const attempts = 21

  let firewalled = false

  for (let n = 0; n < attempts; n++) {
    await p2.hyperjoin._connect(target)

    const connection = p2.hyperjoin.getConnection(toHex(p1Key))
    if (!connection) {
      firewalled = true
      break
    }

    connection.stream.destroy()

    await onceEvent(connection.stream, 'close')
  }

  t.is(firewalled, true, 'last attempt hit firewall')

  const peer = p1.hyperjoin.dialer.peers.get(toHex(p2.hyperjoin.peerId))

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
  const p1Hex = toHex(p1.hyperjoin.keyPair.publicKey)

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

  p2.hyperjoin.scan(p1.cores[0].discoveryKey, { delay: 0 })

  await trapped // pending async firewall

  const suspended = p2.hyperjoin.suspend()
  t.ok(p2.hyperjoin.suspended, 'suspending')

  resolveFirewall(false) // release

  // TODO: hyperjoin.on('dial-start') & hyperjoin.on('dial-end')
  await new Promise((resolve) => setImmediate(resolve))
  await new Promise((resolve) => setImmediate(resolve))

  await suspended
  t.ok('suspended')

  t.is(p2.hyperjoin._inflight.has(p1Hex), false, '_inflight cleared')
  t.is(p2.hyperjoin.hasConnection(p1Hex), false, 'hasConnection(p1) is cleared')
  t.is(p2.hyperjoin.connecting, 0, 'zero connections')

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
  const remote = p1.hyperjoin.peerId

  p2.hyperjoin.dialer.refTopic(topic)
  const suspended = p2.hyperjoin.suspend()
  p2.hyperjoin.dialer.seen({ publicKey: remote }, topic)

  const peer = p2.hyperjoin.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0, 'no dials')

  await suspended

  await p2.hyperjoin.resume()
  await p2.hyperjoin.flush()

  t.ok(peer._outboundAttempts > 0, 'dialed after resume')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('regression: suspend() holds dials until resume()', async (t) => {
  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const hyperjoin = new Hyperjoin(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 0
    }
  )
  await hyperjoin.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  hyperjoin.dialer.refTopic(topic)

  const suspended = hyperjoin.suspend()
  hyperjoin.dialer.seen({ publicKey: remote }, topic)
  await suspended

  const peer = hyperjoin.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0, 'nothing dialed while suspending')

  await hyperjoin.resume()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await hyperjoin.close()
  await store.close()
})

test('suspend resets dial attempts of dropped connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await spawnPeer(t, { bootstrap, seed: seedA })
  const p2 = await spawnPeer(t, { bootstrap, seed: seedB, jitter: 0 })

  const topic = p1.cores[0].discoveryKey
  const remote = p1.hyperjoin.peerId

  p2.hyperjoin.dialer.refTopic(topic)
  p2.hyperjoin.dialer.seen({ publicKey: remote }, topic)
  await p2.hyperjoin.flush()

  const peer = p2.hyperjoin.dialer.lookup(remote)
  peer._outboundAttempts = 4 // took a few dials to connect

  await p2.hyperjoin.suspend()

  t.is(peer._outboundAttempts, 0, 'attempts reset')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})

test('hyperjoin enables dialer throttle', async (t) => {
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap: [], maxPeers: 2 }
  )

  t.is(hyperjoin.dialer._want(), 2)

  await hyperjoin.close()
})

test('holepunch failure switches the peer to relaying', async (t) => {
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    {
      bootstrap: [],
      manual: true,
      jitter: 0,
      relayThrough: b4a.alloc(32, 7)
    }
  )
  await hyperjoin.ready()

  hyperjoin.dht.connect = () => {
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

  hyperjoin.dialer.refTopic(topic)
  hyperjoin.dialer.seen({ publicKey: remote }, topic)
  await hyperjoin.dialer.flush()

  const peer = hyperjoin.dialer.lookup(remote)
  t.is(peer._outboundSuccess, 0, 'failed dial is not a success')
  t.ok(peer.forceRelaying, 'switched to relaying')

  await hyperjoin.close()
})

test('regression: Dialer.flush() resolves when hyperjoin suspends', async (t) => {
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

  const connect = p2.hyperjoin._connect
  p2.hyperjoin._connect = async function (peer) {
    connectEntered()
    await connectReleased
    return connect.call(this, peer)
  }

  const topic = p1.cores[0].discoveryKey
  p2.hyperjoin.dialer.refTopic(topic)
  p2.hyperjoin.dialer.seen({ publicKey: p1.hyperjoin.peerId }, topic)

  await connectStarted

  const suspending = p2.hyperjoin.suspend()
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
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap: [], manual: true }
  )
  await hyperjoin.ready()

  const before = hyperjoin.dialer.lookup(b4a.alloc(32, 1))
  before._outboundAttempts = 5 // failed before we went offline
  before.attemptedAt = Date.now() - 1000

  hyperjoin.dht.emit('network-update') // still online
  t.is(before._outboundAttempts, 5)

  hyperjoin.dht.online = false
  hyperjoin.dht.emit('network-update')

  const failed = hyperjoin.dialer.lookup(b4a.alloc(32, 2))
  failed._outboundAttempts = 5 // failed while we were offline
  failed.attemptedAt = Date.now()

  const dropped = hyperjoin.dialer.lookup(b4a.alloc(32, 3))
  dropped._outboundAttempts = 5 // dropped while we were offline
  dropped.attemptedAt = Date.now() - 1000
  dropped.disconnectedAt = Date.now()

  hyperjoin.dht.online = true
  hyperjoin.dht.emit('network-update')

  t.is(before._outboundAttempts, 5)
  t.is(failed._outboundAttempts, 0)
  t.is(dropped._outboundAttempts, 0)

  await hyperjoin.close()
})

test('ready does not wait for the server to listen', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap }
  )

  await hyperjoin.ready()

  t.is(hyperjoin.listening, false)
  t.ok(hyperjoin.peerId)

  await hyperjoin.flush()

  t.is(hyperjoin.listening, true)

  await hyperjoin.close()
  await testnet.destroy()
})

test('pinned session announces only once the server is listening', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  let release = null
  const gate = new Promise((resolve) => {
    release = resolve
  })

  const listen = hyperjoin.server.listen.bind(hyperjoin.server)
  hyperjoin.server.listen = async (keyPair) => {
    await gate
    return listen(keyPair)
  }

  await hyperjoin.ready()

  const session = hyperjoin.join(topic, { pin: true })
  session.refresh()

  t.is(hyperjoin.listening, false)
  t.is(session.discovery.announce, false, 'not announced before listening')

  release()

  await hyperjoin.flush()

  t.is(hyperjoin.listening, true)

  hyperjoin.refresh()

  t.is(session.discovery.announce, true, 'announced after listening')

  await hyperjoin.close()
  await testnet.destroy()
})

test('failed listen ignores error and nothing announces', async (t) => {
  const dht = new DHT({ bootstrap: [] })

  // creates a defunct hyperjoin
  await dht.destroy()

  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { dht }
  )
  const topic = b4a.alloc(32, 1)

  // This test is not entierly valid;
  // It produces a hyperjoin instance that silently does not replicate.
  // TODO: harden conditions against defunct dependencies.
  let error = null
  hyperjoin.once('error', (err) => {
    error = err
  })

  await hyperjoin.ready()

  const session = hyperjoin.join(topic, { pin: true })
  session.refresh()
  await hyperjoin.flush()

  t.is(error.code, 'NODE_DESTROYED')
  t.is(hyperjoin.listening, false)
  t.is(session.discovery.announce, false, 'not announced')

  await hyperjoin.close()
})

test('regression: suspend does not count a dial still waiting to start', async (t) => {
  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const hyperjoin = new Hyperjoin(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 100
    }
  )
  await hyperjoin.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  hyperjoin.dialer.refTopic(topic)
  hyperjoin.dialer.seen({ publicKey: remote }, topic)

  await hyperjoin.suspend()

  t.is(hyperjoin.dialer.lookup(remote)._outboundAttempts, 0)

  await hyperjoin.close()
  await store.close()
})

test('regression: suspend does not count a dial waiting on the firewall', async (t) => {
  let release = null
  const gate = new Promise((resolve) => {
    release = resolve
  })

  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const hyperjoin = new Hyperjoin(
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
  await hyperjoin.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  hyperjoin.dialer.refTopic(topic)
  hyperjoin.dialer.seen({ publicKey: remote }, topic)
  await new Promise((resolve) => setImmediate(resolve))

  const suspended = hyperjoin.suspend()
  release()
  await suspended

  const peer = hyperjoin.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0)

  await hyperjoin.resume()
  await hyperjoin.flush()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await hyperjoin.close()
  await store.close()
})

test('regression: suspend does not count a dial it cancels', async (t) => {
  const store = new Corestore(await t.tmp())
  const keyPair = await store.createKeyPair('replicator')
  const hyperjoin = new Hyperjoin(
    (stream) => store.replicate(stream),
    () => null,
    {
      keyPair,
      bootstrap: [],
      manual: true,
      jitter: 0
    }
  )
  await hyperjoin.ready()

  const topic = b4a.alloc(32, 1)
  const remote = b4a.alloc(32, 2)

  const suspended = new Promise((resolve) => {
    hyperjoin.once('update', () => resolve(hyperjoin.suspend()))
  })

  hyperjoin.dialer.refTopic(topic)
  hyperjoin.dialer.seen({ publicKey: remote }, topic)
  await suspended

  const peer = hyperjoin.dialer.lookup(remote)
  t.is(peer._outboundAttempts, 0)

  await hyperjoin.resume()
  await hyperjoin.flush()

  t.is(peer._outboundAttempts, 1, 'dialed after resume')

  await hyperjoin.close()
  await store.close()
})
