const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const { spawnSwarm } = require('./helpers')
const { spawnPeer: basicPeer, onceEvent, seedA, seedB } = require('./helpers/backports')
const { toHex } = b4a

test('presuspend() unannounces active topics', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  const topic = b4a.alloc(32, 0x1)

  const rep1 = p1.replicator

  t.is(rep1.announcedSessions.length, 0)
  t.is(rep1.connectionCount, 0)

  const connected = new Promise((resolve) => p1.replicator.on('connected', resolve))

  const session1 = await p1.join(topic, { pin: true })
  await p2.join(topic, { pin: true })

  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  await connected
  t.ok('connected')

  t.is(rep1.isPresuspended, false)
  t.is(rep1.announcedSessions.length, 1)
  t.ok(rep1.connectionCount > 0)
  t.is(session1.isAnnounced, true)

  rep1.presuspend()
  const presuspended = rep1.flush()

  t.is(rep1.isPresuspended, true)
  t.ok(rep1.connectionCount > 0)

  await presuspended

  t.is(session1.isAnnounced, false)
  t.is(rep1.announcedSessions.length, 0)
  t.ok(rep1.connectionCount > 0, 'connections linger')

  await swarm.close()
})

test('presuspend() -> resume(), resumes announcements', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)
  const rep1 = p1.replicator

  const session1 = await p1.join(topic, { pin: true })
  await p2.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  const [connection] = rep1.connections
  t.ok(connection, 'connected')

  rep1.presuspend()
  await rep1.flush()

  await rep1.resume()

  await swarm.flush()
  swarm.refresh()

  t.is(rep1.isPresuspended, false)
  t.is(session1.isAnnounced, true, 'resume reannounces the topic')

  t.is(toHex([...rep1.connections][0].peerId), toHex(connection.peerId), 'connection retained')

  await swarm.close()
})

test('presuspend() -> suspend() -> resume(), resumes networking', async (t) => {
  const swarm = await spawnSwarm(t)
  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 1)
  const rep1 = p1.replicator

  const session1 = await p1.join(topic, { pin: true })
  await p2.join(topic, { pin: true })
  await swarm.propagateWriters()
  swarm.refresh()
  await swarm.flush()

  rep1.presuspend()
  await rep1.flush()

  await rep1.suspend()

  t.is(rep1.suspended, true)
  t.is(rep1.connectionCount, 0, 'suspend drops the connection')
  t.is(session1.isAnnounced, false)

  const { announce, unannounce } = rep1.stats.dht
  await rep1.resume({ presuspended: true })
  await rep1.flush()
  t.absent(rep1.suspended, 'wakeup resumes networking')
  t.ok(rep1.isPresuspended, 'wakeup preserves presuspend')
  t.absent(rep1.dialer.suspended, 'wakeup resumes the dialer')
  t.is(rep1.stats.dht.announce, announce, 'wakeup does not announce')
  t.is(rep1.stats.dht.unannounce, unannounce, 'wakeup does not unannounce')

  await rep1.suspend()
  const connected = new Promise((resolve) => rep1.once('connected', resolve))
  await rep1.resume()
  await swarm.flush() // announce + dial

  swarm.refresh() // de-dupe
  await connected

  t.is(rep1.suspended, false)
  t.is(rep1.isPresuspended, false)
  t.is(session1.isAnnounced, true, 'resume reannounces the topic')
  t.is(rep1.connectionCount, 1, 'resume reconnects')

  await swarm.close()
})

test('presuspend accepts inbound connections', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const p1 = await basicPeer(t, { bootstrap, seed: seedA })
  const p2 = await basicPeer(t, { bootstrap, seed: seedB })

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

  const p1 = await basicPeer(t, { bootstrap, seed: seedA })
  const p2 = await basicPeer(t, { bootstrap, seed: seedB })

  p2.replicator.presuspend()
  await p2.replicator.flush()

  const topic = b4a.alloc(32, 1)
  p2.replicator.dialer.refTopic(topic)
  p2.replicator.dialer.seen({ publicKey: p1.replicator.peerId }, topic)
  await p2.replicator.flush()

  t.ok(p2.replicator.outbound.has(p1.peerId), 'opened outbound connection')

  await p1.destroy()
  await p2.destroy()
  await testnet.destroy()
})
