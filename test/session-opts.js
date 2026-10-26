const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const Replicator = require('..')
const { spawnSwarm } = require('./helpers')
const { seedA, timeout } = require('./helpers/backports')

test('join() defaults: server and client both default to true', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true })

  t.ok(session.server, '.server = true')
  t.ok(session.client, '.client = true')

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('join({ server: false }) prevents a pinned session from ever announcing', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true, server: false })
  t.absent(session.server, '.server disabled')
  await session.refresh()
  await replicator.flush()

  t.is(replicator.listening, true)

  await replicator.refresh()

  t.is(session.isPinned, true, 'still pinned - server:false does not affect active-state')
  t.is(session.discovery.announce, false, 'never announces, even once listening')
  t.is(session.isAnnounced, false)
  t.is(replicator.announcedSessions.length, 0)

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('join({ server: false }) still bootstraps a dht lookup for a pinned session (no peer needed)', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true, server: false })
  await session.refresh()
  await replicator.flush()

  t.is(replicator.listening, true)

  // `shouldAnnounce` never flips for a `server: false` session, so the only
  // thing that can kick off its (client-only) discovery query is the
  // dedicated bootstrap condition in `refresh()`. After initial refresh
  // discovery will continue triggering.
  await replicator.refresh()
  await replicator.refresh()

  t.not(session.discovery.lastRefreshed, -1, 'a dht query was issued')
  t.ok(session.discovery.stats.lookup > 0, 'it was a lookup')
  t.is(session.discovery.stats.announce, 0, 'never an announce')

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('join({ server: false }) can still use outbound connections', async (t) => {
  const swarm = await spawnSwarm(t)
  const opts = { discoveryStartupDelay: 0, autoStartupDelay: 0 }

  const p1 = await swarm.spawnPeer(opts)
  const p2 = await swarm.spawnPeer(opts)
  const topic = b4a.alloc(32, 0x1)

  const p1Connected = new Promise((resolve) => p1.replicator.once('connected', resolve))
  const p2Connected = new Promise((resolve) => p2.replicator.once('connected', resolve))

  const session1 = await p1.join(topic, { pin: true, server: false })
  const session2 = await p2.join(topic, { pin: true })

  await swarm.propagateWriters()
  await swarm.refresh()
  await swarm.flush()
  await Promise.all([p1Connected, p2Connected])

  // p1 opted out of announcing, and never shows up as announced
  t.absent(session1.announce, 'p1 never sets discovery to announce')
  t.absent(session1.isAnnounced, 'p1 session.isAnnounced = false')
  t.is(p1.replicator.announcedSessions.length, 0)
  t.is(p1.replicator.stats.dht.announce, 0, 'no announce dht queries fired')
  t.is(p1.replicator.stats.dht.unannounce, 0)
  t.ok(p1.replicator.stats.dht.lookup > 0, 'does a plain lookup instead of announcing')

  // p2 kept default behaviour and announces normally
  t.ok(session2.announce, 'p2 session set to announce')
  t.ok(session2.isAnnounced, 'p2 session isAnnounced')
  t.is(p2.replicator.announcedSessions.length, 1)

  // p1 still discovers p2 (via lookup) and connects to it as a client
  t.ok(p1.replicator.connectionCount > 0, 'p1 connects despite not announcing')
  t.ok(p2.replicator.connectionCount > 0, 'p2 accepts the inbound connection from p1')

  await swarm.close()
})

test('join({ server: false }) also masks announcing for autostarted (non-pinned) sessions', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  const topic = b4a.alloc(32, 0x1)

  const p1Connected = new Promise((resolve) => p1.replicator.once('connected', resolve))
  const p2Connected = new Promise((resolve) => p2.replicator.once('connected', resolve))

  const session1 = await p1.join(topic, { deferSwarming: false, server: false })
  const session2 = await p2.join(topic, { deferSwarming: false })

  await swarm.propagateWriters()
  await swarm.refresh() // announce & lookup
  await swarm.flush() // dht-query + dials
  await Promise.all([p1Connected, p2Connected])
  await swarm.gc() // de-dupe

  t.is(session1.isAuto, true, 'autoswarm activated the session')
  t.is(session1.announce, false, 'server:false masks the autostarted announce')
  t.is(p1.replicator.announcedSessions.length, 0)

  t.is(session2.isAuto, true)
  t.is(session2.announce, true, 'unaffected peer still announces')
  t.is(p2.replicator.announcedSessions.length, 1)

  t.is(p1.replicator.connectionCount, 1, 'still connects as a client')
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

test('stale closing session cant disable the dialer topic for its replacement', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  // replicator.ready() is NOT awaited yet intentionally

  const core = store.get({ name: 'one' })
  await core.ready()
  const dk = core.discoveryKey
  const hex = b4a.toHex(dk)

  const session1 = replicator.join(store, dk, { pin: true })
  const opening = session1.ready() // also kicks off replicator._open(); won't resolve until replicator.createKeyPair() does

  t.ok(replicator.dialer._activeTopics.has(hex), 'session1 pinned & active -> topic enabled')

  session1.close() // opened === false, opening !== null -> close() defers _close() until `opening` settles
  const closing = session1.closing

  // Replace session1 while it's still "closing" which still waiting on its own first _open().
  replicator.join(store, dk, { pin: true })

  t.ok(replicator.dialer._activeTopics.has(hex), 'session2 pinned & active -> topic (re)enabled')

  await opening.catch(() => {})
  await closing // session1's deferred _close() now runs: _stop()/_transition(DESTROYED) fire 'state' on session1

  t.ok(
    replicator.dialer._activeTopics.has(hex),
    'topic should stay enabled for the active replacement session (fails: session1 stale "state" listener disables it)'
  )

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('join() on a topic whose session is still closing replaces it and waits before announcing', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()
  const dk = core.discoveryKey

  const session1 = replicator.join(store, dk, { pin: true })
  await session1.ready()
  await session1.refresh()
  await replicator.flush()

  t.is(session1.isAnnounced, true, 'session1 announced')

  session1.close() // not awaited - still in-flight
  const closing = session1.closing // same promise the replicator hands to the replacement session

  // rejoin immediately, while session1 is still closing (unannouncing)
  const session2 = replicator.join(store, dk, { pin: true })

  t.not(session2, session1, 'join() returns a fresh session')
  t.absent(session1.closed, 'session1 has not finished closing yet')

  // the replicator's map already points at the replacement
  const keyedSession = replicator.sessions.get(b4a.toHex(dk))
  t.is(keyedSession, session2, 'session2 is tracked in replicator.sessions')
  t.is(
    [...replicator.sessions.values()].includes(session1),
    false,
    'session1 removed from map immediately'
  )

  await session2.ready()
  t.is(
    session2.discovery._wait,
    closing,
    'session2 defers its first discovery refresh/abort on session1 fully closing'
  )

  await closing
  t.is(session1.closed, true)

  await session2.refresh()
  await replicator.flush()
  t.is(session2.isAnnounced, true, 'session2 announced after session1 finished closing')

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('replicator.close() waits for a replaced closing session', async (t) => {
  const { testnet, store, replicator, session } = await announced(t)
  slowUnannounce(replicator)

  session.close()
  replicator.join(store, session.topic, { pin: true })
  await replicator.close()

  t.ok(session.closed)

  await store.close()
  await testnet.destroy()
})

test('replicator.suspend() waits for a replaced closing session', async (t) => {
  const { testnet, store, replicator, session } = await announced(t)
  slowUnannounce(replicator)

  session.close()
  replicator.join(store, session.topic, { pin: true })
  await replicator.suspend()

  t.ok(session.closed)

  await replicator.resume()
  await replicator.close()
  await store.close()
  await testnet.destroy()
})

test('closing a replacement session waits for the session it replaced', async (t) => {
  const { testnet, store, replicator, session } = await announced(t)

  session.close()
  const replacement = replicator.join(store, session.topic, { pin: true })
  await replacement.close()

  t.ok(session.closed)

  await replicator.close()
  await store.close()
  await testnet.destroy()
})

async function announced(t) {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const store = new Corestore(await t.tmp(), { primaryKey: seedA, unsafe: true })
  const replicator = new Replicator(store, { bootstrap, manual: true })
  await replicator.ready()

  const core = store.get({ name: 'one' })
  await core.ready()

  const session = replicator.join(store, core.discoveryKey, { pin: true })
  await session.refresh()
  await replicator.flush()

  return { testnet, store, replicator, session }
}

// a slow network: unannounce takes longer than local teardown
function slowUnannounce(replicator) {
  const unannounce = replicator.dht.unannounce.bind(replicator.dht)
  replicator.dht.unannounce = async (...args) => {
    await timeout(500)
    return unannounce(...args)
  }
}
