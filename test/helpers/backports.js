const Corestore = require('corestore')
const b4a = require('b4a')
const Hyperjoin = require('../..')
const { toHex } = b4a

const Peer = require('../../lib/peer')
Peer.backoffDuration = (n) => [100, 200, 300, 400][n] || 35_000 // faster & safer tests

const seedA = Buffer.alloc(32, 0xa)
const seedB = Buffer.alloc(32, 0xb)
const seedC = Buffer.alloc(32, 0xc)

function seed(byte) {
  return Buffer.alloc(32, byte)
}

function unpromise() {
  let complete, abort
  return [
    new Promise((resolve, reject) => {
      ;((complete = resolve), (abort = reject))
    }),
    complete,
    abort
  ]
}

// TODO: remove this helper, use of timeouts is banned outside of test/backports/
function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function onceEvent(emitter, event) {
  return new Promise((resolve) => emitter.once(event, resolve))
}

function waitForEvents(emitter, event, count) {
  return new Promise((resolve) => {
    let seen = 0

    const onevent = () => {
      if (++seen < count) return
      emitter.removeListener(event, onevent)
      resolve()
    }

    emitter.on(event, onevent)
  })
}

async function joinRemote(peer, remotePeer) {
  const session = peer.store.session()
  const remote = remotePeer.cores.map((core) => session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  const topic = remotePeer.cores[0].discoveryKey
  peer.sessions.push({ session, topic })

  const hyperjoinSession = peer.hyperjoin.join(topic, {
    pin: true
  })
  await hyperjoinSession.ready()
  peer.hyperjoin.refresh()
  await peer.hyperjoin.flush()

  return remote
}

async function spawnPeer(t, opts = {}) {
  const { seed } = opts
  let storeOpts = {}
  if (seed) storeOpts = { primaryKey: seed, unsafe: true }

  const store = new Corestore(await t.tmp(), storeOpts)
  const sessions = []
  const keyPair = await store.createKeyPair('replicator')

  const hyperjoin = new Hyperjoin(
    (stream) => store.replicate(stream),
    opts.topicOf || ((discoveryKey) => topicOf(sessions, discoveryKey)),
    {
      manual: true,
      keyPair,
      ...opts
    }
  )

  await hyperjoin.ready()

  const session = store.session()

  const one = session.get({ name: 'one' })
  await one.ready()
  await one.append([b4a.from('cells'), b4a.from('interlinked')])

  const hyperjoinSession = hyperjoin.join(one.discoveryKey, { pin: true })
  await hyperjoinSession.ready()
  hyperjoin.refresh()
  await hyperjoin.flush()

  const two = session.get({ name: 'two' })
  await two.ready()
  await two.append([b4a.from('within'), b4a.from('Cells')])

  sessions.push({ session, topic: one.discoveryKey })

  const destroy = async () => {
    await hyperjoin.close()
    await store.close()
  }

  const peerId = toHex(hyperjoin.peerId)

  return { store, hyperjoin, session, sessions, peerId, destroy, cores: [one, two] }
}

function topicOf(sessions, discoveryKey) {
  const dkey = toHex(discoveryKey)
  for (const { session, topic } of sessions) {
    const core = session.cores.get(dkey)
    if (!core) continue

    if (session.sessions.list(core.id).length > 0) return topic
  }

  return null
}

module.exports = {
  seedA,
  seedB,
  seedC,
  seed,
  timeout,
  unpromise,
  onceEvent,
  waitForEvents,
  joinRemote,
  spawnPeer
}
