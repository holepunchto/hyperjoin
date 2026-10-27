const Corestore = require('corestore')
const b4a = require('b4a')
const Replicator = require('../..')
const { toHex } = b4a

const Peer = require('../../lib/peer')
Peer.backoffDuration = (n) => [100, 200, 300, 400][n] || 35_000 // faster & safer tests

const seedA = Buffer.alloc(32, 0xa)
const seedB = Buffer.alloc(32, 0xb)
const seedC = Buffer.alloc(32, 0xc)

// TODO: move all to single export at bottom

exports.seedA = seedA
exports.seedB = seedB
exports.seedC = seedC
exports.seed = function seed(byte) {
  return Buffer.alloc(32, byte)
}

exports.timeout = function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

exports.onceEvent = function onceEvent(emitter, event) {
  return new Promise((resolve) => emitter.once(event, resolve))
}

exports.waitForEvents = function waitForEvents(emitter, event, count) {
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

exports.joinRemote = async function joinRemote(peer, remotePeer) {
  const remote = remotePeer.cores.map((core) => peer.session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  const repSession = peer.replicator.join(peer.store.session(), remotePeer.cores[0].discoveryKey, {
    pin: true
  })
  await repSession.ready()
  await repSession.refresh()
  await peer.replicator.flush()

  return remote
}

exports.spawnPeer = async function spawnPeer(t, opts = {}) {
  const { seed } = opts
  let storeOpts = {}
  if (seed) storeOpts = { primaryKey: seed, unsafe: true }

  const store = new Corestore(await t.tmp(), storeOpts)
  const replicator = new Replicator(store, {
    manual: true,
    autoswarm: false,
    discoveryStartupDelay: 0,
    dhtFastRetry: 0,
    ...opts
  })

  await replicator.ready()

  const session = store.session()

  const one = session.get({ name: 'one' })
  await one.ready()
  await one.append([b4a.from('cells'), b4a.from('interlinked')])

  const repSession = replicator.join(session, one.discoveryKey, { pin: true })
  await repSession.ready()
  await repSession.refresh()
  await replicator.flush()

  const two = session.get({ name: 'two' })
  await two.ready()
  await two.append([b4a.from('within'), b4a.from('Cells')])

  const destroy = async () => {
    await replicator.close()
    await store.close()
  }

  const peerId = toHex(replicator.peerId)

  return { store, replicator, session, peerId, destroy, cores: [one, two] }
}
