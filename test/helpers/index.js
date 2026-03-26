const Corestore = require('corestore')
const b4a = require('b4a')
const Replicator = require('../..')

const seedA = Buffer.alloc(32, 0xa)
const seedB = Buffer.alloc(32, 0xb)
const seedC = Buffer.alloc(32, 0xc)

exports.seedA = seedA
exports.seedB = seedB
exports.seedC = seedC
exports.seed = function seed(byte) {
  return Buffer.alloc(32, byte)
}

exports.timeout = function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

exports.onceServerConnection = function onceServerConnection(server, publicKey) {
  return new Promise((resolve) => {
    const onconnection = (socket) => {
      if (!b4a.equals(socket.remotePublicKey, publicKey)) return
      server.removeListener('connection', onconnection)
      resolve(socket)
    }

    server.on('connection', onconnection)
  })
}

exports.joinRemote = async function joinRemote(peer, remotePeer) {
  const remote = remotePeer.cores.map((core) => peer.session.get(core.key))
  await Promise.all(remote.map((core) => core.ready()))

  peer.replicator.join(peer.store.session(), remotePeer.cores[0].discoveryKey)

  return remote
}

exports.connectionKeys = function connectionKeys(replicator) {
  return Array.from(replicator.connections(), (connection) => b4a.toString(connection.peerId, 'hex'))
}

exports.getConnection = function getConnection(replicator, peerId) {
  return replicator.inbound.get(peerId) || replicator.outbound.get(peerId) || null
}

exports.spawnPeer = async function spawnPeer(
  t,
  { seed, bootstrap, lowWatermark, highWatermark, manual = true, port } = {}
) {
  let storeOpts = {}
  if (seed) storeOpts = { primaryKey: seed, unsafe: true }

  const store = new Corestore(await t.tmp(), storeOpts)
  const replicator = new Replicator(store, {
    bootstrap,
    lowWatermark,
    highWatermark,
    manual,
    port
  })

  await replicator.ready()

  const session = store.session()

  const one = session.get({ name: 'one' })
  await one.ready()
  await one.append([b4a.from('cells'), b4a.from('interlinked')])

  replicator.join(session, one.discoveryKey).addLocal(one)

  const two = session.get({ name: 'two' })
  await two.ready()
  await two.append([b4a.from('within'), b4a.from('Cells')])

  const destroy = async () => {
    await replicator.close()
    await store.close()
  }

  const peerId = b4a.toString(replicator.keyPair.publicKey, 'hex')

  return { store, replicator, session, peerId, destroy, cores: [one, two] }
}
