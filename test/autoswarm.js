const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const Replicator = require('..')

test('autoswarm: disabled, all are active (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: false })
  const p2 = await swarm.spawnPeer({ autoswarm: false })
  const topic = b4a.alloc(32, 0x1)
  await Promise.all([
    p1.join(topic),
    p2.join(topic),
  ])
  await swarm.propagate()
  await swarm.refresh()

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.not(p1.replicator.connectionCount, 0)
  t.not(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test('autoswarm: enabled, two new instances do not swarm (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: true })
  const p2 = await swarm.spawnPeer({ autoswarm: true })

  const topic = b4a.alloc(32, 0x1)
  await Promise.all([
    p1.join(topic),
    p2.join(topic),
  ])
  await swarm.propagate()
  await swarm.refresh()

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test('autoswarm: active instances', async (t) => {
  const swarm = await spawnSwarm(t)

  const topic = b4a.alloc(32, 0x1)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()

  async function performJoin (peer) {
    const repSession = await peer.join(topic)
    repSession.startActive()
  }
  await Promise.all([p1, p2].map(performJoin))
  await swarm.propagate()
  await swarm.refresh()
  await swarm.refresh() // TODO: REMOVE!

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.not(p1.replicator.connectionCount, 0)
  t.not(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test.skip('autoswarm: can enable discovery swarming', async (t) => {
  const swarm = await spawnSwarm(t)

  const updatedDiscoveryTimeout = 500
  const p1 = await swarm.spawnPeer({
    refreshInterval: 50, // TODO: not implemented
    gcIdleTime: 100, // TODO: not implemented
    updatedDiscoveryTimeout,
  })
  const p2 = await swarm.spawnPeer()

  const { topic } = swarm

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  const session1 = p1.repSession
  session1.startDiscovery()
  await p1.replicator.refresh()

  t.is(session1.announce, false)
  t.is(session1.discoveryTimeout, discoveryTimeout)

  p2.repSession.startActive()
  await p2.replicator.refresh()

  // discovery window is shortened on successfull replication

  session1.notifyDiscoveryUpdate()
  await p1.replicator.refresh()

  t.is(p1.replicator.sessions.get(toHex(topic)), session1, 'still open')
  t.is(session1.announce, false, 'still not listening')
  t.is(session1.discoveryTimeout, updatedDiscoveryTimeout, 'shortened')

  // discovery should deactivate

  await new Promise((resolve) => setTimeout(resolve, updatedDiscoveryTimeout * 5))
  await p1.replicator.refresh()

  t.absent(p1.replicator.sessions.get(toHex(topic)))

  await swarm.destroy()
})

test.skip('autoswarm: replicator activates fallbacks', (t) => {})

async function spawnSwarm(t) {
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const peers = []
  const writers = new Map()
  const rootPath = await t.tmp()

  const propagate = async () => {
    let done = []
    let cores = 0
    let topics = 0

    for (const [topic, keys] of writers) {
      topics++
      for (const key of keys) {
        for (const peer of peers) {
          if (!peer.sessions.has(topic)) continue
          const { session } = peer.sessions.get(topic)

          const p = session.get({ key }).ready()
          done.push(p)
          cores++
        }
      }
    }

    await Promise.all(done)

    return { cores, topics }
  }

  const refresh = () => Promise.all(peers.map(p => p.refresh()))

  return {
    propagate,
    refresh,

    async spawnPeer(opts) {
      const peer = new TestPeer(rootPath, {
        bootstrap,
        autoswarm: true,
        ...opts
      })

      peers.push(peer)
      peer.on('writer', (topic, core) => {
        const ws = writers.get(topic) || []
        ws.push(core.key)
        writers.set(topic, ws)
      })

      await peer.ready()

      return peer
    },

    async destroy() {
      await Promise.all(peers.map(p => p.destroy()))
      await testnet.destroy()
    }
  }
}

let _peerCounter = 0

class TestPeer extends ReadyResource {
  constructor(path, opts) {
    super()
    this._id = ++_peerCounter
    this._path = `${path}/${this._id}`
    this._opts = opts
    this.sessions = new Map()

    const { seed } = opts
    let storeOpts = {}
    if (seed) storeOpts = { primaryKey: seed, unsafe: true }

    this.store = new Corestore(this._path, storeOpts)
    this.replicator = new Replicator(this.store, {
      manual: true,
      ...this._opts
    })
  }

  async _open() {
    await this.replicator.ready()
  }

  async join (topic, nCores = 3) {
    const ns = this.store.namespace(topic)
    const session = ns.session()
    const repSession = this.replicator.join(session, topic)
    await repSession.ready()

    this.sessions.set(toHex(topic), { session, repSession })

    let done = []
    for (let i = 0; i < nCores; i++) {
      const core = session.get({ name: `peer-${this._id}-writer-${i}` })
      const p = core.ready()
        .then(() => core.append(b4a.from('block')))
        .then(() => this.emit('writer', toHex(topic), core))
      done.push(p)
    }

    await Promise.all(done)

    return repSession
  }

  async _close() {
    await this.replicator.close()
    await this.store.close()
  }

  async refresh() {
    let done = []
    for (const { repSession } of this.sessions.values()) {
      done.push(repSession.refresh())
    }
    return Promise.all(done)
  }

  destroy() {
    return this.close()
  }
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
