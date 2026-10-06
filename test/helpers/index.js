const createTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const Replicator = require('../..')
const { toHex } = b4a

module.exports = { spawnSwarm }

async function spawnSwarm(t) {
  const swarm = new TestSwarm(t)
  await swarm.ready()
  return swarm
}

class TestSwarm extends ReadyResource {
  constructor(t) {
    super()
    this.t = t
    this.testnet = null
    this.bootstrap = null
    this.rootPath = null
    this.peers = []
    this.writers = new Map()
    this._peerCounter = 0
  }

  async _open() {
    this.testnet = await createTestnet()
    this.bootstrap = this.testnet.bootstrap
    this.rootPath = await this.t.tmp()
  }

  async propagateWriters() {
    const done = []

    for (const [topic, keys] of this.writers) {
      for (const key of keys) {
        for (const peer of this.peers) {
          if (!peer.sessions.has(topic)) continue
          const { session } = peer.sessions.get(topic)
          done.push(session.get({ key }).ready())
        }
      }
    }

    await Promise.all(done)
  }

  refresh() {
    return Promise.all(this.peers.map((p) => p.replicator.refresh()))
  }

  flush() {
    return Promise.all(this.peers.map((p) => p.replicator.flush()))
  }

  gc() {
    return Promise.all(this.peers.map((p) => p.replicator._gc()))
  }

  async spawnPeer(opts) {
    const peer = new TestPeer(++this._peerCounter, this.rootPath, {
      bootstrap: this.bootstrap,
      autoswarm: true,
      ...opts
    })

    this.peers.push(peer)
    peer.on('close', () => {
      const i = this.peers.indexOf(peer)
      if (i > -1) this.peers.splice(i, 1)
    })
    peer.on('writer', (topic, core) => {
      const writers = this.writers.get(topic) || []
      writers.push(core.key)
      this.writers.set(topic, writers)
    })

    await peer.ready()

    return peer
  }

  async _close() {
    await Promise.all(this.peers.map((p) => p.destroy()))
    await this.testnet.destroy()
  }

  // lunte-disable-next-line require-await
  async destroy() {
    return this.close()
  }
}

class TestPeer extends ReadyResource {
  constructor(id, path, opts) {
    super()
    this._id = id
    this._path = `${path}/${this._id}`
    this._opts = opts
    this.sessions = new Map()

    const { seed } = opts
    let storeOpts = {}
    if (seed) storeOpts = { primaryKey: seed, unsafe: true }

    this.store = new Corestore(this._path, storeOpts)
    this.replicator = new Replicator(this.store, {
      manual: true,
      autoswarm: true,
      dhtFastRetry: 100,
      ...this._opts
    })
  }

  async _open() {
    await this.replicator.ready()
  }

  async join(topic, opts = {}) {
    const nCores = opts.nCores ?? 3
    delete opts.nCores

    const ns = this.store.namespace(topic)
    const session = ns.session()
    const repSession = this.replicator.join(session, topic, opts)
    await repSession.ready()

    this.sessions.set(toHex(topic), { session, repSession })

    const done = []
    for (let i = 0; i < nCores; i++) {
      const core = session.get({ name: `peer-${this._id}-writer-${i}` })
      const p = core
        .ready()
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

  destroy() {
    return this.close()
  }
}
