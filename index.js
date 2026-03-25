const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const PeerDiscovery = require('./lib/peer-discovery')

const GC_INTERVAL = 5_000
const WATERMARK_LOW = 20
const WATERMARK_HIGH = 40

module.exports = class Replicator extends ReadyResource {
  constructor(rootStore, opts = {}) {
    super()

    const { seed, keyPair } = opts
    if (seed) this.keyPair = DHT.keyPair(seed)
    if (keyPair) this.keyPair = keyPair

    this.root = rootStore

    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })
    this.server = this.dht.createServer(this._onincoming.bind(this))
    this.listening = null

    this.destroyed = false
    this.suspended = false

    this.connections = new Map() // hex(peerId) -> Connection
    this.sessions = new Map() // hex(discoveryKey) -> Session
    this._connecting = new Set() // hex(peerId) -> dedup in-flight dials

    this._lowWatermark = opts.lowWatermark ?? WATERMARK_LOW
    this._highWatermark = opts.highWatermark ?? WATERMARK_HIGH
    this._manual = opts.manual === true

    this._gcInterval = null
    if (!this._manual) this._startGC()
  }

  get discoverySessions() {
    return Array.from(this.sessions.values()).map((sess) => sess.peerDiscovery)
  }

  *cores() {
    for (const session of this.sessions.values()) {
      for (const [discoveryKey, core] of session.cores) {
        yield { discoveryKey, core, session }
      }
    }
  }

  async _open() {
    this.keyPair ||= await this.root.createKeyPair('replicator')
    await this.dht.ready()
  }

  async listen() {
    if (!this.listening) {
      if (this.destroyed) throw new Error('Replicator destroyed')

      await this.ready()
      this.listening = this.server.listen(this.keyPair)
    }

    return this.listening
  }

  async suspend({ log = noop } = {}) {
    if (this.suspended) return

    const pending = []
    const suspending = [this.server.suspend({ log })]

    for (const peerDiscovery of this.discoverySessions) {
      suspending.push(peerDiscovery.suspend({ log }))
    }

    for (const session of this.sessions.values()) {
      suspending.push(session.suspend())
    }

    for (const connection of this.connections.values()) {
      pending.push(new Promise((resolve) => connection.stream.on('close', resolve)))
      connection.destroy()
    }

    this.suspended = true
    this._connecting.clear()
    clearInterval(this._gcInterval)
    this._gcInterval = null

    await Promise.allSettled(suspending)
    await this.dht.suspend({ log })
    await Promise.all(pending)
  }

  async resume({ log = noop } = {}) {
    if (!this.suspended) return

    await this.dht.resume({ log })
    await this.server.resume({ log })

    for (const session of this.sessions.values()) {
      session.resume()
    }

    for (const peerDiscovery of this.discoverySessions) {
      peerDiscovery.resume()
    }

    this.suspended = false
    if (!this._manual) this._startGC()
  }

  async _close() {
    this.destroyed = true

    clearInterval(this._gcInterval)
    this._gcInterval = null

    for (const session of this.sessions.values()) session.destroy()
    this.sessions.clear()

    for (const connection of this.connections.values()) connection.destroy()
    this.connections.clear()

    this._connecting.clear()

    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
  }

  join(store, discoveryKey) {
    let session = this.sessions.get(toHex(discoveryKey))
    if (session) return session

    session = new Session(this, store, discoveryKey)
    this.sessions.set(toHex(discoveryKey), session)

    return session
  }

  _onincoming(conn) {
    if (this.suspended) {
      conn.destroy()
      return
    }

    const peerId = conn.remotePublicKey
    const connection = this._getOrCreateConnection(peerId, conn, false)
  }

  _getOrCreateConnection(peerId, stream, initiator) {
    const hex = toHex(peerId)

    if (this._connecting.has(hex)) {
      console.error('outgoing inbound connection')
      return null
    }

    let connection = this.connections.get(hex)
    if (connection) return connection

    connection = new Connection(this, stream, peerId, initiator)
    this.connections.set(hex, connection)

    this.root.replicate(stream)

    stream.on('close', () => {
      this.connections.delete(hex)
    })

    stream.on('error', safetyCatch)

    return connection
  }

  _evictConnection(connection) {
    this.connections.delete(toHex(connection.peerId))
    connection.destroy()
  }

  async _gc(now = Date.now) {
    // TODO: dedupe

    if (this.connections.size > this._highWatermark) {
      for (const connection of this.connections.values()) {
        if (!connection.isIdle(now)) continue

        this._evictConnection(connection)

        if (this.connections.size <= this._lowWatermark) {
          break // target reached
        }
      }
    }
  }

  _startGC() {
    if (this._gcInterval) return
    this._gcInterval = setInterval(this._gc.bind(this), GC_INTERVAL)
  }
}

class Session {
  constructor(replicator, store, discoveryKey) {
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.discoveryKey = discoveryKey
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._connectPeer = this._connectPeer.bind(this)
    this.store.watch(this._oncore)

    this.peerDiscovery = new PeerDiscovery(replicator, discoveryKey, { onpeer: this._connectPeer })
    this._discoverySession = this.peerDiscovery.session({ server: true, client: true })
  }

  _oncore(core) {
    const dk = toHex(core.discoveryKey)
    if (this.cores.has(dk)) return

    const weak = new Hypercore({ core, weak: true })
    this.cores.set(dk, weak)

    weak.on('close', () => { this.cores.delete(dk) })
  }

  // Manual weak-ref, core must belong to store
  addLocal(core) {
    this._oncore(core.core)
  }

  _connectPeer(peer, data) {
    const peerId = toHex(peer.publicKey)

    if (this.replicator.suspended) return
    if (b4a.equals(peer.publicKey, this.replicator.keyPair.publicKey)) return
    if (this.replicator.connections.has(peerId)) return
    if (this.replicator._connecting.has(peerId)) return

    this.replicator._connecting.add(peerId)

    const stream = this.replicator.dht.connect(peer.publicKey, {
      keyPair: this.replicator.keyPair
    })

    stream.on('open', () => {
      this.replicator._connecting.delete(peerId)

      if (this.peerDiscovery.destroyed || this.replicator.suspended) {
        stream.destroy()
        return
      }

      this.replicator._getOrCreateConnection(peer.publicKey, stream, true)
    })

    stream.on('error', () => {
      this.replicator._connecting.delete(peerId)
    })

    stream.on('close', () => {
      this.replicator._connecting.delete(peerId)
    })
  }

  async suspend() { /* stub */ }
  async resume() { /* stub */ }

  destroy() {
    this.store.unwatch(this._oncore)
    this.cores.clear()
    this.peerDiscovery.destroy()
  }
}

class Connection {
  constructor(replicator, stream, peerId, initiator) {
    this.replicator = replicator
    this.initiator = initiator
    this.stream = stream
    this.peerId = peerId
  }

  get protomux() {
    return Hypercore.getProtocolMuxer(this.stream)
  }

  isIdle(now = Date.now) {
    if (this._protomuxSessions() === 0) return true

    return false
  }

  _protomuxSessions() {
    const mux = this.protomux

    if (mux.isIdle()) return 0

    let opened = 0
    for (const { core } of this.replicator.cores()) {
      if (core.closing) continue
      if (!mux.opened({ protocol: 'hypercore/alpha', id: core.discoveryKey })) continue
      opened++
    }

    return opened
  }

  destroy() {
    this.stream.destroy()
  }
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

function noop() {}

module.exports.PeerDiscovery = PeerDiscovery
module.exports.Connection = Connection
module.exports.Session = Session
