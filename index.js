const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const Discovery = require('@holepunchto/replicator-discovery')

const GC_INTERVAL = 5_000
const WATERMARK_LOW = 20
const WATERMARK_HIGH = 40

const ERR_DUPLICATE = 'Duplicate connection'
const ERR_DESTROYED = 'Swarm has been destroyed'

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

    this.inbound = new Map() // hex(peerId) -> Connection
    this.outbound = new Map() // hex(peerId) -> Connection
    this._inflight = new Set() // hex(peerId) -> dedup in-flight dials

    this.sessions = new Map() // hex(discoveryKey) -> Session

    this._lowWatermark = opts.lowWatermark ?? WATERMARK_LOW
    this._highWatermark = opts.highWatermark ?? WATERMARK_HIGH
    this._manual = opts.manual === true

    this._gcInterval = null
    if (!this._manual) this._startGC()
  }

  get peerId () {
    return this.keyPair && this.keyPair.publicKey
  }

  get connectionCount () {
    return this.inbound.size + this.outbound.size
  }

  get connections() { return this._connections() }
  get connecting() { return this._inflight.size }

  *_connections () {
    yield *this.inbound.values()
    yield *this.outbound.values()
  }

  async _open() {
    this.keyPair ||= await this.root.createKeyPair('replicator')
    await this.dht.ready()
  }

  *cores() {
    for (const session of this.sessions.values()) {
      for (const [discoveryKey, core] of session.cores) {
        yield { discoveryKey, core, session }
      }
    }
  }

  getConnection(peerId) {
    if (typeof peerId !== 'string') peerId = toHex(peerId)

    return this.inbound.get(peerId) || this.outbound.get(peerId)
  }

  hasConnection(peerId) {
    if (typeof peerId !== 'string') peerId = toHex(peerId)
    return !!this.getConnection(peerId) || this._inflight.has(peerId)
  }

  listen() {
    if (!this.listening) {
      if (this.destroyed) throw new Error('Replicator destroyed')

      this.listening = this.ready()
        .then(() => this.server.listen(this.keyPair))
    }

    return this.listening
  }

  async suspend({ log = noop } = {}) {
    if (this.suspended) return

    const pending = []
    const suspending = [this.server.suspend({ log })]

    for (const session of this.sessions.values()) {
      suspending.push(session.suspend({ log }))
    }

    for (const connection of this.connections) {
      pending.push(new Promise((resolve) => connection.stream.on('close', resolve)))
      connection.destroy()
    }

    this.suspended = true
    this._inflight.clear()
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

    this.suspended = false

    const pending = []

    for (const session of this.sessions.values()) {
      pending.push(session.resume({ log }))
    }

    await Promise.all(pending)

    if (!this._manual) this._startGC()
  }

  async _close() {
    this.destroyed = true

    clearInterval(this._gcInterval)
    this._gcInterval = null

    for (const session of this.sessions.values()) session.destroy()
    this.sessions.clear()

    for (const connection of this.connections) connection.destroy()
    this.inbound.clear()
    this.outbound.clear()

    this._inflight.clear()

    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
  }

  join(store, discoveryKey) {
    let session = this.sessions.get(toHex(discoveryKey))
    if (session) return session

    if (!this.suspended) this.listen().catch(safetyCatch)

    session = new Session(this, store, discoveryKey)
    const dk = toHex(discoveryKey)
    this.sessions.set(dk, session)

    session.on('close', () => this.sessions.delete(dk))
    store.on('close', session.close.bind(session))

    session.ready().catch(safetyCatch)

    return session
  }

  _onincoming(conn) {
    if (this.destroyed || this.suspended) {
      // TODO: Investigate why a final server connection can be received after close
      conn.on('error', noop)
      return conn.destroy(ERR_DESTROYED)
    }

    const peerId = conn.remotePublicKey
    const hex = toHex(peerId)

    // TODO: clean up. the expectedInitiator case is covered in _gc();
    // Disabling it here for now.
    if (this.inbound.has(hex)) {
      // const existing = this.getConnection(hex).stream
      const existing = this.inbound.get(hex).stream
      // If both connections are from the same peer,
      // - pick the new one if the existing stream is already established (has sent and received bytes),
      //   because the other client must have lost that connection and be reconnecting
      // - otherwise, pick the one thats expected to initiate in a tie break
      const existingIsOutdated = existing.rawBytesRead > 0 && existing.rawBytesWritten > 0
      // const expectedInitiator = b4a.compare(conn.publicKey, conn.remotePublicKey) > 0
      const keepNew = existingIsOutdated // || expectedInitiator === conn.isInitiator

      if (keepNew === false) {
        existing.sendKeepAlive()
        conn.on('error', noop)
        conn.destroy(new Error(ERR_DUPLICATE))
      } else {
        existing.on('error', noop)
        existing.destroy(new Error(ERR_DUPLICATE))
        this._handleServerConnectionSwap(existing, conn)
      }

      return
    }

    this._initConnection(false, peerId, conn)
  }

  _initConnection(initiator, peerId, stream) {
    const hex = toHex(peerId)

    const map = initiator ? this.outbound : this.inbound

    if (map.has(hex)) throw new Error('unreachable: connection leak') // sanity check

    // this.stats.connects.server.opened++ // TODO: backport stats

    const connection = new Connection(this, stream, peerId, initiator)
    map.set(hex, connection)

    this.root.replicate(stream)

    stream.on('close', () => {
      map.delete(hex)
      this.emit('disconnect', connection)
      this.emit('update')
    })

    stream.on('error', safetyCatch)

    this.emit('connection', connection) // TODO: rename to '_connection'; used by tests, don't misuse
    this.emit('update')

    return connection
  }

  _evictConnection(connection) {
    const key = toHex(connection.peerId)

    if (connection.initiator) {
      this.outbound.delete(key)
    } else {
      this.inbound.delete(key)
    }
    connection.destroy()
    this.emit('update')
  }

  _handleServerConnectionSwap(existing, conn) {
    let closed = false

    existing.on('close', () => {
      if (closed) return

      conn.removeListener('error', noop)
      conn.removeListener('close', onclose)

      this._onincoming(conn)
    })

    conn.on('error', noop)
    conn.on('close', onclose)

    function onclose() {
      closed = true
    }
  }

  async _gc(now = Date.now) {
    // Evict peer-dupes
    const [src, dst] = this.inbound.size < this.outbound.size
      ? [this.inbound, this.outbound]
      : [this.outbound, this.inbound]

    for (const [key, connection] of src) {
      if (!dst.has(key)) continue

      const cmp = b4a.compare(this.peerId, connection.peerId)
      if (cmp === 0) throw new Error('unreachable: self connection')

      // low-peerId keeps inbound
      const target = (cmp > 0) ^ connection.initiator
        ? connection
        : dst.get(key)

      this._evictConnection(target)
    }

    // Evict idle
    if (this.connectionCount > this._highWatermark) {
      for (const connection of this.connections) {
        if (!connection.isIdle(now)) continue

        this._evictConnection(connection)

        if (this.connectionCount <= this._lowWatermark) {
          break // target reached
        }
      }
    }

    // TODO: Evict long-term idle (no bytes change)
  }

  _startGC() {
    if (this._gcInterval) return
    this._gcInterval = setInterval(this._gc.bind(this), GC_INTERVAL)
  }
}

class Session extends ReadyResource {

  constructor(replicator, store, discoveryKey) {
    super()
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.discoveryKey = discoveryKey
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._connectPeer = this._connectPeer.bind(this)
    this.store.watch(this._oncore)

    this.discovery = null
  }

  async _open() {
    const { replicator, discoveryKey } = this

    await replicator.ready()

    this.discovery = new Discovery(replicator.dht, discoveryKey, {
      announce: true,
      wait: replicator.listening,
      suspended: replicator.suspended,
      keyPair: replicator.keyPair,
      onpeer: this._connectPeer,
      relayAddresses: () => replicator.server.relayAddresses,
    })

    this.discovery.refresh().catch(safetyCatch)
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
    if (b4a.equals(peer.publicKey, this.replicator.peerId)) return
    if (this.replicator.outbound.has(peerId)) return
    if (this.replicator.inbound.has(peerId)) return
    if (this.replicator._inflight.has(peerId)) return

    this.replicator._inflight.add(peerId)
    const clearInflight = (err) => {
      this.replicator._inflight.delete(peerId)
      if (!err) this.replicator.emit('update')
    }

    const stream = this.replicator.dht.connect(peer.publicKey, {
      keyPair: this.replicator.keyPair,
      // relayAddresses: peerInfo.relayAddresses
      // relayThrough
    })

    this.replicator.emit('update')

    stream.on('open', () => {
      clearInflight()
      stream.off('error', clearInflight)
      stream.off('close', clearInflight)

      if (this.discovery.destroyed || this.replicator.suspended) {
        stream.destroy()
        return
      }

      this.replicator._initConnection(true, peer.publicKey, stream)
    })

    stream.on('error', clearInflight)
    stream.on('close', clearInflight)
  }

  async suspend({ log = noop } = {}) {
    await this.discovery.suspend({ log })
  }

  async resume({ log = noop } = {}) {
    await this.discovery.resume()
  }

  async _close() {
    this.store.unwatch(this._oncore)
    this.cores.clear()
    await this.discovery.destroy()
  }

  async destroy() {
    return this.close()
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

module.exports.PeerDiscovery = Discovery
module.exports.Connection = Connection
module.exports.Session = Session
