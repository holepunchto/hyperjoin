const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const ScopeLock = require('scope-lock')

const IDLE_TIMEOUT = 30_000

module.exports = class Replicator extends ReadyResource {
  constructor(rootStore, opts = {}) {
    super()

    const { seed, keyPair } = opts
    if (seed) this.keyPair = DHT.keyPair(seed)
    if (keyPair) this.keyPair = keyPair

    this.root = rootStore
    this.maxConnections = opts.maxConnections ?? Infinity

    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })

    this.connections = new Map() // hex(peerId) -> Connection
    this.sessions = new Map() // hex(discoveryKey) -> Session
    this._connecting = new Set() // hex(peerId) — dedup in-flight dials

    this.ready().catch(safetyCatch)
  }

  get topics() {
    return Array.from(this.sessions.values()).map((sess) => sess.topic)
  }

  async _open() {
    this.keyPair ||= await this.root.createKeyPair('replicator')

    await this.dht.ready()

    this.server = this.dht.createServer(this._onincoming.bind(this))
    await this.server.listen(this.keyPair)
  }

  async _close() {
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
    const peerId = conn.remotePublicKey
    const connection = this._getOrCreateConnection(peerId, conn, false)

    // discoveryKey unknown, adding to all sessions
    for (const session of this.sessions.values()) {
      for (const core of session.cores.values()) {
        connection.addRef(core.discoveryKey)
      }
    }
  }

  _getOrCreateConnection(peerId, stream, initiator) {
    const hex = toHex(peerId)

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

  _destroyConnection(connection) {
    this.connections.delete(toHex(connection.peerId))
    connection.destroy()
  }

  async _gc(now = Date.now) {
    while (this.connections.size > this.maxConnections) {
      // find Connection without cores
      for (const connection of this.connections.values()) {
        if (connection._protomuxSessions() !== 0) continue
        this._destroyConnection(connection)
      }
    }

    for (const connection of this.connections.values()) {
      if (!connection.isIdle(now)) continue

      this._destroyConnection(connection)
    }

    /* V2
    for (const topic of this.topics) {
      topic.reannounce().catch(safetyCatch)
    }
    */
  }
}

// a topic is a single discovery key announced + looked up on the dht
class Topic {
  constructor(session, discoveryKey) {
    this.session = session
    this.replicator = session.replicator
    this.discoveryKey = discoveryKey
    this.destroyed = false

    this._activeQuery = null
    this._queue = Promise.resolve()
    this._lock = new ScopeLock({ debounce: true })
  }

  async reannounce() {
    if (this.destroyed) return

    const proceed = await this._lock.lock()
    if (!proceed) return

    const query = this.replicator.dht.announce(
      this.discoveryKey,
      this.replicator.keyPair,
      this.replicator.server.relayAddresses
    )
    this._activeQuery = query

    try {
      for await (const data of query) {
        if (this.destroyed) return
        for (const peer of data.peers) this._connectPeer(peer)
      }

      await query.finished()
    } catch (err) {
      if (!this.destroyed) safetyCatch(err)
    } finally {
      this._activeQuery = null
      this._lock.unlock()
    }
  }

  _connectPeer(peer) {
    const peerId = toHex(peer.publicKey)

    // skip self
    if (b4a.equals(peer.publicKey, this.replicator.server.publicKey)) return

    // already connected
    if (this.replicator.connections.has(peerId)) return

    // already dialing
    if (this.replicator._connecting.has(peerId)) return
    this.replicator._connecting.add(peerId)

    const stream = this.replicator.dht.connect(peer.publicKey, { keyPair: this.replicator.keyPair })

    stream.on('open', () => {
      this.replicator._connecting.delete(peerId)

      if (this.destroyed) {
        stream.destroy()
        return
      }

      this.replicator._getOrCreateConnection(peer.publicKey, stream, true, this.discoveryKey)
    })

    stream.on('error', () => {
      this.replicator._connecting.delete(peerId)
    })

    stream.on('close', () => {
      this.replicator._connecting.delete(peerId)
    })
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    if (this._activeQuery) {
      this._activeQuery.destroy()
      this._activeQuery = null
    }
  }
}

class Connection {
  constructor(replicator, stream, peerId, initiator) {
    this.replicator = replicator
    this.initiator = initiator
    this.stream = stream
    this.peerId = peerId
    this.refs = new Map()
    this.lastActive = Date.now()
  }

  get protomux() {
    return this.stream.noiseStream.userData
  }

  addRef(discoveryKey) {
    const hex = toHex(discoveryKey)
    if (this.refs.has(hex)) return

    this.refs.set(hex, {
      discoveryKey,
      lastActive: Date.now()
    })
  }

  hasRef(discoveryKey) {
    const hex = toHex(discoveryKey)
    return this.refs.has(hex)
  }

  removeRef(discoveryKey) {
    this.refs.delete(toHex(discoveryKey))
  }

  touch(discoveryKey) {
    // TODO: reintroduce
    const ref = this.refs.get(toHex(discoveryKey))
    if (!ref) return

    const now = Date.now()
    ref.lastActive = now
    this.lastActive = now
  }

  isIdle(now = Date.now) {
    if (this.refs.size === 0) return true

    const ts = now()
    for (const ref of this.refs.values()) {
      if (ts - ref.lastActive < IDLE_TIMEOUT) return false
    }

    return true
  }

  _protomuxSessions() {
    const mux = this.protomux

    let opened = 0
    const cores = this.replicator.root.cores.map.values() // 9999+ cores?

    for (const core of cores) {
      if (core.closing) continue
      if (!mux.opened({ protocol: 'hypercore/alpha', id: core.discoveryKey })) continue
      opened++
    }

    return opened
  }

  destroy() {
    this.refs.clear()
    this.stream.destroy()
  }
}

class Session {
  constructor(replicator, store, discoveryKey) {
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.discoveryKey = discoveryKey
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this.store.watch(this._oncore)
    this._backfillOpen().catch(safetyCatch)

    this.topic = new Topic(this, discoveryKey)
    this.topic.reannounce().catch(safetyCatch)
  }

  // store watch() not triggered for already open cores
  async _backfillOpen() {
    // not sure about this
    for await (const dk of this.store.list()) {
      const core = this.replicator.root.cores.get(dk)
      if (!core) continue
      this._oncore(core)
    }
  }

  _oncore(core) {
    const dk = toHex(core.discoveryKey)
    if (this.cores.has(dk)) return

    const weak = new Hypercore({ core, weak: true })
    this.cores.set(dk, weak)

    weak.on('close', () => {
      this.cores.delete(dk)

      weak.removeListener('append', weak.__onlocalappend)

      for (const connection of this.replicator.connections.values()) {
        connection.removeRef(weak.discoveryKey)
      }
    })

    // add ref to existing connections
    for (const connection of this.replicator.connections.values()) {
      connection.addRef(weak.discoveryKey)
    }
  }

  destroy() {
    this.store.unwatch(this._oncore)
    this.cores.clear()
    this.topic.destroy()
  }
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

module.exports.Topic = Topic
module.exports.Connection = Connection
module.exports.Session = Session
