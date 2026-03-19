const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const ScopeLock = require('scope-lock')

const IDLE_TIMEOUT = 30_000

module.exports = class Replicator extends ReadyResource {
  constructor(store, opts = {}) {
    super()

    const { seed, keyPair } = opts
    if (seed) this.keyPair = DHT.keyPair(seed)
    if (keyPair) this.keyPair = keyPair

    this.store = store

    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })

    this.server = this.dht.createServer((conn) => this._onincoming(conn))

    this.sessions = new Map() // hex(peerId) -> Session
    this.topics = new Map() // hex(discoveryKey) -> Topic

    this._connecting = new Set() // hex(peerId) — dedup in-flight dials
    this._unwatcher = null

    this.ready().catch(safetyCatch)
  }

  async _open() {
    this.keyPair ||= await this.store.createKeyPair('replicator')

    await this.dht.ready()
    await this.server.listen(this.keyPair)

    // watch the corestore for new cores, replicate them automatically
    this._unwatcher = this.store.watch((core) => this._oncore(core))
  }

  async _close() {
    if (this._unwatcher) this._unwatcher()

    for (const topic of this.topics.values()) topic.destroy()
    for (const session of this.sessions.values()) session.destroy()

    this.topics.clear()
    this.sessions.clear()

    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
  }

  // called by the store watcher when a core is opened
  _oncore(core) {
    const hex = b4a.toString(core.discoveryKey, 'hex')
    if (this.topics.has(hex)) return

    // weak ref — closes when the real core is gc'd or closed by user
    const weak = new Hypercore({ core, weak: true })

    const topic = new Topic(this, weak)
    this.topics.set(hex, topic)

    weak.on('close', () => {
      this.topics.delete(hex)
      topic.destroy()

      for (const session of this.sessions.values()) {
        session.removeRef(weak.discoveryKey)
      }
    })

    topic.reannounce().catch(safetyCatch)

    // replicate over any existing sessions
    for (const session of this.sessions.values()) {
      session.addRef(topic.discoveryKey)
      topic.replicate(session)
    }
  }

  // incoming connection from a remote peer
  _onincoming(conn) {
    const peerId = conn.remotePublicKey
    const session = this._getOrCreateSession(peerId, conn, false)

    for (const topic of this.topics.values()) {
      session.addRef(topic.discoveryKey)
      topic.replicate(session)
    }
  }

  _getOrCreateSession(peerId, connection, initiator) {
    const hex = b4a.toString(peerId, 'hex')

    let session = this.sessions.get(hex)
    if (session) return session

    session = new Session(connection, peerId, initiator)
    this.sessions.set(hex, session)

    connection.on('close', () => {
      this.sessions.delete(hex)
    })

    connection.on('error', safetyCatch)

    return session
  }

  get connections() {
    return this.sessions.size
  }

  async _gc(now = Date.now) {
    for (const [key, session] of this.sessions) {
      if (!session.isIdle(now)) continue

      session.destroy()
      this.sessions.delete(key)
    }

    for (const [_, topic] of this.topics) {
      topic.reannounce().catch(safetyCatch)
    }
  }
}

// a topic is a single discovery key announced + looked up on the dht
class Topic {
  constructor(replicator, core) {
    this.replicator = replicator
    this.core = core
    this.discoveryKey = core.discoveryKey
    this.destroyed = false

    this._cachedBump = 0
    this._activeQuery = null
    this._queue = Promise.resolve()
    // this._pendingForce = false // V2
    this._lock = new ScopeLock({ debounce: true })

    this._onlocalappend = this._onlocalappend.bind(this)
    core.on('append', this._onlocalappend)
  }

  _onlocalappend() {
    const len = this.core.length
    if (this._cachedBump >= len) return

    this.reannounce(true).catch(safetyCatch)
  }

  async reannounce(/* forceConnect = false V2 */) {
    if (this.destroyed) return
    // this._pendingForce ||= forceConnect // V2

    await this._lock.lock()

    const query = this.replicator.dht.announce(
      this.discoveryKey,
      this.replicator.keyPair,
      this.replicator.server.relayAddresses
      // { bump: this.core.length, clear: true } // V2
    )
    this._activeQuery = query

    try {
      // V1
      for await (const data of query) {
        if (this.destroyed) return
        for (const peer of data.peers) this._onpeer(peer)
      }

      /* V2
      const peers = []
      let connect = this._pendingForce

      for await (const data of query) {
        if (this.destroyed) return
        for (const p of data.peers) peers.push(p)

        // update detected from dht
        if (this._cachedBump < data.bump) {
          this._cachedBump = data.bump
          connect = true
          // TODO: session.touch(this.discoveryKey) ?
        }
      }

      if (connect) {
        for (const peer of peers) {
          this._onpeer(peer)
        }
      }
      */

      await query.finished()
    } catch (err) {
      if (!this.destroyed) safetyCatch(err)
    } finally {
      // this._pendingForce = false
      this._activeQuery = null
      this._lock.unlock()
    }
  }

  _onpeer(peer) {
    const hex = b4a.toString(peer.publicKey, 'hex')

    // skip self
    if (b4a.equals(peer.publicKey, this.replicator.server.publicKey)) return

    // already connected
    if (this.replicator.sessions.has(hex)) {
      const session = this.replicator.sessions.get(hex)
      session.addRef(this.discoveryKey)
      this.replicate(session)
      return
    }

    // already dialing
    if (this.replicator._connecting.has(hex)) return
    this.replicator._connecting.add(hex)

    const conn = this.replicator.dht.connect(peer.publicKey, { keyPair: this.replicator.keyPair })

    conn.on('open', () => {
      this.replicator._connecting.delete(hex)

      if (this.destroyed) {
        conn.destroy()
        return
      }

      const session = this.replicator._getOrCreateSession(peer.publicKey, conn, true)
      session.addRef(this.discoveryKey)
      this.replicate(session)
    })

    conn.on('error', () => {
      this.replicator._connecting.delete(hex)
    })

    conn.on('close', () => {
      this.replicator._connecting.delete(hex)
    })
  }

  replicate(session) {
    const hex = b4a.toString(this.discoveryKey, 'hex')
    if (session.sent.has(hex)) return
    session.sent.add(hex)

    const dk = this.discoveryKey
    const core = this.core

    core.replicate(session.connection)

    const touch = () => session.touch(dk)

    core.on('append', touch)
    core.on('download', touch)
    core.on('upload', touch)

    session.connection.on('close', () => {
      core.removeListener('append', touch)
      core.removeListener('download', touch)
      core.removeListener('upload', touch)
    })
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    if (this._activeQuery) {
      this._activeQuery.destroy()
      this._activeQuery = null
    }
    this.core.removeListener('append', this._onlocalappend)
  }
}

class Session {
  constructor(connection, peerId, initiator) {
    this.initiator = initiator
    this.connection = connection
    this.peerId = peerId
    this.refs = new Map()
    this.sent = new Set()
    this.lastActive = Date.now()
  }

  addRef(discoveryKey) {
    const hex = b4a.toString(discoveryKey, 'hex')
    if (this.refs.has(hex)) return

    this.refs.set(hex, {
      discoveryKey,
      lastActive: Date.now(),
      idle: false
    })
  }

  removeRef(discoveryKey) {
    this.refs.delete(b4a.toString(discoveryKey, 'hex'))
  }

  touch(discoveryKey) {
    const ref = this.refs.get(b4a.toString(discoveryKey, 'hex'))
    if (!ref) return

    const now = Date.now()
    ref.lastActive = now
    ref.idle = false
    this.lastActive = now
  }

  markIdle(discoveryKey) {
    // not used
    const ref = this.refs.get(b4a.toString(discoveryKey, 'hex'))
    if (ref) ref.idle = true
  }

  isIdle(now = Date.now) {
    if (this.refs.size === 0) return true

    const ts = now()
    for (const ref of this.refs.values()) {
      if (!ref.idle && ts - ref.lastActive < IDLE_TIMEOUT) return false
    }

    return true
  }

  destroy() {
    this.refs.clear()
    this.sent.clear()
    this.connection.destroy()
  }
}

module.exports.Topic = Topic
module.exports.Session = Session
