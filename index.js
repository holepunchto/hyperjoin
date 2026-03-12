const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const b4a = require('b4a')

const GC_INTERVAL = 10_000
const IDLE_TIMEOUT = 30_000

module.exports = class Replicator extends ReadyResource {
  constructor(opts = {}) {
    super()
    const { seed, keyPair = DHT.keyPair(seed) } = opts

    this.keyPair = keyPair
    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })

    this.server = this.dht.createServer((conn) => this._onincoming(conn))

    this.gcInterval = opts.gcInterval || GC_INTERVAL
    this.idleTimeout = opts.idleTimeout || IDLE_TIMEOUT
    this.sessions = new Map() // hex(peerId) -> Session
    this.topics = new Map() // hex(discoveryKey) -> Topic

    this._gcTimer = null

    this.ready().catch(safetyCatch)
  }

  async _open() {
    await this.dht.ready()
    await this.server.listen(this.keyPair)
    this._gcTimer = setInterval(() => this._gc(), this.gcInterval)
  }

  async _close() {
    clearInterval(this._gcTimer)

    for (const topic of this.topics.values()) topic.destroy()
    for (const session of this.sessions.values()) session.destroy()

    this.topics.clear()
    this.sessions.clear()

    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
  }

  // public api: start replicating a core
  async replicate(core) {
    if (!this.opened) await this.ready()
    const hex = b4a.toString(core.discoveryKey, 'hex')

    if (this.topics.has(hex)) return this.topics.get(hex)

    const topic = new Topic(this, core)
    this.topics.set(hex, topic)

    return topic.findPeers().catch(safetyCatch)
  }

  // stop replicating a core
  unreplicate(core) {
    const hex = b4a.toString(core.discoveryKey, 'hex')
    const topic = this.topics.get(hex)

    if (!topic) return

    this.topics.delete(hex)
    topic.destroy()

    for (const session of this.sessions.values()) {
      session.removeRef(core.discoveryKey)
    }
  }

  // incoming connection from a remote peer (we are the server side)
  _onincoming(conn) {
    console.log('incoming!') // todo seeing this 9 times
    const peerId = conn.remotePublicKey
    const session = this._getOrCreateSession(peerId, conn)

    // replicate all our topics over this connection
    for (const topic of this.topics.values()) {
      session.addRef(topic.discoveryKey)
      topic.replicate(session)
    }
  }

  _getOrCreateSession(peerId, connection) {
    const hex = b4a.toString(peerId, 'hex')

    let session = this.sessions.get(hex)
    if (session) return session

    session = new Session(connection, peerId, { idleTimeout: this.idleTimeout })
    this.sessions.set(hex, session)

    connection.on('close', () => {
      this.sessions.delete(hex)
    })

    connection.on('error', safetyCatch)

    return session
  }

  _gc() {
    for (const [id, session] of this.sessions) {
      if (!session.alive) {
        session.destroy()
        this.sessions.delete(id)
      }
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

    this._activeQuery = null
  }

  async findPeers() {
    if (this.destroyed) return

    // announce so other peers can find us, and get back peers that already announced
    const query = (this._activeQuery = this.replicator.dht.announce(
      this.discoveryKey,
      this.replicator.keyPair,
      this.replicator.server.relayAddresses
    ))

    console.log('looking for peers', this.discoveryKey.toString('hex'))

    try {
      for await (const data of query) {
        if (this.destroyed) return

        for (const peer of data.peers) {
          this._onpeer(peer)
        }
      }
    } catch (err) {
      if (!this.destroyed) safetyCatch(err)
    } finally {
      if (this._activeQuery === query) this._activeQuery = null
    }

    return this
  }

  _onpeer(peer) {
    const hex = b4a.toString(peer.publicKey, 'hex')

    // skip self
    if (b4a.equals(peer.publicKey, this.replicator.server.publicKey)) return

    // already connected to this peer — just add the ref
    if (this.replicator.sessions.has(hex)) {
      const session = this.replicator.sessions.get(hex)
      session.addRef(this.discoveryKey)
      this.replicate(session)
      return
    }

    const conn = this.replicator.dht.connect(peer.publicKey)

    conn.on('open', () => {
      if (this.destroyed) {
        conn.destroy()
        return
      }

      const session = this.replicator._getOrCreateSession(peer.publicKey, conn)
      session.addRef(this.discoveryKey)
      this.replicate(session)
    })

    conn.on('error', safetyCatch)
  }

  // core replication + liveness tracking to a session
  replicate(session) {
    const hex = b4a.toString(this.discoveryKey, 'hex')
    if (session.sent.has(hex)) return
    session.sent.add(hex)

    const dk = this.discoveryKey
    const core = this.core

    core.replicate(session.connection)

    const ondownload = () => session.touch(dk)
    const onsync = () => session.markIdle(dk)

    core.on('download', ondownload)
    core.on('sync', onsync)

    session.connection.on('close', () => {
      core.removeListener('download', ondownload)
      core.removeListener('sync', onsync)
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

class Session {
  constructor(connection, peerId, opts = {}) {
    this.connection = connection
    this.peerId = peerId
    this.refs = new Map() // hex(discoveryKey) -> { discoveryKey, lastActive, idle }
    this.sent = new Set() // hex(discoveryKey) — tracks which cores are already replicated
    this.lastActive = Date.now()
    this.idleTimeout = opts.idleTimeout
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
    const ref = this.refs.get(b4a.toString(discoveryKey, 'hex'))
    if (ref) ref.idle = true
  }

  get alive() {
    console.log(this.refs)
    if (this.refs.size === 0) return false

    const now = Date.now()
    for (const ref of this.refs.values()) {
      if (!ref.idle && now - ref.lastActive < this.idleTimeout) return true
    }

    return false
  }

  destroy() {
    this.refs.clear()
    this.sent.clear()
    this.connection.destroy()
  }
}

module.exports.Topic = Topic
module.exports.Session = Session
