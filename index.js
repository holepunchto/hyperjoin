const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const PeerStation = require('./lib/station')
const Session = require('./lib/session')
const { getStreamError } = require('streamx')

const GC_INTERVAL = 5000
const MIN_PEERS = 20
const MAX_PEERS = 40

const ERR_DUPLICATE = 'DuplicateConnection'
const ERR_IDLE = 'IdleConnection'
const ERR_DESTROYED = 'ReplicatorDestroyed'
const ERR_SUSPEND = 'SuspendDrop'

module.exports = class Replicator extends ReadyResource {
  constructor(rootStore, opts = {}) {
    super()

    const {
      seed,
      keyPair,
      relayThrough,
      minPeers = MIN_PEERS,
      maxPeers = MAX_PEERS,
      maxClientConnections,
      maxServerConnections,
      maxParallel,
      firewall = allowAll,
      suspended,
      autoswarm = false // heuristics not enabled by default
    } = opts

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

    this.ps = new PeerStation(this, {
      firewall,
      backoffs: opts.backoffs,
      jitter: opts.jitter,
      suspended,
      maxParallel,
      maxClientConnections,
      maxServerConnections
    })

    this.relayThrough = relayThrough && toRelaySelector(relayThrough)

    this.server = this.dht.createServer(
      {
        firewall: this._handleFirewall.bind(this),
        relayThrough: this._selectRelay.bind(this),
        handshakeClearWait: opts.handshakeClearWait
      },
      this._onincoming.bind(this)
    )

    this.destroyed = false
    this.suspended = suspended

    this.sessions = new Map() // hex(discoveryKey) -> Session
    this.inbound = new Map() // hex(peerId) -> Connection
    this.outbound = new Map() // hex(peerId) -> Connection
    this._inflight = new Set() // hex(peerId) -> dedup in-flight dials

    this._minPeers = minPeers
    this._maxPeers = maxPeers
    this.autoswarm = autoswarm

    this.stats = {
      updates: 0,
      bannedPeers: 0,
      connects: {
        client: {
          attempted: 0,
          opened: 0,
          closed: 0
        },
        server: {
          opened: 0,
          closed: 0
        }
      }
    }
    this.dht.on('network-change', this._handleNetworkChange.bind(this))
    this.dht.on('network-update', this._handleNetworkUpdate.bind(this))
    this.on('update', this._handleUpdate.bind(this))

    this._manual = opts.manual === true
    this._gcInterval = null
    if (!this._manual) this._startGC()
  }

  get peerId() {
    return this.keyPair && this.keyPair.publicKey
  }

  get connectionCount() {
    return this.inbound.size + this.outbound.size
  }

  get connecting() {
    return this._inflight.size
  }

  // used by backports testsuite
  get _firewall() {
    return this.ps._firewall
  }
  set _firewall(callback) {
    this.ps._firewall = callback
  }

  get connections() {
    return this._connections() // combined iterator
  }

  get peers() {
    return this.ps.peers
  }

  *_connections() {
    yield* this.inbound.values()
    yield* this.outbound.values()
  }

  async _open() {
    this.keyPair ||= await this.root.createKeyPair('replicator')
    await this.dht.ready()

    await this.server.listen(this.keyPair)
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

  _selectRelay(force) {
    if (!this.relayThrough) return null
    return this.relayThrough(force, this)
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
      this._evictConnection(connection, ERR_SUSPEND)
    }

    await this.ps.suspend({ log })

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
    await this.ps.resume({ log })

    this.suspended = false

    const pending = []

    for (const session of this.sessions.values()) {
      pending.push(session.resume({ log }))
    }

    await Promise.all(pending)

    if (!this._manual) this._startGC()
  }

  join(store, topic, { active = false, discover = false } = {}) {
    const dk = toHex(topic)
    if (this.sessions.has(dk)) throw new Error('session exists')

    active ||= !this.autoswarm && !discover // TODO: deprecated

    const session = new Session(this, store, topic, { active, discover })
    this.sessions.set(dk, session)

    session.on('close', () => this.sessions.delete(dk))
    store.on('close', session.close.bind(session))

    session.ready()
      .then(() => !this.autoswarm && session.refresh()) // TODO: deprecated
      .catch(safetyCatch)

    return session
  }

  _onincoming(conn) {
    if (this.destroyed || this.suspended) {
      // OLD-TODO: Investigate why a final server connection can be received after close
      conn.on('error', noop)
      return conn.destroy(ERR_DESTROYED)
    }

    const peerId = conn.remotePublicKey
    const hex = toHex(peerId)

    if (this.inbound.has(hex)) {
      const existing = this.inbound.get(hex).stream
      // If both connections are from the same peer,
      // - pick the new one if the existing stream is already established (has sent and received bytes),
      //   because the other client must have lost that connection and be reconnecting
      const keepNew = existing.rawBytesRead > 0 && existing.rawBytesWritten > 0

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

    // When reaching here, the connection will always be 'opened' next tick

    this._initConnection(false, peerId, conn)
  }

  // stream param expected opened stream
  _initConnection(initiator, peerId, stream, topic) {
    const hex = toHex(peerId)

    const map = initiator ? this.outbound : this.inbound

    if (map.has(hex)) throw new Error('unreachable: connection leak') // sanity check

    const connection = new Connection(this, stream, peerId, initiator)

    connection.info._connected()
    connection.info.client = initiator

    map.set(hex, connection)
    this.root.replicate(stream)

    if (initiator) this.stats.connects.client.opened++
    else this.stats.connects.server.opened++

    this.emit('connected', connection) // testsuite only
    this.emit('update')

    return connection
  }

  _evictConnection(connection, err) {
    connection.destroy(err)
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

  _handleFirewall(remotePublicKey, payload) {
    if (b4a.equals(remotePublicKey, this.keyPair.publicKey)) return true

    return this.ps.handleFirewall(remotePublicKey, payload)
  }

  async _connect(peer, queued, topic) {
    if (this.suspended) return

    const hex = toHex(peer.publicKey)
    if (b4a.equals(peer.publicKey, this.peerId)) return

    if (this.outbound.has(hex)) return
    if (this.inbound.has(hex)) return
    if (this._inflight.has(hex)) return

    const peerInfo = this.ps.lookup(peer.publicKey)

    // TODO: Support async firewalling at some point.
    if (this._handleFirewall(peerInfo.publicKey, null)) {
      return
    }

    this._inflight.add(hex)

    let stream
    let error = null
    const opened = await new Promise((resolve) => {
      const onerror = (err) => {
        error = err
      }

      const onclose = () => {
        stream.off('error', onerror)
        resolve(false)
      }

      const onopen = () => {
        stream.off('error', onerror)
        stream.off('close', onclose)
        resolve(true)
      }

      stream = this.dht.connect(peer.publicKey, {
        keyPair: this.keyPair,
        relayAddresses: peerInfo.relayAddresses,
        relayThrough: this._selectRelay(peerInfo.forceRelaying)
      })

      stream.once('error', onerror)
      stream.once('close', onclose)
      stream.once('open', onopen)

      this.stats.connects.client.attempted++
      this.emit('update')
    })

    this._inflight.delete(hex)

    if (!opened) {
      this.ps.connectFailed(peer.publicKey, error)
      this.emit('update')
      return
    }

    // stream opened - hand-off
    this._initConnection(true, peer.publicKey, stream, topic)
  }

  async _gc(now = Date.now()) {
    // Evict peer-dupes
    const [src, dst] =
      this.inbound.size < this.outbound.size
        ? [this.inbound, this.outbound]
        : [this.outbound, this.inbound]

    for (const [key, connection] of src) {
      if (!dst.has(key)) continue

      const cmp = b4a.compare(this.peerId, connection.peerId)
      if (cmp === 0) throw new Error('unreachable: self connection')

      // low-peerId keeps inbound
      const target = (cmp > 0) ^ connection.initiator ? connection : dst.get(key)

      this._evictConnection(target, ERR_DUPLICATE)
    }

    // Evict idle
    if (this.connectionCount > this._maxPeers) {
      for (const connection of this.connections) {
        if (!connection.isIdle(now)) continue

        this._evictConnection(connection, ERR_IDLE)

        if (this.connectionCount <= this._minPeers) {
          break // target reached
        }
      }
    }

    // TODO: Evict long-term idle (no bytes change)

    this.ps.collectGarbage()
  }

  _startGC() {
    if (this._gcInterval) return
    this._gcInterval = setInterval(this._gc.bind(this), GC_INTERVAL)
  }

  _handleUpdate() {
    this.stats.updates++
  }

  async refresh(now = Date.now()) {
    await this.flush() // TODO: blocks unnanounce

    let done = []
    for (const sess of this.sessions.values()) {
      done.push(sess.refresh(now))
    }
    await Promise.all(done)

    await this._gc(now)
  }

  // @deprecated, legacy compat (dht + connection queue, done)
  async flush() {
    const pending = []
    for (const sess of this.sessions.values()) {
      pending.push(sess.flush())
    }
    await Promise.all(pending)

    // pending connects
    await this.ps.flush()
  }


  async _handleNetworkUpdate() {
    if (this.dht.online === false) return
    this._handleNetworkChange()
  }

  async _handleNetworkChange() {
    if (this.suspended) return

    for (const connection of this.connections) {
      connection.stream.sendKeepAlive()
    }

    const refreshes = []

    for (const session of this.sessions.values()) {
      refreshes.push(session.discovery.refresh())
    }

    await Promise.allSettled(refreshes)
  }

  async _close() {
    this.destroyed = true

    clearInterval(this._gcInterval)
    this._gcInterval = null
    const tasks = []
    for (const session of this.sessions.values()) tasks.push(session.destroy())
    this.sessions.clear()

    for (const connection of this.connections) this._evictConnection(connection)
    this.inbound.clear()
    this.outbound.clear()

    this._inflight.clear()
    this.ps.destroy()

    await Promise.all(tasks)
    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
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
    this._onclose = this._onclose.bind(this)
    stream.on('close', this._onclose)
  }

  get protomux() {
    return Hypercore.getProtocolMuxer(this.stream)
  }

  get info() {
    return this.replicator.ps.lookup(this.peerId)
  }

  isIdle(now = Date.now()) {
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

  _onclose() {
    const rep = this.replicator

    const err = getStreamError(this.stream)

    const key = toHex(this.peerId)
    if (this.initiator) {
      rep.outbound.delete(key)
      rep.stats.connects.client.closed++
    } else {
      rep.inbound.delete(key)
      rep.stats.connects.server.closed++
    }

    this.replicator.ps.disconnected(this.peerId, err)

    rep.emit('disconnected', this)
    rep.emit('update')
  }

  destroy(err) {
    this.stream.destroy(err)
  }
}

function noop() {}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

function allowAll() {
  return false
}

function toRelaySelector(relayThrough) {
  return typeof relayThrough === 'function'
    ? relayThrough
    : (force, replicator) => (force || replicator.dht.randomized ? relayThrough : null)
}
