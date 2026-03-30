const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const Discovery = require('@holepunchto/replicator-discovery')
const PeerStation = require('./lib/station')
const { getStreamError } = require('streamx')

const GC_INTERVAL = 5_000
const MIN_PEERS = 20
const MAX_PEERS = 40
const MAX_PARALLEL = 3
const MAX_CLIENT_CONNECTIONS = Infinity // TODO: Change
const MAX_SERVER_CONNECTIONS = Infinity

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
      maxClientConnections = MAX_CLIENT_CONNECTIONS,
      maxServerConnections = MAX_SERVER_CONNECTIONS,
      maxParallel = MAX_PARALLEL,
      firewall = allowAll,
      suspended
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
      suspended
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

    this.listening = null
    this.destroyed = false
    this.suspended = suspended

    this.sessions = new Map() // hex(discoveryKey) -> Session
    this.inbound = new Map() // hex(peerId) -> Connection
    this.outbound = new Map() // hex(peerId) -> Connection
    this._inflight = new Set() // hex(peerId) -> dedup in-flight dials

    this._minPeers = minPeers
    this._maxPeers = maxPeers

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

  _selectRelay(force) {
    if (!this.relayThrough) return null
    return this.relayThrough(force, this)
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

    for (const connection of this.connections) this._evictConnection(connection)
    this.inbound.clear()
    this.outbound.clear()

    this._inflight.clear()
    this.ps.destroy()

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
    // Disabling it here for now:
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

    // this.stats.connects.server.opened++ // TODO: backport stats
    // this.stats.connects.client.closed++ // TODO: backport stats

    this.emit('_connection', connection) // testsuite only
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

    // TODO: Support async firewalling at some point. (clientside)
    if (this._handleFirewall(peerInfo.publicKey, null)) {
      // if (queued) this._flushMaybe(peerInfo)
      return
    }

    this._inflight.add(hex)

    let stream
    let error = null
    const opened = await new Promise((resolve) => {
      const onerror = (err) => { error = err }

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

      // this.stats.connects.client.attempted++ // TODO: backport stats
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
}

class Session extends ReadyResource {
  constructor(replicator, store, discoveryKey) {
    super()
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.discoveryKey = discoveryKey
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._onpeer = this._onpeer.bind(this)
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
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer
    })

    this.discovery.refresh().catch(safetyCatch)
  }

  // TODO: remove + set onpeer: this.replicator.ps.seen.bind(this.replicator.ps)
  _onpeer(peer, data) {
    this.replicator.ps.seen(peer, this.discoveryKey)

    // TODO: should invoked by station
    this.replicator._connect(peer)
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
    // TODO: ensure Peers on this topic are destroyed as well
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

  _onclose () {
    const rep = this.replicator

    const err = getStreamError(this.stream)
    this.replicator.ps.disconnected(this.peerId, err)

    const key = toHex(this.peerId)
    if (this.initiator) {
      rep.outbound.delete(key)
      // this.stats.connects.client.closed++
    } else {
      rep.inbound.delete(key)
      // this.stats.connects.server.closed++
    }

    rep.emit('_disconnect', this)
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
