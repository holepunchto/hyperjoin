const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const Dialer = require('./lib/dialer')
const Session = require('./lib/session')
const { getStreamError } = require('streamx')

const AUTO_REFRESH_INTERVAL = 5000

// sessions
const DEFAULT_AUTO_SLOTS = 8
const DEFAULT_SWARM_GC_IDLE_TIME = 30 * 1000

const DEFAULT_DISCOVERY_TIMEOUT = 60 * 1000
const UPDATED_DISCOVERY_TIMEOUT = 10 * 1000
const DISCOVERY_STARTUP_DELAY = 5 * 1000 // 5s
const AUTO_STARTUP_DELAY = 5 * 1000 // 5s
const MIN_ACTIVE_TIME = 30 * 1000

// connections
const MIN_PEERS = 20
const MAX_PEERS = 40
const NOMAD_THRESHOLD = 60 * 1000

const ERR_DUPLICATE = 'DuplicateConnection'
const ERR_IDLE = 'IdleConnection'
const ERR_DESTROYED = 'ReplicatorDestroyed'
const ERR_SUSPEND = 'SuspendDrop'
const ERR_MAXPEERS = 'MaxPeersCap'

module.exports = class Replicator extends ReadyResource {
  constructor(rootStore, opts = {}) {
    super()

    const {
      seed,
      keyPair,
      relayThrough,
      minPeers = MIN_PEERS,
      maxPeers = MAX_PEERS,
      maxParallel,
      nomad = NOMAD_THRESHOLD,
      firewall = allowAll,
      suspended,
      autoswarm = true,
      manual = false,
      interval = AUTO_REFRESH_INTERVAL,
      gcDisabled = false,

      // Discovery
      defaultDiscoveryTimeout = DEFAULT_DISCOVERY_TIMEOUT,
      updatedDiscoveryTimeout = UPDATED_DISCOVERY_TIMEOUT,
      discoveryStartupDelay = DISCOVERY_STARTUP_DELAY,

      // Idle
      gcIdleTime = DEFAULT_SWARM_GC_IDLE_TIME,

      // autoswarm
      slots = DEFAULT_AUTO_SLOTS,
      autoStartupDelay = AUTO_STARTUP_DELAY,
      minActiveTime = MIN_ACTIVE_TIME
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

    this.dialer = new Dialer(this, {
      firewall,
      backoffs: opts.backoffs,
      jitter: opts.jitter,
      suspended,
      maxParallel
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
    this._nomad = nomad
    this._lastConnectionEvict = 0

    this.autoswarm = autoswarm
    this._autoRefreshInterval = interval
    this._gcIdleTime = gcIdleTime
    this.slots = slots
    this._autoStartupDelay = autoStartupDelay
    this._minActiveTime = minActiveTime

    this._defaultDiscoveryTimeout = defaultDiscoveryTimeout
    this._updatedDiscoveryTimeout = updatedDiscoveryTimeout
    this._discoveryStartupDelay = discoveryStartupDelay

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
    this.root.once('close', this._onrootclose.bind(this))

    this._gcDisabled = gcDisabled
    this._manual = !!manual
    this._refreshInterval = null
    if (!this._manual) this._startAutoRefresh()
  }

  get peerId() {
    return this.server.publicKey
  }

  get connectionCount() {
    return this.inbound.size + this.outbound.size
  }

  get connecting() {
    return this._inflight.size
  }

  // used by backports testsuite
  get _firewall() {
    return this.dialer._firewall
  }
  set _firewall(callback) {
    this.dialer._firewall = callback
  }

  get connections() {
    return this._connections() // combined iterator
  }

  get peers() {
    return this.dialer.peers
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
      pending.push(this._evictConnection(connection, ERR_SUSPEND))
    }

    this.dialer.suspend({ log })

    this.suspended = true
    this._inflight.clear()
    this._stopAutoRefresh()

    await Promise.allSettled(suspending)

    await this.dht.suspend({ log })

    await Promise.all(pending)
  }

  async resume({ log = noop } = {}) {
    if (!this.suspended) return

    await this.dht.resume({ log })
    await this.server.resume({ log })
    this.dialer.resume({ log })

    this.suspended = false

    const pending = []

    for (const session of this.sessions.values()) {
      pending.push(session.resume({ log }))
    }

    await Promise.all(pending)

    if (!this._manual) this._startAutoRefresh()
  }

  join(store, topic, { priority = 0, active = false, discover = false } = {}) {
    const dk = toHex(topic)
    if (this.sessions.has(dk)) throw new Error('session exists')

    active ||= !this.autoswarm && !discover // TODO: deprecated

    const session = new Session(this, store, topic, { priority, active, discover })
    this.sessions.set(dk, session)

    session.on('close', () => {
      this.sessions.delete(dk)
      this.emit('remove', session)
    })

    store.on('close', session.close.bind(session))

    session
      .ready()
      .then(() => !this.autoswarm && session.refresh()) // TODO: deprecated
      .catch(safetyCatch)

    return session
  }

  _onincoming(conn) {
    if (this._isTearingdown) {
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

    // Hard limit max-drop
    if (this.connectionCount >= this._maxPeers + 5) {
      conn.on('error', noop)
      return conn.destroy(ERR_MAXPEERS)
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

  async _evictConnection(connection, err) {
    if (connection.stream.closed) return
    const closed = new Promise((resolve) => connection.stream.once('close', resolve))
    connection.destroy(err)
    await closed
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

    return this.dialer.handleFirewall(remotePublicKey, payload)
  }

  get _isTearingdown() {
    return (
      this.destroyed || // replicator begin-teardown
      this.suspended || // replicator
      this.closed || // ready-resource fully torn down
      this.closing
    ) // ready-resource synonymous w/ destroyed
  }

  async _connect(peer, queued, topic) {
    if (this._isTearingdown) return

    const hex = toHex(peer.publicKey)
    if (b4a.equals(peer.publicKey, this.peerId)) return

    if (this.outbound.has(hex)) return
    if (this.inbound.has(hex)) return
    if (this._inflight.has(hex)) return

    const peerInfo = this.dialer.lookup(peer.publicKey)

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

    // opened after teardown
    if (this._isTearingdown) {
      stream.once('error', noop)
      stream.destroy(new Error(ERR_DESTROYED))
      return
    }

    if (!opened) {
      this.dialer.connectFailed(peer.publicKey, error)
      this.emit('update')
      return
    }

    // stream opened - hand-off
    this._initConnection(true, peer.publicKey, stream, topic)
  }

  async _gc(now = Date.now()) {
    if (this._gcDisabled) return

    const evictions = []
    let connectionsEvicted = 0
    let sessionsEvicted = 0

    // peer-dedupe

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

      evictions.push(this._evictConnection(target, ERR_DUPLICATE))
      connectionsEvicted++
    }

    // GC idle sessions

    for (const session of this.sessions.values()) {
      if (!session._shouldEvict(now)) continue
      evictions.push(session.destroy())
      sessionsEvicted++
    }

    // GC Connections

    if (!this._lastConnectionEvict) this._lastConnectionEvict = now

    const gcConn = (connection, reason = ERR_IDLE) => {
      connection._evicted = true
      evictions.push(this._evictConnection(connection, reason))
      connectionsEvicted++
      this._lastConnectionEvict = now
    }

    if (this.connectionCount > this._maxPeers) {
      const nEvict = this.connectionCount - this._maxPeers

      // Evict idle connections
      for (const connection of this.connections) {
        if (!connection.isIdle(now)) continue

        gcConn(connection, ERR_IDLE)

        if (connectionsEvicted >= nEvict) {
          break // target reached
        }
      }

      // Heuristic evict if still over max-cap
      const nHeuristicEvict = nEvict - connectionsEvicted

      if (nHeuristicEvict > 0) {
        // TODO: avoid eviction for connection:
        // - from pinned topic
        // - good throughput
        const connections = Array.from(this.connections)
          .filter((c) => !c._evicted)
          .sort((a, b) => {
            const aRookie = a._survived < 1
            const bRookie = b._survived < 1

            // Evict rookie over established
            if (aRookie !== bRookie) return aRookie ? -1 : 1

            // Evict newest, comparing 2 rookies.
            if (aRookie) return b.connectedTime - a.connectedTime

            // Evict stale, comparing 2 established
            return a.lastActivity - b.lastActivity
          })

        for (let i = 0; i < nHeuristicEvict; i++) {
          gcConn(connections[i], ERR_MAXPEERS)
        }
      }
    }

    // Force peer rotation when fully saturated over extend period of time.
    const treshold = this._nomad + Math.random() * Math.max(this._nomad, 10000)
    if (
      !connectionsEvicted && // nothing evicted
      this.connectionCount === this._maxPeers && // at max cap
      now - this._lastConnectionEvict > treshold // topology growing stale
    ) {
      const connections = Array.from(this.connections).filter((c) => !c._evicted)
      const connection = connections[Math.floor(Math.random() * connections.length)]

      if (connection) {
        gcConn(connection, ERR_IDLE)
      }
    }

    for (const connection of this.connections) {
      if (connection._evicted) continue
      connection._survived++
    }

    await Promise.all(evictions)

    // GC Peer cache

    this.dialer.collectGarbage()

    if (sessionsEvicted) this.emit('evicted', sessionsEvicted)
    if (connectionsEvicted) this.emit('dropped', connectionsEvicted)
  }

  _handleUpdate() {
    this.stats.updates++
  }

  _onrootclose() {
    if (this.destroyed || this.closing) return
    this.destroy().catch((err) => this.emit('error', err))
  }

  async refresh(now = Date.now()) {
    await this.flush() // TODO: blocks unnanounce

    this._autoselect(now) // updates sessions

    const done = []

    for (const sess of this.sessions.values()) {
      if (sess.closing || sess.closed) continue
      done.push(sess.refresh(now)) // applies state
    }

    for (const conn of this.connections) {
      conn.refresh(now)
    }

    await Promise.all(done)

    await this._gc(now)
  }

  // @deprecated, legacy compat (dht + connection queue, done)
  async flush() {
    const pending = []
    for (const sess of this.sessions.values()) {
      if (sess.closing || sess.closed || sess.state === 'destroyed') continue
      pending.push(sess.flush())
    }
    await Promise.all(pending)

    // pending connects
    await this.dialer.flush()
  }

  /**
   * Autoswarm policies
   */
  _autoselect(now = Date.now()) {
    const pool = []
    let slots = this.slots

    for (const session of this.sessions.values()) {
      if (session.isDiscovering) continue
      if (session.isPinned) continue
      if (session.closing || session.closed) continue

      // don't deactivate pending nor newly-started topics
      if (session.isAuto) {
        const timeActive = now - session._startTime
        if (timeActive < session._startDelay + this._minActiveTime) {
          slots-- // reserved & immovable
          continue
        }
      }

      pool.push(session)
    }

    if (slots < 0) throw new Error('Assertfail: negative slots')

    // weight prio by lexical order
    const order = new Map()
    const priorities = pool.map((i) => i.priority).sort()
    for (let i = 0; i < priorities.length; i++) {
      order.set(priorities[i], i)
    }

    function score(session) {
      let v = 0
      v += order.get(session.priority) // primary

      // secondary; rotate over time by last start
      if (session.isAuto) {
        const uptime = now - session._startTime
        v -= (uptime / 60_000) * 0.5
      }

      return v
    }

    function heuristicSort(a, b) {
      return score(b) - score(a)
    }

    // rotate
    pool.sort(heuristicSort)

    // autostop
    for (let i = slots; i < pool.length; i++) {
      pool[i]._autoStop(now)
    }

    let active = 0
    for (const session of pool) if (session.active) active++
    if (active >= slots) return // no-op

    // autostart
    const limit = Math.min(slots - active, pool.length)
    for (let i = 0; i < limit; i++) {
      pool[i]._autoStart(now)
    }
  }

  async _handleNetworkUpdate() {
    if (this.dht.online === false) return
    await this._handleNetworkChange()
  }

  async _handleNetworkChange() {
    if (this.suspended) return

    for (const connection of this.connections) {
      connection.stream.sendKeepAlive()
    }

    await this.refresh()
  }

  _stopAutoRefresh() {
    if (!this._refreshInterval) return
    clearInterval(this._refreshInterval)
    this._refreshInterval = null
  }

  _startAutoRefresh() {
    if (this._refreshInterval) return
    this._refreshInterval = setInterval(
      () => this.refresh().catch(safetyCatch),
      this._autoRefreshInterval
    )
    this._refreshInterval.unref()
  }

  async _close() {
    this.destroyed = true

    this.root.removeListener('close', this._onrootclose)
    this._stopAutoRefresh()

    const tasks = []
    for (const session of this.sessions.values()) {
      tasks.push(session.destroy())
    }
    this.sessions.clear()

    for (const connection of this.connections) {
      tasks.push(this._evictConnection(connection))
    }

    this.inbound.clear()
    this.outbound.clear()

    this._inflight.clear()
    this.dialer.destroy()

    await Promise.all(tasks)
    await this.server.close()
    if (!this.dht.destroyed) await this.dht.destroy()
  }

  async destroy() {
    await this.close()
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

    this._evicted = false
    this.bytesRead = -1
    this.bytesWritten = -1
    this.lastActivity = -1
    this.refresh()
    this._survived = 0 // incremented on gc
  }

  get protomux() {
    return Hypercore.getProtocolMuxer(this.stream)
  }

  get info() {
    return this.replicator.dialer.lookup(this.peerId)
  }

  get connectedTime() {
    return this.info.connectedTime
  }

  isIdle(now = Date.now()) {
    if (this._protomuxSessions() === 0) return true

    return false
  }

  _protomuxSessions() {
    const mux = this.protomux

    let opened = 0

    for (const ch of mux) {
      if (ch.protocol !== 'hypercore/alpha') continue
      const dkeystr = toHex(ch.id)

      for (const session of this.replicator.sessions.values()) {
        if (!session.active) continue

        if (session.cores.has(dkeystr)) {
          opened++
          break
        }
      }
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

    this.replicator.dialer.disconnected(this.peerId, err)

    rep.emit('disconnected', this)
    rep.emit('update')
  }

  refresh(now = Date.now()) {
    if (
      this.bytesRead === this.stream.rawBytesRead &&
      this.bytesWritten === this.stream.rawBytesWritten
    ) {
      return
    }

    this.bytesRead = this.stream.rawBytesRead
    this.bytesWritten = this.stream.rawBytesWritten
    this.lastActivity = now
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
