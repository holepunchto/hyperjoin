const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const Dialer = require('./lib/dialer')
const Connection = require('./lib/connection')
const Session = require('./lib/session')
const Scanner = require('./lib/scan.js')
const safetyCatch = require('safety-catch')
const Discovery = require('hyperannounce')

const b4a = require('b4a')
const { sortConnections } = require('./lib/heuristics')
const { toHex } = b4a

const AUTO_REFRESH_INTERVAL = 5000

// sessions
const POOL_SLOTS = 8
const SESSION_MAX_IDLE = 30 * 1000
const SESSION_MIN_ACTIVE = 30 * 1000

// connections
const MIN_PEERS = 20
const MAX_PEERS = 40

const ERR_DUPLICATE = 'DuplicateConnection'
const ERR_IDLE = 'IdleConnection'
const ERR_DESTROYED = 'HyperjoinDestroyed'
const ERR_SUSPEND = 'SuspendDrop'
const ERR_MAXPEERS = 'MaxPeersCap'

module.exports = class Hyperjoin extends ReadyResource {
  constructor(onreplicate, topicOf, opts = {}) {
    super()

    if (typeof onreplicate !== 'function') throw new Error('function `onreplicate` required')
    if (typeof topicOf !== 'function') throw new Error('function `topicOf` required')

    const {
      seed = null,
      keyPair = DHT.keyPair(seed),

      relayThrough,
      minPeers = MIN_PEERS,
      maxPeers = MAX_PEERS,
      maxParallel,
      firewall = allowAll,
      suspended,
      pool = true,
      manual = false,
      interval = AUTO_REFRESH_INTERVAL,
      gcDisabled = false,

      // Idle
      gcIdleTime = SESSION_MAX_IDLE,

      // autoswarm
      slots = POOL_SLOTS,
      minActiveTime = SESSION_MIN_ACTIVE
    } = opts

    this.keyPair = keyPair
    this._onreplicate = onreplicate
    this._topicOf = topicOf

    this._dhtOwned = !opts.dht
    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })

    this._firewall = firewall
    this._safeIgnore = this._safeIgnore.bind(this)

    const handlers = {
      ondial: (peer) => this._connect(peer),
      ondialerror: this._safeIgnore,
      isConnected: (key) => this.hasConnection(key),
      canRelay: () => !!this.relayThrough,
      want: () => {
        if (this._isTearingdown) return 0

        return this._maxPeers - this.connectionCount // throttle
      }
    }

    this.dialer = new Dialer(handlers, {
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

    this.suspended = suspended
    this._presuspended = false
    this._suspending = Promise.resolve()
    this.listening = false
    this._pendingListen = null

    this.sessions = new Map() // hex(discoveryKey) -> Session
    this.scans = new Map() // hex(discoveryKey) -> Scanner
    this.inbound = new Map() // hex(peerId) -> Connection
    this.outbound = new Map() // hex(peerId) -> Connection

    this._inflight = new Set() // hex(peerId), dedup in-flight dials
    this._inflightCancel = new Set() // cancelCallbacks
    this._minPeers = minPeers
    this._maxPeers = maxPeers
    this._lastConnectionEvict = 0

    this.slots = slots
    this.pool = pool
    this._autoRefreshInterval = interval
    this._gcIdleTime = gcIdleTime
    this._minActiveTime = minActiveTime
    this._gcPending = null

    this._online = this.dht.online
    this._offlineAt = 0

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
      },
      dht: {
        lookup: 0,
        announce: 0,
        unannounce: 0
      }
    }

    this._handleNetworkChange = this._handleNetworkChange.bind(this)
    this.dht.on('network-change', this._handleNetworkChange)

    this._handleNetworkUpdate = this._handleNetworkUpdate.bind(this)
    this.dht.on('network-update', this._handleNetworkUpdate)

    this.on('update', this._handleUpdate.bind(this))

    this._gcDisabled = gcDisabled
    this._manual = !!manual
    this._refreshInterval = null
    if (!this._manual) this._startAutoRefresh()
  }

  _safeIgnore(err) {
    // if and only if an error occurs here
    // for any other reason than teardown
    // it must be addressed and not ignored
    if (!this._isTearingdown) {
      this.emit('error', err)
    }

    safetyCatch(err)
  }

  get peerId() {
    if (this.keyPair) return this.keyPair.publicKey

    return null
  }

  get connectionCount() {
    return this.inbound.size + this.outbound.size
  }

  get connecting() {
    return this._inflight.size
  }

  get connections() {
    return this._connections() // combined iterator
  }

  get peers() {
    return this.dialer.peers
  }

  get isPresuspended() {
    return this._presuspended
  }

  get destroyed() {
    return !!(this.closing || this.closed)
  }

  get collected() {
    return this._gcPending || Promise.resolve()
  }

  *_connections() {
    yield* this.inbound.values()
    yield* this.outbound.values()
  }

  async _open() {
    this._pendingListen = this._safeListen()
  }

  async _safeListen() {
    try {
      await this.server.listen(this.keyPair)
      this.listening = true

      this.refresh()
    } catch (err) {
      this._safeIgnore(err)
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

  get activeSessions() {
    const active = []
    for (const session of this.sessions.values()) {
      if (!session.active || session._suspended) continue
      active.push(session)
    }
    return active
  }

  get announcedSessions() {
    const announced = []
    for (const session of this.sessions.values()) {
      if (!session.isAnnounced) continue
      announced.push(session)
    }
    return announced
  }

  _selectRelay(force) {
    if (!this.relayThrough) return null
    return this.relayThrough(force, this)
  }

  suspend({ log = noop } = {}) {
    if (!this.suspended) {
      this.suspended = true

      this._suspending = this._beginSuspension({ log })
    }

    return this._suspending
  }

  async _beginSuspension({ log = noop } = {}) {
    this._cancelInflight()
    await this.flush()

    const pending = []
    const suspending = [this.server.suspend({ log }), this.dialer.suspend()]

    for (const session of this.sessions.values()) {
      suspending.push(session.suspend({ log }))
    }

    for (const connection of this.connections) {
      connection.info.resetAttempts()
      pending.push(this._evictConnection(connection, ERR_SUSPEND))
    }

    for (const scan of this.scans.values()) {
      suspending.push(scan.suspend({ log }))
    }

    this._inflight.clear()
    this._stopAutoRefresh()

    await Promise.allSettled(suspending)

    if (this._dhtOwned) await this.dht.suspend({ log })

    await Promise.all(pending)
  }

  // unannounce sessions
  presuspend() {
    if (this._presuspended) return

    this._presuspended = true
    this.refresh()
  }

  async resume({ log = noop, presuspended = false } = {}) {
    if (this._presuspended !== presuspended) {
      this._presuspended = presuspended
      this.refresh()
    }

    if (!this.suspended) return

    if (this._dhtOwned) await this.dht.resume({ log })

    await this.server.resume({ log })

    this.suspended = false

    this.dialer.resume()

    const pending = []

    for (const session of this.sessions.values()) {
      pending.push(session.resume({ log }))
    }

    for (const scan of this.scans.values()) {
      scan.resume()
    }

    await Promise.all(pending)

    if (!this._manual) this._startAutoRefresh()

    this.refresh()
  }

  join(
    topic,
    { priority = 0, pin = false, delay = 5000, deferSwarming = true /* deprecated */ } = {}
  ) {
    const dk = toHex(topic)

    const existing = this.sessions.get(dk)

    let wait = null
    if (existing) {
      if (existing.closing === null) throw new Error('session exists')

      // Replace closing session but defer announcing until it finishes closing
      this.sessions.delete(dk)
      wait = existing.closing
    }

    if (deferSwarming === false) {
      delay = 0
    }

    const session = new Session(this, topic, {
      priority,
      pin,
      wait,
      delay
    })

    this.sessions.set(dk, session)

    session.on('close', () => {
      if (this.sessions.get(dk) === session) this.sessions.delete(dk)
      this.emit('leave', session)
    })

    if (session.active) this.dialer.refTopic(dk)

    session.on('state', (state) => {
      if (session.active) {
        this.dialer.refTopic(dk)
      } else if (state === 'inactive') {
        this.dialer.unrefTopic(dk)
      }
    })

    this.emit('join', session)

    return session
  }

  leave(topic) {
    const session = this.sessions.get(toHex(topic))
    if (!session) return

    return session.destroy()
  }

  pin(topic) {
    const session = this.sessions.get(toHex(topic))
    if (!session) return

    session.pin()
  }

  unpin(topic) {
    const session = this.sessions.get(toHex(topic))
    if (!session) return

    session.unpin()
  }

  bump(topic, priority, dhtValue = -1) {
    const session = this.sessions.get(toHex(topic))
    if (!session) return

    session.priority = priority
    session.bump(dhtValue)
  }

  async lookup(topic) {
    // TODO: reuse existing discovery w/ node-cache
    // scans and announcing session are currently safe,
    // inactive pooled session not safe - causes side-effects.
    // using naive & safe new construction for now:
    const discovery = new Discovery(this.dht, topic, {
      announce: false,
      onstats: () => {
        this.stats.dht.lookup++
      },
      onerror: (err) => this._safeIgnore(err)
    })

    discovery.refresh()
    await discovery.flush()
    const value = discovery.bump.seen
    await discovery.destroy()

    return value
  }

  scan(
    topic,
    {
      limit = 1,
      delay = 3000,
      timeout = 60000,
      force = false,
      deferSwarming = true /* deprecated */
    } = {}
  ) {
    if (this._isTearingdown) return

    const key = toHex(topic)

    if (deferSwarming === false) delay = 0

    const existing = this.scans.get(key)
    if (existing && !existing.destroyed) {
      if (force) {
        existing._forceRefresh(delay)
      }

      return existing
    }

    const onpeer = (peer) => {
      // ignore self
      if (b4a.equals(peer.publicKey, this.peerId)) return

      const info = this.dialer.seen(peer, topic)
      info.scannedAt = Date.now()
    }

    const scan = new Scanner(this.dht, topic, {
      ready: this.ready(),
      limit,
      delay,
      timeout,
      suspended: this.suspended,
      onpeer,
      onlookup: () => {
        this.stats.dht.lookup++
      },
      onerror: (err) => this._safeIgnore(err)
    })

    this.scans.set(key, scan)

    this.dialer.refTopic(topic)

    scan.start()

    this.emit('scan', topic)

    scan.complete.then(() => {
      this.dialer.unrefTopic(topic)

      if (this.scans.get(key) !== scan) return
      this.scans.delete(key)

      this.emit('scanned', topic)
    })

    return scan
  }

  _onincoming(conn) {
    if (this._isTearingdown) {
      conn.on('error', noop)
      return conn.destroy(ERR_DESTROYED)
    }

    const peerId = conn.remotePublicKey
    const hex = toHex(peerId)

    const peerInfo = this.dialer.lookup(peerId)
    const initiator = false
    const attemptDone = peerInfo.markAttempt(initiator)

    if (this.inbound.has(hex)) {
      const existing = this.inbound.get(hex).stream
      // If both connections are from the same peer,
      // - pick the new one if the existing stream is already established (has sent and received bytes),
      //   because the other client must have lost that connection and be reconnecting
      const keepNew = existing.rawBytesRead > 0 && existing.rawBytesWritten > 0

      if (keepNew === false) {
        attemptDone(true, ERR_DUPLICATE)

        existing.sendKeepAlive()
        conn.on('error', noop)
        conn.destroy(new Error(ERR_DUPLICATE))
      } else {
        attemptDone()

        existing.on('error', noop)
        existing.destroy(new Error(ERR_DUPLICATE))
        this._handleServerConnectionSwap(existing, conn)
      }

      return
    }

    // Hard limit max-drop
    if (this.connectionCount >= this._maxPeers + 5) {
      attemptDone(true, ERR_MAXPEERS)
      conn.on('error', noop)
      return conn.destroy(ERR_MAXPEERS)
    }

    attemptDone()

    // connection 'opened' on next tick
    this._initConnection(false, peerId, conn)
  }

  // `stream` expected to be fully opened
  _initConnection(initiator, peerId, stream) {
    const hex = toHex(peerId)

    const map = initiator ? this.outbound : this.inbound

    if (map.has(hex)) throw new Error('unreachable: connection leak') // sanity check

    const connection = new Connection(this, stream, peerId, initiator)

    map.set(hex, connection)

    this._onreplicate(connection.stream)
    connection._watchChannels()

    if (initiator) this.stats.connects.client.opened++
    else this.stats.connects.server.opened++

    this.emit('connected', connection)

    const peer = this.dialer.lookup(peerId)
    for (const [topic, scan] of this.scans) {
      if (peer._topics.has(topic)) scan.resolveConnected()
    }

    this.emit('update')

    return connection
  }

  async _evictConnection(connection, err) {
    if (connection.destroying) return connection.destroyed

    // TODO: make public / only used by sim
    this.emit('_evictConnection', connection.peerId, /* reason */ err)

    connection.destroy(err)
    await connection.destroyed
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

  _handleBan(peer) {
    this.stats.bannedPeers++
    this.emit('ban', peer, peer.banReason)
  }

  async _handleFirewall(remotePublicKey, payload) {
    // firewall self
    if (b4a.equals(remotePublicKey, this.keyPair.publicKey)) return true

    const peer = this.dialer.lookup(remotePublicKey)
    if (peer.isBanned) return true

    const firewalled = await this._firewall(remotePublicKey, payload)

    if (firewalled) {
      peer.setBanned('firewalled')

      const { banned } = this.dialer.checkBans()
      for (const p of banned) this._handleBan(p)
    }

    return firewalled
  }

  get _isTearingdown() {
    return (
      this.suspended || // hyperjoin
      this.closing ||
      this.closed
    )
  }

  async _connect(peer, topic) {
    if (this._isTearingdown) return true

    const hex = toHex(peer.publicKey)
    if (b4a.equals(peer.publicKey, this.peerId)) return true

    if (this.outbound.has(hex)) return true
    if (this.inbound.has(hex)) return true
    if (this._inflight.has(hex)) return true

    const peerInfo = this.dialer.lookup(peer.publicKey)

    this._inflight.add(hex)

    let firewalled = true
    try {
      firewalled = await this._handleFirewall(peerInfo.publicKey, null)
    } finally {
      // userhandler threw or firewalled - clear ref
      if (firewalled) this._inflight.delete(hex)
    }

    if (firewalled) return true

    if (this._isTearingdown) {
      this._inflight.delete(hex)
      return ERR_SUSPEND
    }

    let stream
    let error = null
    let cancelCallback = null

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

      cancelCallback = () => stream.destroy(ERR_SUSPEND)
      this._inflightCancel.add(cancelCallback)

      this.stats.connects.client.attempted++
      this.emit('update')
    })

    this._inflight.delete(hex)
    if (cancelCallback) this._inflightCancel.delete(cancelCallback)

    if (!opened) {
      this.emit('update')
      return error || true
    }

    // opened after teardown
    if (this._isTearingdown) {
      stream.once('error', noop)
      const e = new Error(ERR_DESTROYED)
      stream.destroy(e)
      return ERR_DESTROYED
    }

    // stream opened - hand-off
    this._initConnection(true, peer.publicKey, stream, topic)

    return false
  }

  async _gc(now = Date.now()) {
    if (this._isTearingdown) return

    const evictions = []
    let connectionsEvicted = 0
    let sessionsEvicted = 0

    let active = 0
    for (const connection of this.connections) {
      if (!connection.destroying) active++
    }

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
      if (target.destroying) continue

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
      evictions.push(this._evictConnection(connection, reason))
      connectionsEvicted++
      this._lastConnectionEvict = now
    }

    const evict = active - this._maxPeers

    if (evict > 0) {
      // Evict idle connections
      for (const connection of this.connections) {
        if (connectionsEvicted >= evict) {
          break // target reached
        }

        if (connection.destroying) continue
        if (!connection.isIdle(now)) continue

        gcConn(connection, ERR_IDLE)
      }

      // Last resort if still over max capacity
      const nHeuristicEvict = evict - connectionsEvicted

      if (nHeuristicEvict > 0) {
        const remaining = Array.from(this.connections).filter((c) => !c.destroying)
        const sorted = sortConnections(remaining, now)

        for (let i = 0; i < nHeuristicEvict; i++) {
          gcConn(sorted[i], ERR_MAXPEERS)
        }
      }
    }

    for (const connection of this.connections) {
      if (connection.destroying) continue
      connection._survived++
    }

    await Promise.all(evictions)

    if (sessionsEvicted) this.emit('evicted', sessionsEvicted)

    if (connectionsEvicted) this.emit('dropped', connectionsEvicted)
  }

  _handleUpdate() {
    this.stats.updates++
  }

  refresh(now = Date.now()) {
    this._autoselect(now) // updates sessions

    for (const sess of this.sessions.values()) {
      if (sess.closing || sess.closed) continue
      sess.refresh(now) // applies state
    }

    for (const conn of this.connections) {
      conn.refresh(now)
    }

    if (!this._gcDisabled) this.gc(now)

    this.dialer.refresh(now)

    const { banned } = this.dialer.checkBans()
    for (const p of banned) this._handleBan(p)
  }

  gc(now = Date.now()) {
    if (this._gcPending || this._isTearingdown) return this.collected

    const nextTick = async () => {
      try {
        await new Promise((resolve) => setImmediate(resolve))

        await this._gc(now)
      } catch (err) {
        this._safeIgnore(err)
      } finally {
        this._gcPending = null
      }
    }

    this._gcPending = nextTick()

    return this._gcPending
  }

  async flush() {
    if (this._pendingListen && !this.suspended) {
      await this._pendingListen
      this._pendingListen = null
    }

    const pending = []

    pending.push(this.collected)

    for (const sess of this.sessions.values()) {
      if (sess.closing || sess.closed || sess.state === 'destroyed') continue
      pending.push(sess.flush())
    }

    for (const scan of this.scans.values()) {
      pending.push(scan.flush())
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
      if (session.isPinned) continue
      if (session.closing || session.closed) continue

      // don't deactivate pending nor newly-started topics
      if (session.isAuto) {
        const timeActive = now - session._startTime
        const minActive = session._startDelay + this._minActiveTime

        if (this.pool && timeActive < minActive) {
          slots-- // reserved & immovable
          continue
        }
      }

      pool.push(session)
    }

    slots = Math.max(slots, 0) // `slots` reduced during runtime.

    const order = new Map()
    const priorities = pool.map((i) => i.priority).sort((a, b) => a - b)
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

    if (!this.pool) slots = 0

    // autostop
    for (let i = slots; i < pool.length; i++) {
      pool[i]._autoStop(now)
    }

    // autostart
    const limit = Math.min(pool.length, slots)

    for (let i = 0; i < limit; i++) {
      pool[i]._autoStart(now)
    }
  }

  async _handleNetworkUpdate() {
    // Rising edge for being online
    const networkCameOnline = this.dht.online && !this._online
    if (this._online && !this.dht.online) this._offlineAt = Date.now()
    this._online = this.dht.online
    if (!networkCameOnline) return

    this.dialer.resetBackoff(this._offlineAt) // failures while offline were ours
    await this._handleNetworkChange()
  }

  async _handleNetworkChange() {
    if (this.suspended) return

    for (const connection of this.connections) {
      connection.stream.sendKeepAlive()
    }

    this.refresh()
  }

  _stopAutoRefresh() {
    if (!this._refreshInterval) return
    clearInterval(this._refreshInterval)
    this._refreshInterval = null
  }

  _startAutoRefresh() {
    if (this._refreshInterval) return
    this._refreshInterval = setInterval(() => this.refresh(), this._autoRefreshInterval)
    this._refreshInterval.unref()
  }

  // TODO: redesign
  _cancelInflight() {
    for (const cancel of this._inflightCancel) cancel()
  }

  async _close() {
    this.dht.removeListener('network-change', this._handleNetworkChange)
    this.dht.removeListener('network-update', this._handleNetworkUpdate)

    this._stopAutoRefresh()

    const tasks = []
    for (const session of this.sessions.values()) {
      tasks.push(session.destroy())
    }

    for (const scan of this.scans.values()) {
      scan.destroy()
    }

    // wait for all un-announces before dropping connections
    await Promise.all(tasks)

    this.sessions.clear()

    for (const connection of this.connections) {
      tasks.push(this._evictConnection(connection))
    }

    this.inbound.clear()
    this.outbound.clear()

    this._cancelInflight()
    this._inflight.clear()
    this.dialer.destroy()

    await Promise.all(tasks)
    await this.server.close()
    if (this._dhtOwned && !this.dht.destroyed) await this.dht.destroy()
  }

  async destroy() {
    await this.close()
  }
}

function noop() {}

function allowAll() {
  return false
}

function toRelaySelector(relayThrough) {
  return typeof relayThrough === 'function'
    ? relayThrough
    : (force, hyperjoin) => (force || hyperjoin.dht.randomized ? relayThrough : null)
}
