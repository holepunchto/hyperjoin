const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const safetyCatch = require('safety-catch')
const Dialer = require('./lib/dialer')
const Connection = require('./lib/connection')
const Session = require('./lib/session')
const PeerDiscoverySession = require('./lib/peer-discovery-session')

const b4a = require('b4a')
const { toHex } = b4a

const AUTO_REFRESH_INTERVAL = 5000

const DHT_FAST_RETRY = 3 * 1000 // 3s

// connections
const MIN_PEERS = 20
const MAX_PEERS = 40
const WANDERLUST_THRESHOLD = 60 * 1000
const CONNECTION_IDLE_TIMEOUT = 3 * 60 * 1000 // 3 Minutes

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
      keyPair = seed ? DHT.keyPair(seed) : null,
      relayThrough,
      minPeers = MIN_PEERS,
      maxPeers = MAX_PEERS,
      maxParallel,
      wanderlust = WANDERLUST_THRESHOLD,
      firewall = allowAll,
      suspended = false,
      manual = false,
      interval = AUTO_REFRESH_INTERVAL,
      gcDisabled = false,
      dhtFastRetry = DHT_FAST_RETRY,
      connectionIdleTimeout = CONNECTION_IDLE_TIMEOUT,
      connectionGc = true,
      wakeup = null
    } = opts

    this.keyPair = keyPair
    this.root = rootStore
    this.dht =
      opts.dht ||
      new DHT({
        bootstrap: opts.bootstrap,
        nodes: opts.nodes,
        port: opts.port
      })

    this.dialer = new Dialer(
      {
        ondial: (peer) => this._connect(peer),
        ondialerror: (err) => this.emit('error', err),
        isConnected: (key) => this.hasConnection(key),
        canRelay: () => !!this.relayThrough,
        want: () => {
          if (!this.discoveryRunning) return 0

          return this._maxPeers - this.connectionCount // throttle
        }
      },
      {
        backoffs: opts.backoffs,
        jitter: opts.jitter,
        suspended,
        maxParallel
      }
    )

    this.relayThrough = relayThrough && toRelaySelector(relayThrough)
    this.wakeup = wakeup // protomux-wakeup, attached to every connection

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
    this.isBackgrounded = false

    this.sessions = new Map() // hex(topic) -> Session
    this.inbound = new Map() // hex(peerId) -> Connection
    this.outbound = new Map() // hex(peerId) -> Connection

    this.dhtFastRetry = dhtFastRetry
    this.connectionIdleTimeout = connectionIdleTimeout // no protocol traffic for this long is idle
    this.connectionGc = connectionGc // drop idle connections even below maxPeers

    this.stats = {
      updates: 0,
      bannedPeers: 0,
      connects: {
        // TODO: nit, flatten and 'connections'
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
        unannounce: 0,
        errors: 0
      }
    }

    this._dhtOwned = !opts.dht
    this._firewall = firewall
    this._suspending = Promise.resolve()

    this._inflight = new Set() // hex(peerId), dedup in-flight dials
    this._inflightCancel = new Set() // cancelCallbacks
    this._minPeers = minPeers
    this._maxPeers = maxPeers
    this._wanderlust = wanderlust
    this._lastConnectionEvict = 0

    this._online = false
    this._gcDisabled = gcDisabled
    this._manual = !!manual
    this._autoRefreshInterval = interval
    this._refreshInterval = null

    this._handleNetworkChange = this._handleNetworkChange.bind(this)
    this.dht.on('network-change', this._handleNetworkChange)

    this._handleNetworkUpdate = this._handleNetworkUpdate.bind(this)
    this.dht.on('network-update', this._handleNetworkUpdate)

    this._onrootclose = this._onrootclose.bind(this)
    this.root.once('close', this._onrootclose)

    this.on('update', this._handleUpdate.bind(this))

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

  get connections() {
    return this._connections() // combined iterator
  }

  get peers() {
    return this.dialer.peers
  }

  // @deprecated, use isBackgrounded
  get isPresuspended() {
    return this.isBackgrounded
  }

  // announces, lookups and dials only run while resumed and in the foreground
  get discoveryRunning() {
    return !this.suspended && !this.isBackgrounded
  }

  *_connections() {
    yield* this.inbound.values()
    yield* this.outbound.values()
  }

  async _open() {
    // TODO: this await is the root cause
    // for `refresh()` being async.
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

  // sessions doing dht work right now
  get activeSessions() {
    const active = []
    for (const session of this.sessions.values()) {
      if (session.suspended) continue
      if (!session.isServer && !session.isClient) continue
      active.push(session)
    }
    return active
  }

  get announcedSessions() {
    const announced = []
    for (const session of this.sessions.values()) {
      if (!session.announced) continue
      announced.push(session)
    }
    return announced
  }

  _selectRelay(force) {
    if (!this.relayThrough) return null
    return this.relayThrough(force, this)
  }

  // Join a topic, like hyperswarm: `server` announces it, `client` looks it up
  // and connects to its peers, at most `limit` of them.
  // Every join returns its own handle, destroy it (or close `store`) to leave.
  join(store, topic, { server = true, client = true, limit = Infinity } = {}) {
    if (this.destroyed || this.closing) throw new Error('Replicator is destroyed')

    const hex = toHex(topic)
    let session = this.sessions.get(hex)

    // a session that is closing is never reused, a fresh one replaces it
    if (!session || session.closing) session = this._createSession(store, topic, session)

    const discovery = new PeerDiscoverySession(session, { server, client, limit })
    session.add(discovery)

    session.ready().catch(safetyCatch)

    return discovery
  }

  _createSession(store, topic, previous = null) {
    const hex = toHex(topic)
    const session = new Session(this, store, topic, { previous })

    this.sessions.set(hex, session)

    session.on('update', () => this._updateClient(hex))
    session.on('close', () => {
      if (this.sessions.get(hex) === session) {
        this.sessions.delete(hex)
        this._updateClient(hex)
      }
      this.emit('remove', session)
    })

    this.emit('join', session)

    return session
  }

  // the dialer only connects to peers of topics we are a client of
  _updateClient(hex) {
    const session = this.sessions.get(hex)
    if (session && session.isClient) this.dialer.topicEnable(hex)
    else this.dialer.topicDisable(hex)
  }

  // Stay connected to a peer by public key, like hyperswarm.
  // Refcounted, and never dropped by gc while joined.
  joinPeer(publicKey) {
    this.dialer.joinPeer(publicKey)
    this.emit('update')
  }

  leavePeer(publicKey) {
    this.dialer.leavePeer(publicKey)
    this.emit('update')
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
    const suspending = [this.server.suspend({ log }), this.dialer.suspend({ log })]

    for (const session of this.sessions.values()) {
      suspending.push(session.suspend({ log }))
    }

    for (const connection of this.connections) {
      connection.info.resetAttempts()
      pending.push(this._evictConnection(connection, ERR_SUSPEND))
    }

    this._inflight.clear()
    this._stopAutoRefresh()

    await Promise.allSettled(suspending)

    if (this._dhtOwned) await this.dht.suspend({ log })

    await Promise.all(pending)
  }

  // The app went to the background: stop announcing, looking up and dialing.
  // Connections stay up and inbound connections are still accepted.
  async background() {
    if (this.isBackgrounded) return
    this.isBackgrounded = true

    this.dialer.suspend().catch(safetyCatch)

    const pending = []
    for (const session of this.sessions.values()) pending.push(session.suspend())
    await Promise.allSettled(pending)
  }

  // Back in the foreground: announce, look up and dial again
  async foreground() {
    if (!this.isBackgrounded) return
    this.isBackgrounded = false

    if (this.suspended) return // resume() brings everything back

    this.dialer.resume()

    const pending = []
    for (const session of this.sessions.values()) pending.push(session.resume())
    await Promise.allSettled(pending)

    await this.refresh()
  }

  // @deprecated, use background()
  presuspend() {
    return this.background()
  }

  // Back from a suspend or the background: announce, look up and dial again
  async resume({ log = noop } = {}) {
    if (!this.suspended) return this.foreground()

    if (this._dhtOwned) await this.dht.resume({ log })

    await this.server.resume({ log })

    this.isBackgrounded = false
    this.suspended = false

    const pending = []
    for (const session of this.sessions.values()) pending.push(session.resume({ log }))
    await Promise.all(pending)

    // only dial once the replicator is fully resumed
    this.dialer.resume()

    if (!this._manual) this._startAutoRefresh()
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

    // Both peers dialed each other. Each side keeps the same stream:
    // the peer with the lower key keeps its inbound one.
    const crossed = (initiator ? this.inbound : this.outbound).get(hex)
    const dialing = !initiator && this._inflight.has(hex)
    if (crossed || dialing) {
      const keepInbound = b4a.compare(this.peerId, peerId) < 0
      if (keepInbound === initiator) {
        stream.on('error', noop)
        stream.destroy(new Error(ERR_DUPLICATE))
        return null
      }
      if (crossed) crossed.destroy(ERR_DUPLICATE)
    }

    // the connection and protomux attach their 'data' listeners in the same tick
    const connection = new Connection(this, stream, peerId, initiator)

    map.set(hex, connection)

    if (this.wakeup) this.wakeup.addStream(connection.stream)
    this.root.replicate(connection.stream)
    connection.watchChannels()

    if (initiator) this.stats.connects.client.opened++
    else this.stats.connects.server.opened++

    this.emit('connected', connection)
    this.emit('update')

    return connection
  }

  async _evictConnection(connection, err) {
    if (connection.stream.closed) return

    this.emit('_evictConnection', connection.peerId, /* reason */ err)

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

  // backgrounded still accepts inbound connections, see background()
  get _isTearingdown() {
    return (
      this.destroyed || // replicator begin-teardown
      this.suspended || // replicator
      this.closed || // ready-resource fully torn down
      this.closing
    )
  }

  // Resolves to the dial's failure cause, falsy once connected. The dialer records the attempt.
  async _connect(peer) {
    if (this._isTearingdown || this.isBackgrounded) return true

    const hex = toHex(peer.publicKey)
    if (b4a.equals(peer.publicKey, this.peerId)) return true

    if (this.outbound.has(hex)) return true
    if (this.inbound.has(hex)) return true
    if (this._inflight.has(hex)) return true

    this._inflight.add(hex)

    let firewalled = true
    try {
      firewalled = await this._handleFirewall(peer.publicKey, null)
    } finally {
      // userhandler threw or firewalled - clear ref
      if (firewalled) this._inflight.delete(hex)
    }

    if (firewalled) return true

    // no dials start in the background, it may have begun while the firewall ran
    if (this._isTearingdown || this.isBackgrounded) {
      this._inflight.delete(hex)
      return true
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
        relayAddresses: peer.relayAddresses,
        relayThrough: this._selectRelay(peer.forceRelaying)
      })

      stream.once('error', onerror)
      stream.once('close', onclose)
      stream.once('open', onopen)

      cancelCallback = () => stream.destroy('suspending')
      this._inflightCancel.add(cancelCallback)

      this.stats.connects.client.attempted++
      this.emit('update')
    })

    this._inflight.delete(hex)
    if (cancelCallback) this._inflightCancel.delete(cancelCallback)

    // opened after teardown
    if (this._isTearingdown) {
      stream.once('error', noop)
      stream.destroy(new Error(ERR_DESTROYED))
      return ERR_DESTROYED
    }

    if (!opened) {
      this.emit('update')
      return error || true
    }

    // stream opened - hand-off
    this._initConnection(true, peer.publicKey, stream)

    return false
  }

  // Order to drop connections in, first goes first. Connections the dialer
  // does not want (it would not reconnect them) go before wanted ones, then
  // pristine's heuristic: rookies before established, then the stalest.
  // Joined peers (joinPeer) are never dropped.
  _dropOrder(now) {
    return Array.from(this.connections)
      .filter((c) => !c.evicted && !c.info.isJoined)
      .map((c) => ({ connection: c, wanted: this.dialer.wants(c.info), idle: c.isIdle(now) }))
      .sort((a, b) => {
        if (a.wanted !== b.wanted) return a.wanted ? 1 : -1
        if (a.idle !== b.idle) return a.idle ? -1 : 1

        const aRookie = a.connection.survived < 1
        const bRookie = b.connection.survived < 1

        // Evict rookie over established
        if (aRookie !== bRookie) return aRookie ? -1 : 1

        // Evict newest, comparing 2 rookies.
        if (aRookie) return b.connection.connectedAt - a.connection.connectedAt

        // Evict stale, comparing 2 established
        return a.connection.lastActivity - b.connection.lastActivity
      })
      .map((e) => e.connection)
  }

  // a peer we want is waiting for a free connection slot
  _isPeerWaiting(now) {
    for (const peer of this.peers.values()) {
      if (this.dialer.isEligable(peer, now)) return true
    }
    return false
  }

  async _gc(now = Date.now()) {
    if (this._gcDisabled) return

    const evictions = []
    let connectionsEvicted = 0

    const drop = (connection, reason) => {
      connection.evicted = true
      evictions.push(this._evictConnection(connection, reason))
      connectionsEvicted++
      this._lastConnectionEvict = now
    }

    if (!this._lastConnectionEvict) this._lastConnectionEvict = now

    let open = 0
    for (const connection of this.connections) if (!connection.evicted) open++

    // 1. idle connections the dialer would not reconnect, at any connection count.
    // Wanted ones stay, dropping them would only make the dialer redial them.
    if (this.connectionGc) {
      for (const connection of this.connections) {
        if (connection.evicted || connection.info.isJoined) continue
        if (this.dialer.wants(connection.info) || !connection.isIdle(now)) continue

        drop(connection, ERR_IDLE)
        open--
      }
    }

    // 2. over the cap
    if (open > this._maxPeers) {
      for (const connection of this._dropOrder(now)) {
        if (open <= this._maxPeers) break
        drop(connection, ERR_MAXPEERS)
        open--
      }
    }

    // 3. at the cap, a wanted peer is waiting: make room by dropping an unwanted connection
    if (open >= this._maxPeers && this._isPeerWaiting(now)) {
      const [first] = this._dropOrder(now)

      if (first && !this.dialer.wants(first.info)) {
        drop(first, ERR_MAXPEERS)
        open--
      }
    }

    // 4. force peer rotation when fully saturated over an extended period of time
    const treshold = this._wanderlust + Math.random() * Math.max(this._wanderlust, 10000)
    if (
      !connectionsEvicted && // nothing evicted
      open === this._maxPeers && // at max cap
      now - this._lastConnectionEvict > treshold // topology growing stale
    ) {
      const connections = this._dropOrder(now)
      const connection = connections[Math.floor(Math.random() * connections.length)]

      if (connection) drop(connection, ERR_IDLE)
    }

    for (const connection of this.connections) {
      if (connection.evicted) continue
      connection.survived++
    }

    await Promise.all(evictions)

    if (connectionsEvicted) this.emit('dropped', connectionsEvicted)
  }

  _handleUpdate() {
    this.stats.updates++
  }

  _onrootclose() {
    if (this.destroyed || this.closing) return
    this.destroy().catch((err) => this.emit('error', err))
  }

  // Drops idle or surplus connections and reschedules dials.
  // Manual replicators take no actions unless refresh is called.
  async refresh(now = Date.now()) {
    await this._gc(now)

    this.dialer.refresh(now)

    const { banned } = this.dialer.checkBans()
    for (const p of banned) this._handleBan(p)
  }

  async flush() {
    const pending = []
    for (const session of this.sessions.values()) {
      if (session.closing || session.closed) continue
      pending.push(session.flush())
    }
    await Promise.all(pending)

    // pending connects
    await this.dialer.flush()
  }

  async _handleNetworkUpdate() {
    // Rising edge for being online
    const networkCameOnline = this.dht.online && !this._online
    this._online = this.dht.online
    if (!networkCameOnline) return

    this.dialer.resetBackoff() // failures while offline were ours
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

  // TODO: redesign
  _cancelInflight() {
    for (const cancel of this._inflightCancel) cancel()
  }

  async _close() {
    this.destroyed = true

    this.root.removeListener('close', this._onrootclose)
    this.dht.removeListener('network-change', this._handleNetworkChange)
    this.dht.removeListener('network-update', this._handleNetworkUpdate)

    this._stopAutoRefresh()

    const tasks = []
    for (const session of this.sessions.values()) {
      tasks.push(session.close())
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
    : (force, replicator) => (force || replicator.dht.randomized ? relayThrough : null)
}
