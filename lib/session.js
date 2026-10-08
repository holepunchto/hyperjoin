const ReadyResource = require('ready-resource')
const Discovery = require('replicator-discovery')
const b4a = require('b4a')

const INACTIVE = 'inactive'
const PINNED = 'pinned'
const AUTOSTARTED = 'autostarted'
const DISCOVERING = 'discovering'
const DESTROYED = 'destroyed'

module.exports = class ReplicatorSession extends ReadyResource {
  constructor(replicator, store, topic, opts = {}) {
    super()
    this.replicator = replicator
    this.store = store // instance of Replicator.root.session()
    this.topic = topic

    this._onpeer = this._onpeer.bind(this)
    this._onbump = this._onbump.bind(this)
    this._onstoreclose = this.close.bind(this)
    this.store.on('close', this._onstoreclose)
    this._onstats = this._onstats.bind(this)

    this.discovery = null
    this._opts = opts

    this._deferSwarming = opts.deferSwarming !== false

    this.state = INACTIVE
    this._suspended = replicator.suspended

    this.priority = opts.priority || 0
    this._startDelay = 0
    this._startTime = 0
    this._stopTime = 0
    this._starts = 0
    this._timeActive = 0
    this._discoveryTimeout = 0
    this._forceRefresh = false

    if (opts.pin) this.pin()
    else if (opts.discover) this.discover()
  }

  async _open() {
    const { replicator, topic } = this
    await replicator.ready()

    const { suspended, keyPair, _dhtFastRetry: fastRetry } = replicator

    this.discovery = new Discovery(replicator.dht, topic, {
      announce: false,
      suspended,
      keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      onbump: this._onbump,
      onstats: this._onstats,
      fastRetry,
      ...this._opts
    })
  }

  get isPinned() {
    return this.state === PINNED
  }
  get isDiscovering() {
    return this.state === DISCOVERING
  }
  get isAuto() {
    return this.state === AUTOSTARTED
  }
  get active() {
    return this.state !== INACTIVE && this.state !== DESTROYED
  }
  get announce() {
    return !!(this.discovery && this.discovery.announce)
  }
  get isAnnounced() {
    return !!(this.discovery && this.discovery._needsUnannounce)
  }

  hasCore(dkey) {
    const core = this.store.cores.get(dkey)
    if (!core) return false

    // Check if store session has core session
    const sessions = this.store.sessions.list(core.id)
    return sessions.length > 0
  }

  _transition(target) {
    const prev = this.state
    this.state = target
    this.emit('state', target, prev)
    if (prev === DISCOVERING && target !== DISCOVERING) this.emit('discovered')
  }

  _start(mode, now = Date.now()) {
    if (this.state !== INACTIVE) throw new Error('Invalid state: ' + this.state)

    this._startTime = now
    this._stopTime = 0
    this._starts++
    this._startDelay = 0

    const { _autoStartupDelay, _defaultDiscoveryTimeout, _discoveryStartupDelay } = this.replicator

    switch (mode) {
      case AUTOSTARTED:
        this._startDelay = this._deferSwarming ? approximateFuture(_autoStartupDelay) : 0
        break

      case DISCOVERING:
        this._discoveryTimeout = _defaultDiscoveryTimeout
        this._startDelay = this._deferSwarming ? approximateFuture(_discoveryStartupDelay) : 0
        break

      case PINNED:
        break

      default:
        throw new Error(`Unknown state: ${mode}`)
    }

    this._transition(mode)

    this.emit('activate', this)
    this.replicator.emit('activate', this)
  }

  _stop(now = Date.now()) {
    if (this.state === INACTIVE) return // no-op

    this._stopTime = now
    this._timeActive += now - this._startTime
    this._startTime = 0
    this._startDelay = 0

    switch (this.state) {
      case DISCOVERING:
        this._discoveryTimeout = 0
        this.discovery.abort()
        break
    }

    this._transition(INACTIVE)

    this.emit('deactivate', this)
    this.replicator.emit('deactivate', this)
  }

  pin() {
    if (this.isPinned) return

    if (this.isAuto || this.isDiscovering) this._stop() // upgrade

    this._start(PINNED)
  }

  unpin() {
    if (!this.isPinned) return
    this._stop()
  }

  _autoStart(now = Date.now()) {
    if (this.isAuto) return
    this._start(AUTOSTARTED, now)
  }

  _autoStop(now = Date.now()) {
    if (!this.isAuto) return
    this._stop(now)
  }

  discover() {
    if (this.isDiscovering) return

    if (this.isPinned /* || this.isAuto */) {
      this._forceRefresh = true
      return
    }

    if (this.isAuto) {
      this._stop() // degrade into discovering
    }

    this._start(DISCOVERING)
  }

  _stopDiscovery(now = Date.now()) {
    if (!this.isDiscovering) return
    this._stop(now)
  }

  bump(value = -1) {
    if (value !== -1) {
      this.discovery.update(value)
    }

    // adjust discovery timeout
    if (this.isDiscovering) {
      const { _updatedDiscoveryTimeout } = this.replicator
      this._startTime = Date.now() // reset
      this._discoveryTimeout = _updatedDiscoveryTimeout
    }
  }

  get seen() {
    if (!this.discovery) return -1
    return this.discovery.bump.seen
  }

  async refresh(now = Date.now()) {
    await this.ready() // <-- wait root corestore keygen

    if (this.isDiscovering) {
      const expired = now - this._startTime > this._discoveryTimeout
      if (expired) this._stopDiscovery(now)
    }

    const startDelayed = now - this._startTime >= this._startDelay

    const autostartActive = this.isAuto && startDelayed
    const queryRunnerActive = this.discovery.active

    // announce

    const announcing = this.discovery.announce

    let shouldAnnounce = false
    shouldAnnounce ||= this.isPinned
    shouldAnnounce ||= autostartActive
    shouldAnnounce &&= !this.replicator.isPresuspended

    this.discovery.setAnnounce(shouldAnnounce) // sets Dirty & aborts query if active

    // TODO: move to `replicator-discovery`
    // refresh

    let doRefresh = false

    doRefresh ||= announcing !== shouldAnnounce // un-announce
    doRefresh ||= this.isDiscovering && !queryRunnerActive && startDelayed

    doRefresh ||= this._forceRefresh
    this._forceRefresh = false

    if (doRefresh) {
      this.discovery.refresh()
    }
  }

  async flush() {
    await this.ready()
    await this.discovery.flush()
  }

  _shouldEvict(now = Date.now()) {
    if (this.active) return false

    // applies to "pending" (_starts === 0)
    if (this._stopTime === 0) {
      this._stopTime = now
    }

    const idleDuration = now - this._stopTime
    const { _gcIdleTime } = this.replicator

    if (idleDuration < _gcIdleTime) return false // retain in pool

    return true // expired
  }

  _onpeer(peer) {
    if (b4a.equals(peer.publicKey, this.replicator.peerId)) {
      return // filter self
    }

    this.replicator.dialer.seen(peer, this.topic)
  }

  _onbump(value) {
    this.emit('bump', value)
  }

  _onstats(_, action) {
    switch (action) {
      case 'lookup':
        this.replicator.stats.dht.lookup++
        break

      case 'announce':
        this.replicator.stats.dht.announce++
        break

      case 'unannounce':
        this.replicator.stats.dht.unannounce++
        break
    }
  }

  async suspend({ log = noop } = {}) {
    if (this._suspended) return
    this._suspended = true

    await this.ready()
    await this.discovery.suspend({ log })
  }

  async resume({ log = noop } = {}) {
    await this.ready()
    await this.discovery.resume({ log })
    this._forceRefresh = true
    this._suspended = false
  }

  async _close() {
    this.store.removeListener('close', this._onstoreclose)
    this._stop()
    this._transition(DESTROYED)
    if (this.discovery) await this.discovery.destroy()
  }

  // lunte-disable-next-line require-await
  async destroy() {
    return this.close()
  }
}

function approximateFuture(delay) {
  const jitter = Math.floor(Math.random() * 0.5 * delay)
  return delay + jitter
}

function noop() {}
