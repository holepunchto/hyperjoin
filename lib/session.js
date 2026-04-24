const ReadyResource = require('ready-resource')
const Hypercore = require('hypercore')
const Discovery = require('@holepunchto/replicator-discovery')
const b4a = require('b4a')

module.exports = class ReplicatorSession extends ReadyResource {
  constructor(replicator, store, topic, opts = {}) {
    super()
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.topic = topic
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._onpeer = this._onpeer.bind(this)
    this._lastCoreAdd = 0
    this.store.watch(this._oncore)
    this.store.on('close', this.close.bind(this))

    this.discovery = null
    this._opts = opts

    this._deferSwarming = opts.deferSwarming !== false

    this.pinned = false
    this.isDiscovering = false

    this._discoveryStartTime = 0
    this._discoveryTimeout = 0
    this._idleStartTime = 0
    this._startDelay = 0

    this._autostarted = false
    this.priority = 0
    this._autostarts = 0
    this._autoStartTime = 0
    this._autoEndTime = 0
    this._timeActive = 0

    if (opts.active) this.pin()
    else if (opts.discover) this.startDiscovery()
  }

  async _open() {
    const { replicator, topic } = this
    await replicator.ready()

    const { suspended, keyPair } = replicator

    this.discovery = new Discovery(replicator.dht, topic, {
      announce: this.pinned,
      suspended,
      keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      verbose: true,
      ...this._opts
    })

    const done = []
    for (const core of this.store.sessions) {
      const p = core.ready().then(() => this._oncore(core.core))
      done.push(p)
    }
    await Promise.all(done)
  }

  pin() {
    if (this.pinned) return
    this.pinned = true
    // TODO:
    // if (!this.started) this._autoStart()
  }

  unpin() {
    if (!this.pinned) return
    this.pinned = false
    this._autoStop() // TODO: immediate returns
  }

  get active() {
    return this.pinned || this._autostarted || this.isDiscovering
  }

  get announce() {
    return this.discovery.announce
  }

  _autoStart(now = Date.now()) {
    if (this._autostarted) return

    const { _startAutoSwarmingDelay } = this.replicator

    this._autostarted = true
    this._autoStartTime = now
    this._autostarts++
    this._autoEndTime = 0
    this._idleStartTime = 0

    this._startDelay = 0
    if (this._deferSwarming) {
      this._startDelay = approximateFuture(_startAutoSwarmingDelay)
    }

    this.emit('activate', this)
    this.replicator.emit('activate', this)
  }

  _autoStop(now = Date.now()) {
    if (!this._autostarted) return

    this._idleStartTime = now
    this._autoEndTime = now
    this._timeActive += now - this._autoStartTime
    this._autoStartTime = 0
    this._startDelay = 0
    this._autostarted = false

    this.emit('deactivate', this)
    this.replicator.emit('deactivate', this)
  }

  startDiscovery() {
    const { _defaultDiscoveryTimeout, _startDiscoverySwarmingDelay } = this.replicator

    this.isDiscovering = true
    this._idleStartTime = 0
    this._discoveryStartTime = Date.now()

    this._discoveryTimeout = _defaultDiscoveryTimeout

    this._startDelay = 0
    if (this._deferSwarming) {
      this._startDelay = approximateFuture(_startDiscoverySwarmingDelay)
    }
  }

  // TODO: rename bump(version)
  // valid for both announce+discover
  bumpDiscovery() {
    if (!this.isDiscovering) return
    const { _updatedDiscoveryTimeout } = this.replicator
    this._discoveryStartTime = Date.now()
    this._discoveryTimeout = _updatedDiscoveryTimeout
    this._idleStartTime = 0

    // schedule version commit
  }

  _stopDiscovering(now = Date.now()) {
    this.isDiscovering = false
    this._idleStartTime = now

    // TODO: public api; discovery.stop() // Kill the timer
    if (this.discovery._timer) clearTimeout(this.discovery._timer)
  }

  async refresh(now = Date.now()) {
    // TODO: remove
    if (!this.replicator.autoswarm) {
      return this.discovery.refresh()
    }

    if (this.isDiscovering) {
      const expired = now - this._discoveryStartTime > this._discoveryTimeout
      if (expired) this._stopDiscovering(now)
    }

    const autostartActive = this._autostarted && now - this._autoStartTime > this._startDelay
    // TODO: expose public api or remove nested timer
    const discoveryOwned = !!(this.discovery._currentRefresh || this.discovery._timer)

    // announce

    const announcing = this.discovery.announce

    let shouldAnnounce = false
    shouldAnnounce ||= this.pinned
    shouldAnnounce ||= autostartActive

    this.discovery.setAnnounce(shouldAnnounce) // destroys active query
    await this.discovery.flushed()

    // refresh

    let doRefresh = false
    doRefresh ||= announcing !== shouldAnnounce // un-announce
    doRefresh ||=
      this.isDiscovering && !discoveryOwned && now - this._discoveryStartTime > this._startDelay
    doRefresh ||= this.pinned

    if (!doRefresh) return

    const ok = await this.discovery.refresh()

    // TODO: (remove) temporary guard against legacy error handler
    if (!ok) console.warn('error during refresh')
  }

  async flush() {
    await this.ready()
    await this.discovery.flushed()
  }

  // TODO: rewrite state transitions.
  // Discovery story:
  // 1. pending (start delay)
  // 2. active window (peers queried and dialed)
  // 3. unset isDiscovering; this.active => false
  // 4. (global) If session spends amount of time inactive, should be eviced + event.
  //
  // Pinned story:
  // 1. session.pin()
  // 2. session.unpin()
  // 3. (global) evict by timeout
  //
  // Auto activation story:
  // 1. _autoStart()
  // 2. _autoStop()
  // 3. (global) evict by timeout
  //
  _shouldEvict(now = Date.now()) {
    if (this.active) return false

    // TODO: legacy bug; requires 2 gc cycles to detect evictions.
    // (partially solved now)
    if (this._idleStartTime === 0) {
      this._idleStartTime = now
    }

    const idleDuration = now - this._idleStartTime
    const { _gcIdleTime } = this.replicator

    if (idleDuration < _gcIdleTime) return false // retain in pool

    return true // expried
  }

  _onpeer(peer, data) {
    this.replicator.ps.seen(peer, this.topic)
  }

  _oncore(core) {
    const dk = toHex(core.discoveryKey)
    if (this.cores.has(dk)) return

    const weak = new Hypercore({ core, weak: true })
    this.cores.set(dk, weak)

    weak.on('close', () => {
      this.cores.delete(dk)
    })

    this._lastCoreAdd = Date.now()
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

function approximateFuture(delay) {
  const jitter = Math.floor(Math.random() * 0.5 * delay)
  return delay + jitter
}

function noop() {}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
