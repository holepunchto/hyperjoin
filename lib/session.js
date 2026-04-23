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
    this.store.watch(this._oncore)
    this.store.on('close', this.close.bind(this))

    this.discovery = null
    this._opts = opts

    this.isIdle = false
    this.isActive = false
    this.isDiscovering = false

    this._deferSwarming = opts.deferSwarming !== false

    this.discoveryStartTime = 0
    this.discoveryTimeout = 0
    this.idleStartTime = 0
    this.startDelay = 0

    this._autoannounced = false

    if (opts.active) this.setActive(true)
    else if (opts.discover) this.startDiscovery()
  }

  async _open() {
    const { replicator, topic } = this
    await replicator.ready()

    const { suspended, keyPair } = replicator

    this.discovery = new Discovery(replicator.dht, topic, {
      announce: this.isActive,
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

  setActive(value) {
    this.isActive = value
  }

  startDiscovery() {
    const { _defaultDiscoveryTimeout, _startDiscoverySwarmingDelay } = this.replicator

    this.isDiscovering = true
    this.idleStartTime = 0
    this.discoveryStartTime = Date.now()

    this.discoveryTimeout = _defaultDiscoveryTimeout

    this.startDelay = 0
    if (this._deferSwarming) {
      this.startDelay = approximateFuture(_startDiscoverySwarmingDelay)
    }
  }

  bumpDiscovery() {
    if (!this.isDiscovering) return
    const { _updatedDiscoveryTimeout } = this.replicator
    this.discoveryStartTime = Date.now()
    this.discoveryTimeout = _updatedDiscoveryTimeout
    this.idleStartTime = 0
  }

  async refresh(now = Date.now()) {
    // TODO: remove
    if (!this.replicator.autoswarm) {
      return this.discovery.refresh()
    }

    const evict = this._checkInactive(now)

    // announce

    const announcing = this.discovery.announce
    let shouldAnnounce = false
    shouldAnnounce ||= this.isActive
    shouldAnnounce ||= this._autoannounced

    this.discovery.setAnnounce(shouldAnnounce) // destroys active query

    await this.discovery.flushed()

    // refresh

    let doRefresh = false
    doRefresh ||= announcing !== shouldAnnounce // un-announce
    doRefresh ||= this.isDiscovering && now - this.discoveryStartTime > this.startDelay
    doRefresh ||= this.isActive

    if (doRefresh) {
      const ok = await this.discovery.refresh()
      // TODO: (remove) temporary guard against legacy error handler
      if (!ok) console.warn('error during refresh')
    }

    if (evict) {
      this.isIdle = true
      this.emit('idle', this)
    }
  }

  async flush() {
    await this.ready()
    await this.discovery.flushed()
  }

  _checkInactive(now = Date.now()) {
    let discoveryExpired = false
    if (this.isDiscovering) {
      discoveryExpired = now - this.discoveryStartTime > this.discoveryTimeout
    }

    // unset flag
    if (this.isDiscovering && discoveryExpired /* && marked complete */) {
      this.isDiscovering = false
    }

    if (this.isActive || this.isDiscovering || this._autoannounced) {
      return false // session active
    }

    // idle counters

    if (this.idleStartTime === 0) {
      this.idleStartTime = now
    }

    const idleDuration = now - this.idleStartTime
    const { _gcIdleTime } = this.replicator

    if (idleDuration < _gcIdleTime) return false // session active

    return true // session inactive
  }

  get announce() {
    return this.discovery.announce
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
