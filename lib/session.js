const ReadyResource = require('ready-resource')
const Hypercore = require('hypercore')
const Discovery = require('@holepunchto/replicator-discovery')
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
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._onpeer = this._onpeer.bind(this)
    this._lastCoreAdd = 0
    this.store.watch(this._oncore)
    this.store.on('close', this.close.bind(this))

    this.discovery = null
    this._opts = opts

    this._deferSwarming = opts.deferSwarming !== false

    this.state = INACTIVE

    this.priority = opts.priority || 0
    this._startDelay = 0
    this._startTime = 0
    this._stopTime = 0
    this._starts = 0
    this._timeActive = 0
    this._discoveryTimeout = 0

    if (opts.active) this.pin()
    else if (opts.discover) this.discover()
  }

  async _open() {
    const { replicator, topic } = this
    await replicator.ready()

    const { suspended, keyPair } = replicator

    this.discovery = new Discovery(replicator.dht, topic, {
      announce: this.isPinned,
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
    return this.discovery.announce
  }

  _start(mode, now = Date.now()) {
    if (this.state !== INACTIVE) throw new Error('Invalid state: ' + this.state)

    this._startTime = now
    this._stopTime = 0
    this._starts++
    this._startDelay = 0

    const { _startAutoSwarmingDelay, _defaultDiscoveryTimeout, _startDiscoverySwarmingDelay } =
      this.replicator

    switch (mode) {
      case AUTOSTARTED:
        if (this._deferSwarming) {
          this._startDelay = approximateFuture(_startAutoSwarmingDelay)
        }
        break

      case DISCOVERING:
        this._discoveryTimeout = _defaultDiscoveryTimeout

        if (this._deferSwarming) {
          this._startDelay = approximateFuture(_startDiscoverySwarmingDelay)
        }
        break

      case PINNED:
        break

      default:
        throw new Error(`Unknown state: ${mode}`)
    }

    this.state = mode

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
        this.discovery.abort().catch(console.warn)
        break
    }

    this.state = INACTIVE

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

  async discover() {
    if (this.isDiscovering || this.isPinned) return

    // TODO: should not degrade auto, just relaunch fresh query
    if (this.isAuto) this._stop()
    this._start(DISCOVERING)

    return Promise.resolve(-1) // TODO: resolve latest bump value; rejections not allowed
  }

  _stopDiscovery(now = Date.now()) {
    if (!this.isDiscovering) return
    this._stop(now)
  }

  bump() {
    // adjust discovery timeout
    if (this.isDiscovering) {
      const { _updatedDiscoveryTimeout } = this.replicator
      this._startTime = Date.now() // reset
      this._discoveryTimeout = _updatedDiscoveryTimeout
    }

    // TODO: schedule version commit
    // this._version = 0
  }

  async refresh(now = Date.now()) {
    // TODO: remove
    if (!this.replicator.autoswarm) {
      return this.discovery.refresh()
    }

    if (this.isDiscovering) {
      const expired = now - this._startTime > this._discoveryTimeout
      if (expired) this._stopDiscovery(now)
    }

    const autostartActive = this.isAuto && now - this._startTime > this._startDelay
    const queryRunnerActive = this.discovery.active

    // announce

    const announcing = this.discovery.announce

    let shouldAnnounce = false
    shouldAnnounce ||= this.isPinned
    shouldAnnounce ||= autostartActive

    this.discovery.setAnnounce(shouldAnnounce) // destroys active query
    await this.discovery.flushed()

    // refresh

    let doRefresh = false
    doRefresh ||= announcing !== shouldAnnounce // un-announce
    doRefresh ||=
      this.isDiscovering && !queryRunnerActive && now - this._startTime > this._startDelay
    doRefresh ||= this.isPinned

    if (!doRefresh) return

    const ok = await this.discovery.refresh()

    // TODO: (remove) temporary guard against legacy error handler
    if (!ok) console.warn('error during refresh')
  }

  async flush() {
    await this.ready()
    await this.discovery.flushed()
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
    this._stop()
    this.state = DESTROYED
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
