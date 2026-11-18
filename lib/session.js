const ReadyResource = require('ready-resource')
const Discovery = require('hyperannounce')
const b4a = require('b4a')

const INACTIVE = 'inactive'
const PINNED = 'pinned'
const AUTOSTARTED = 'autostarted'
const DESTROYED = 'destroyed'

module.exports = class ReplicatorSession extends ReadyResource {
  constructor(replicator, topic, opts = {}) {
    super()
    this.replicator = replicator
    this.topic = topic
    this.state = INACTIVE

    this._onpeer = this._onpeer.bind(this)
    this._onbump = this._onbump.bind(this)
    this._onstats = this._onstats.bind(this)

    const { priority = 0, pin, delay, wait } = opts

    this._wait = wait
    this._delay = delay
    this._suspended = replicator.suspended
    this.priority = priority
    this._startDelay = 0
    this._startTime = 0
    this._stopTime = 0
    this._starts = 0
    this._timeActive = 0
    this._forceRefresh = false

    const { dht, suspended, keyPair } = replicator

    this.discovery = new Discovery(dht, topic, {
      announce: false,
      suspended,
      keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      onbump: this._onbump,
      onstats: this._onstats,
      ...opts
    })

    if (pin) this.pin()
  }

  get isPinned() {
    return this.state === PINNED
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

  _transition(target) {
    const prev = this.state
    this.state = target
    this.emit('state', target, prev)
  }

  _start(mode, now = Date.now()) {
    if (this.state !== INACTIVE) throw new Error('Invalid state: ' + this.state)

    this._startTime = now
    this._stopTime = 0
    this._starts++
    this._startDelay = 0

    switch (mode) {
      case AUTOSTARTED:
        if (this._delay > 0) {
          this._startDelay = approximateFuture(this._delay)
        }
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

    this._transition(INACTIVE)

    this.emit('deactivate', this)
    this.replicator.emit('deactivate', this)
  }

  pin() {
    if (this.isPinned) return

    if (this.isAuto) this._stop() // upgrade

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

  bump(value = -1) {
    if (value === -1) return
    this.discovery.update(value)
  }

  get seen() {
    if (!this.discovery) return -1
    return this.discovery.bump.seen
  }

  refresh(now = Date.now()) {
    const startDelayed = now - this._startTime >= this._startDelay
    const autostartActive = this.isAuto && startDelayed

    // announce

    const announcing = this.discovery.announce

    let shouldAnnounce = false
    shouldAnnounce ||= this.isPinned
    shouldAnnounce ||= autostartActive
    shouldAnnounce &&= !this.replicator.isPresuspended
    shouldAnnounce &&= this.replicator.listening

    this.discovery.setAnnounce(shouldAnnounce) // sets Dirty & aborts query if active

    // TODO: move doRefresh condition to `hyperannounce`
    // refresh

    let doRefresh = false
    doRefresh = announcing !== shouldAnnounce // un-announce

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

    if (this._wait) await this._wait

    await this.discovery.suspend({ log })
  }

  async resume({ log = noop } = {}) {
    await this.ready()
    this.discovery.resume({ log })
    this._suspended = false
  }

  async _close() {
    this._stop()
    this._transition(DESTROYED)

    if (this._wait) await this._wait
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
