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

    this.isActive = !!opts.active

    this.isDiscovering = !!opts.discover
    this.discoveryStartTime = 0
    this.discoveryTimeout =  0

    if (this.isDiscovering) {
      this.discoveryStartTime = Date.now()
      this.discoveryTimeout = this.replicator._defaultDiscoveryTimeout
    }

    this.idleStartTime = 0
    this._autoannounce = false

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
    this.isDiscovering = true
    this.discoveryStartTime = Date.now()
    this.discoveryTimeout = this.replicator._defaultDiscoveryTimeout
    this.idleStartTime = 0
  }

  bumpDiscovery() {
    if (!this.isDiscovering) return

    this.discoveryStartTime = Date.now()
    this.discoveryTimeout = this.replicator._updatedDiscoveryTimeout
    this.idleStartTime = 0
  }

  async refresh(now = Date.now()) {
    // TODO: remove
    if (!this.replicator.autoswarm) {
      return this.discovery.refresh()
    }

    let discoveryExpired = false
    if (this.isDiscovering) {
      discoveryExpired = now - this.discoveryStartTime > this.discoveryTimeout
    }

    if (this.isDiscovering && discoveryExpired) {
      this.isDiscovering = false
    }

    const keepAlive = this.isActive || this.isDiscovering
    if (keepAlive) {
      this.idleStartTime = 0
    } else {
      const idleSince = discoveryExpired ? this.discoveryStartTime + this.discoveryTimeout : now
      if (this.idleStartTime === 0) this.idleStartTime = idleSince
    }

    const announcing = this.discovery.announce
    let shouldAnnounce = false
    shouldAnnounce ||= this.isActive
    shouldAnnounce ||= this._autoannounce // TODO: heuristic driver

    this.discovery.setAnnounce(shouldAnnounce) // destroys active query

    await this.discovery.flushed()

    let doRefresh = false
    doRefresh ||= announcing !== shouldAnnounce // un-announce
    doRefresh ||= this.isDiscovering /* + timeout */
    doRefresh ||= this.isActive /* + timeout */

    if (doRefresh) {
      const ok = await this.discovery.refresh()
      // temporary guard against legacy error handler
      if (!ok) console.warn('error during refresh')
    }
  }

  async flush() {
    await this.ready()
    await this.discovery.flushed()
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

function noop() {}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
