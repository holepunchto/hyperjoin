const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const Discovery = require('@holepunchto/replicator-discovery')
const b4a = require('b4a')

module.exports = class Session extends ReadyResource {
  constructor(replicator, store, topic, opts = {}) {
    super()
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.topic = topic
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._onpeer = this._onpeer.bind(this)
    this.store.watch(this._oncore)

    this.discovery = null
    this._opts = opts

    this.isActive = false
    this.isDiscovering = false
    this._autoannounce = false
  }

  async _open() {
    const { replicator, topic } = this
    const { announce } = this._opts

    await replicator.ready()

    const { suspended, keyPair } = replicator

    this.discovery = new Discovery(replicator.dht, topic, {
      announce,
      suspended,
      keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      verbose: true,
      ...this._opts
    })

    // starts Discovery internal refresh timer
    this.refresh().catch(safetyCatch)
  }

  setActive(value) {
    this.isActive = value
  }

  async refresh() {
    if (!this.replicator.autoswarm) { // TODO: remove
      return this.discovery.refresh()
    }

    const announcing = this.discovery.announce
    let shouldAnnounce = false
    shouldAnnounce ||= this.isActive
    shouldAnnounce ||= this._autoannounce // TODO: heuristic driver

    this.discovery.setAnnounce(shouldAnnounce) // destroys active query

    await this.discovery.flushed()

    let doRefresh = false
    doRefresh ||= announcing !== shouldAnnounce
    doRefresh ||= this.isDiscovering /* + timeout */

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

  // TODO: REMOVE!
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
  }

  async destroy() {
    return this.close()
  }
}

function noop() {}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}
