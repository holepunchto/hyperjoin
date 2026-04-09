const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const Hypercore = require('hypercore')
const Discovery = require('@holepunchto/replicator-discovery')
const b4a = require('b4a')

module.exports = class ReplicatorSession extends ReadyResource {
  constructor(replicator, store, discoveryKey, opts = {}) {
    super()
    this.replicator = replicator
    this.store = store // instance from Replicator.root.session()
    this.discoveryKey = discoveryKey
    this.cores = new Map() // discoveryKey => Core

    this._oncore = this._oncore.bind(this)
    this._onpeer = this._onpeer.bind(this)
    this.store.watch(this._oncore)

    this.discovery = null
    this._opts = opts
  }

  async _open() {
    const { replicator, discoveryKey } = this
    const { announce } = this._opts

    await replicator.ready()

    const { suspended, keyPair } = replicator

    this.discovery = new Discovery(replicator.dht, discoveryKey, {
      announce,
      suspended,
      keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      ...this._opts
    })

    // starts Discovery internal refresh timer
    this.discovery.refresh().catch(safetyCatch)
  }

  setAnnounce(value) {
    this.discovery.setAnnounce(value)

    this.refresh().catch(safetyCatch)
  }

  async refresh() {
    return this.discovery.refresh()
  }

  get announce() {
    return this.discovery.announce
  }

  _onpeer(peer, data) {
    this.replicator.ps.seen(peer, this.discoveryKey)
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

  // Manual weak-ref, core must belong to store
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
