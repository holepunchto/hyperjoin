const ReadyResource = require('ready-resource')
const Discovery = require('replicator-discovery')
const b4a = require('b4a')

// One per topic. Combines the flags of every join() on the topic, like
// hyperswarm: server if any join is a server, client if any is a client.
module.exports = class ReplicatorSession extends ReadyResource {
  constructor(replicator, store, topic, { previous = null } = {}) {
    super()

    this.replicator = replicator
    this.store = store // instance of Replicator.root.session(), closing it leaves the topic
    this.topic = topic
    this.hex = b4a.toString(topic, 'hex')
    this.discovery = null
    this.isServer = false
    this.isClient = false
    this.limit = Infinity // max connected peers, highest limit of the client joins
    this.suspended = !replicator.discoveryRunning // suspended or backgrounded

    this._previous = previous // a closing session for the same topic
    this._discoverySessions = new Set()

    this._onpeer = this._onpeer.bind(this)
    this._onstats = this._onstats.bind(this)
    this._onerror = this._onerror.bind(this)
    this._onstoreclose = this.close.bind(this)

    this.store.on('close', this._onstoreclose)
  }

  async _open() {
    const { replicator, topic } = this
    await replicator.ready()

    // its unannounce must not race our announce, both use the same key pair
    if (this._previous) await this._previous.close()
    this._previous = null

    this.discovery = new Discovery(replicator.dht, topic, {
      announce: this.isServer,
      suspended: this.suspended,
      keyPair: replicator.keyPair,
      relayAddresses: () => replicator.server.relayAddresses,
      onpeer: this._onpeer,
      onstats: this._onstats,
      onerror: this._onerror,
      fastRetry: replicator.dhtFastRetry
    })

    if (this.isServer || this.isClient) this.discovery.refresh()
  }

  hasCore(dkey) {
    const core = this.store.cores.get(dkey)
    if (!core) return false

    // Check if store session has core session
    const sessions = this.store.sessions.list(core.id)
    return sessions.length > 0
  }

  get announced() {
    return !!this.discovery && this.discovery.announce && !this.suspended
  }

  add(discoverySession) {
    this._discoverySessions.add(discoverySession)
    this.refresh()
  }

  remove(discoverySession) {
    if (!this._discoverySessions.delete(discoverySession)) return this.closing

    if (this._discoverySessions.size > 0) {
      this.refresh()
      return this.flush()
    }

    // last one left, like hyperswarm's leave()
    this.isServer = false
    this.isClient = false
    this.emit('update')

    return this.close()
  }

  // Recompute the flags from the joins and apply them
  refresh() {
    let isServer = false
    let isClient = false
    let limit = 0

    for (const s of this._discoverySessions) {
      if (s.isServer) isServer = true
      if (s.isClient) {
        isClient = true
        limit = Math.max(limit, s.limit)
      }
    }

    if (!isClient) limit = Infinity

    const changed = isServer !== this.isServer || isClient !== this.isClient

    this.isServer = isServer
    this.isClient = isClient
    this.limit = limit

    if (!changed) return

    this.emit('update')

    if (this.discovery) this._query()
  }

  _query() {
    this.discovery.setAnnounce(this.isServer)

    if (this.isServer || this.isClient) {
      this.discovery.refresh() // un-announces too when we stop being a server
      return
    }

    const aborting = this.discovery.abort(true) // nothing wanted, just un-announce
    if (aborting) aborting.catch(noop)
  }

  async flush() {
    await this.ready()
    if (this.discovery.destroyed) return
    await this.discovery.flush()
  }

  // Peers of this topic we are connected to, dialing, or about to dial.
  // Peers in backoff do not count, so another peer can take their place.
  _claimedPeers(now = Date.now()) {
    const { dialer } = this.replicator
    let n = 0

    for (const peer of dialer.peers.values()) {
      if (!peer.hasTopic(this.hex)) continue

      const claimed =
        this.replicator.hasConnection(peer.publicKey) ||
        peer.isDialing ||
        dialer.isEligable(peer, now)

      if (claimed) n++
    }

    return n
  }

  _onpeer(peer) {
    if (b4a.equals(peer.publicKey, this.replicator.peerId)) {
      return // filter self
    }

    // only clients connect, a server waits to be found
    if (!this.isClient) return

    const { dialer } = this.replicator
    const known = dialer.peers.get(b4a.toString(peer.publicKey, 'hex'))

    // `limit` caps the connected peers of the topic, like hyperswarm.
    // Peers already on the topic are always seen again, keeping their relays and expiry fresh.
    const onTopic = !!known && known.hasTopic(this.hex)
    if (!onTopic && this.limit !== Infinity && this._claimedPeers() >= this.limit) return

    dialer.seen(peer, this.topic)
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

  _onerror(err) {
    this.replicator.stats.dht.errors++
    this.replicator.emit('discovery-error', err, this)
  }

  // stops all dht work and un-announces (suspend, or the app went to the background)
  async suspend({ log = noop } = {}) {
    if (this.suspended) return
    this.suspended = true

    await this.ready()
    await this.discovery.suspend({ log })
  }

  async resume({ log = noop } = {}) {
    if (!this.suspended) return
    this.suspended = false

    await this.ready()
    if (this.suspended || this.discovery.destroyed) return

    this.discovery.setAnnounce(this.isServer)
    this.discovery.resume({ log }) // re-runs the announce or lookup
  }

  async _close() {
    this.store.removeListener('close', this._onstoreclose)

    this.isServer = false
    this.isClient = false
    this._discoverySessions.clear()

    if (this._previous) await this._previous.close()
    if (this.discovery) await this.discovery.destroy()
  }
}

function noop() {}
