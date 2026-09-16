const b4a = require('b4a')
const Peer = require('./peer')
const spq = require('shuffled-priority-queue')
const ScopeLock = require('scope-lock')

const MAX_PARALLEL = 3
const ERR_FIREWALL = 'Peer is firewalled'

class Dialer {
  constructor(replicator, opts = {}) {
    this.replicator = replicator // TODO: remove direct reference

    const { firewall, suspended = false, maxParallel = MAX_PARALLEL, jitter = 500 } = opts
    this._firewall = firewall
    this.maxParallel = maxParallel
    this.jitter = jitter // exposed: every requeue dial inherits the phase

    this._bans = new Map() // hex -> expiry: ban memory, independent of topic liveness

    if (!this.suspended) this._scheduleScan() // the requeue engine: Peer owns the clocks, the dialer owns the phase
    this.peers = new Map() // hex(peerId) -> Peer
    this.suspended = suspended
    this.destroyed = false

    this._queue = spq()
    this._flushes = []
    this._drainLock = new ScopeLock({ debounce: true })
  }

  _enqueue(peer) {
    if (this._queue.has(peer)) return

    this._queue.add(peer)

    this._attemptClientConnections()
  }

  _dialable(peer, now) {
    return !this._bans.has(toHex(peer.publicKey)) && peer.shouldDial(now)
  }

  _scheduleScan () {
    clearTimeout(this._scanTimeout) // idempotent arm: double resume can never multiply the chain
    const myTimer = setTimeout(() => {
      if (this._scanTimeout !== myTimer) return // stale generation: aborted or superseded
      this._scan()
      if (!this.destroyed && this._scanTimeout === myTimer) this._scheduleScan() // self-chaining, like BulkTimer; no fixed tick
    }, this.jitter + this.jitter * Math.random())
    this._scanTimeout = myTimer
    myTimer.unref() // a dialer never pins the process
  }

  _scan() {
    if (this.suspended || this.destroyed) return

    const now = Date.now()

    for (const peer of this.peers.values()) {
      if (!this._dialable(peer, now)) continue
      if (!this._shouldRequeue(peer)) continue
      if (this.replicator.hasConnection(peer.publicKey)) continue
      this._enqueue(peer)
    }
  }

  _flushMaybe() {
    const rep = this.replicator
    if (rep.connecting > 0) return false
    if (this._queue.length && this._withinMaxPeers() && !rep.isPresuspended) return false

    while (this._flushes.length) {
      const flush = this._flushes.pop()
      flush(true)
    }

    return true
  }

  _withinMaxPeers() {
    const rep = this.replicator
    return rep.connectionCount < rep._maxPeers
  }

  _shouldConnect() {
    const rep = this.replicator
    return (
      !this.destroyed &&
      !this.suspended &&
      !rep.isPresuspended &&
      rep.connecting < this.maxParallel &&
      this._withinMaxPeers() &&
      rep.outbound.size < this.replicator._maxPeers
    )
  }

  async flush() {
    const allFlushed = []

    for (const session of this.replicator.sessions.values()) {
      allFlushed.push(session.ready().then(() => session.discovery.flushed()))
    }

    await Promise.all(allFlushed)

    if (this._flushMaybe()) return true

    return new Promise((resolve) => {
      this._flushes.push(resolve)
    })
  }

  lookup(publicKey, relayAddresses) {
    if (b4a.equals(publicKey, this.replicator.peerId)) return null

    const key = toHex(publicKey)

    let peer = this.peers.get(key)
    if (!peer) {
      peer = new Peer(publicKey)
      this.peers.set(key, peer)
    }

    // new is always better
    if (relayAddresses) peer.relayAddresses = relayAddresses

    return peer
  }

  _shouldRequeue(peer) {
    if (this.suspended) return false
    if (this.destroyed) return false
    if (peer.isOrphaned(this.replicator.sessions)) return false // nothing the dialer could serve

    return true
  }

  // Called when the PeerQueue indicates a connection should be attempted.
  async _attemptClientConnections() {
    // Guard against re-entries - unsure if it still needed but doesn't hurt
    if (this.suspended || this.destroyed) return

    const acquired = await this._drainLock.lock()
    if (!acquired) return

    while (this._queue.length && this._shouldConnect()) {
      const peer = this._queue.shift()

      if (!this._dialable(peer, Date.now())) continue // the verdict may have moved since enqueue

      this.replicator
        ._connect(peer)
        .catch((err) => this.replicator.emit('error', err))
        .then(() => {
          this._flushMaybe()
          this._attemptClientConnections()
        })
    }

    this._drainLock.unlock()
    this._flushMaybe()
  }

  async handleFirewall(remotePublicKey, payload) {
    const hex = toHex(remotePublicKey)
    if (this._bans.has(hex)) return true // enforcement memory outlives the record

    let peer = this.peers.get(hex)

    const firewalled = await this._firewall(remotePublicKey, payload)

    if (firewalled) {
      if (!peer) peer = this.lookup(remotePublicKey)
      this._banPeer(peer, true, new Error(ERR_FIREWALL))
    }

    return firewalled
  }

  _banPeer(peer, banned, err) {
    if (banned) {
      peer.setBanned(Date.now())
      this._bans.set(toHex(peer.publicKey), peer.expiresAt) // the TTL statement lives in Peer
    }
    this.replicator.stats.bannedPeers++
    this.replicator.emit('ban', peer, err)
  }

  seen(dhtPeer, topic) {
    const { publicKey, relayAddresses } = dhtPeer
    const peer = this.lookup(publicKey, relayAddresses)
    if (!peer) return

    const now = Date.now()
    const rep = this.replicator
    const isConnected = rep.hasConnection(peer.publicKey)
    const dialable = this._dialable(peer, now) // gate BEFORE observe: retryAt is due since the previous sighting

    peer.observe(now, topic)

    if (!isConnected && dialable) this._enqueue(peer)
  }

  connectFailed(peerId, error) {
    const peer = this.lookup(peerId)
    const now = Date.now()

    if (this.replicator.relayThrough && shouldForceRelaying(error.code)) {
      peer.forceRelaying = true
      peer.attempts = 0 // fastconnect to relay
    }

    peer.markAttemptDone(now, true) // refusal verdict: the dial never opened

    if (shouldBan(error)) {
      // pre-init ban
      this._banPeer(peer, true, error)
    }
    this._maybeDeletePeer(peer)
  }

  _maybeDeletePeer(peer) {
    if (this._queue.has(peer)) return
    if (this.replicator.hasConnection(peer.publicKey)) return
    if (!peer.isRetired && this._shouldRequeue(peer)) return // pending retry: the scan will requeue

    this.peers.delete(toHex(peer.publicKey)) // retired or irrelevant: amnesia (a sighting can resurrect)
  }

  collectGarbage() {
    const sessions = this.replicator.sessions
    const now = Date.now()

    for (const [hex, expiry] of this._bans) {
      if (now > expiry) this._bans.delete(hex) // strict: dies with the record, not a tick early
    }

    for (const [key, peer] of this.peers) {
      if (this.replicator.hasConnection(key)) continue // connected

      if (peer.isOrphaned(sessions) || peer.isExpired(now)) this.peers.delete(key)
    }
  }

  suspend({ log = noop } = {}) {
    this.suspended = true
    clearTimeout(this._scanTimeout) // abort the chain; resume re-arms
    this._scanTimeout = null // aborted: stale fires must self-identify and stay dead

    this._queue = spq()
    log('dialer suspended')
  }

  resume({ log = noop } = {}) {
    if (this.destroyed) return // a dead dialer never revives
    this.suspended = false
    this._scheduleScan() // restart the chain suspend aborted
    this._attemptClientConnections()
    log('dialer resumed')
  }

  destroy() {
    clearTimeout(this._scanTimeout)
    this._scanTimeout = null
    this._drainLock.destroy()
    this.destroyed = true
    this.peers.clear()
    while (this._flushes.length) this._flushes.pop()(false)
  }

  disconnected(peerId, error) {
    const peer = this.lookup(peerId)

    const now = Date.now()
    peer.markDisconnected(now)

    if (shouldBan(error)) {
      this._banPeer(peer, true, error)
    }

    this._maybeDeletePeer(peer)
    this._attemptClientConnections()
  }
}

function shouldForceRelaying(code) {
  return (
    code === 'HOLEPUNCH_ABORTED' ||
    code === 'HOLEPUNCH_DOUBLE_RANDOMIZED_NATS' ||
    code === 'REMOTE_NOT_HOLEPUNCHABLE'
  )
}

function shouldBan() {
  // return !!err && err.name === 'HypercoreError' && err.code === 'INVALID_OPERATION'
  return false
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

function noop() {}

module.exports = Dialer
