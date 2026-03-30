const b4a = require('b4a')
const PeerInfo = require('./peer-info')
const RetryTimer = require('./retry-timer')
const spq = require('shuffled-priority-queue')
const ScopeLock = require('scope-lock')

const MAX_PARALLEL = 3
const MAX_CLIENT_CONNECTIONS = Infinity // TODO: Change
const MAX_SERVER_CONNECTIONS = Infinity
const ERR_FIREWALL = 'Peer is firewalled'

class PeerStation {
  constructor(replicator, opts = {}) {
    this.replicator = replicator // TODO: remove direct reference

    const {
      firewall,
      suspended = false,
      maxParallel = MAX_PARALLEL,
      maxClientConnections = MAX_CLIENT_CONNECTIONS,
      maxServerConnections = MAX_SERVER_CONNECTIONS
    } = opts
    this._firewall = firewall
    this.maxParallel = maxParallel
    this.maxClientConnections = maxClientConnections
    this.maxServerConnections = maxServerConnections

    this._timer = new RetryTimer(this._requeue.bind(this), {
      backoffs: opts.backoffs,
      jitter: opts.jitter
    })

    this.peers = new Map() // hex(peerId) -> PeerInfo
    this.explicitPeers = new Set()
    this.suspended = suspended
    this.destroyed = false

    this._queue = spq()
    this._flushes = []
    this._drainLock = new ScopeLock({ debounce: true })
  }

  _enqueue(peerInfo) {
    if (peerInfo.queued) return

    peerInfo.queued = true
    this._queue.add(peerInfo)

    this._attemptClientConnections()
  }

  _requeue(batch) {
    if (this.suspended) return

    for (const peerInfo of batch) {
      peerInfo.waiting = false

      if (
        peerInfo._updatePriority() === false ||
        this.replicator.hasConnection(peerInfo.publicKey) ||
        peerInfo.queued
      ) {
        continue
      }

      peerInfo.queued = true
      this._queue.add(peerInfo)
    }

    this._attemptClientConnections()
  }

  _flushMaybe() {
    const rep = this.replicator
    if (this._queue.length || rep.connecting > 0) return false

    while (this._flushes.length) {
      const flush = this._flushes.pop()
      flush(true)
    }

    return true
  }

  _withinMaxPeers() {
    const rep = this.replicator
    const e = this.explicitPeers.size
    const factor = e === 0 ? 1 : e < 2 ? 2 : e < 4 ? 3 : 4
    return factor * rep.connectionCount < rep._maxPeers
  }

  _shouldConnectExplicit() {
    const rep = this.replicator
    return !this.destroyed && !this.suspended && rep.connecting < this.maxParallel
  }

  _shouldConnect() {
    const rep = this.replicator
    return (
      !this.destroyed &&
      !this.suspended &&
      rep.connecting < this.maxParallel &&
      this._withinMaxPeers() &&
      rep.outbound.size < this.maxClientConnections
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

    let peerInfo = this.peers.get(key)
    if (!peerInfo) {
      peerInfo = new PeerInfo({ publicKey })
      this.peers.set(key, peerInfo)
    }

    // new is always better
    if (relayAddresses) peerInfo.relayAddresses = relayAddresses

    return peerInfo
  }

  _shouldRequeue(peerInfo) {
    if (this.suspended) return false
    if (this.destroyed) return false
    if (peerInfo.banned) return false
    if (peerInfo.explicit) return true

    for (const topic of peerInfo.topics) {
      const key = toHex(topic)
      if (this.replicator.sessions.has(key)) return true
    }

    return false
  }

  // Called when the PeerQueue indicates a connection should be attempted.
  async _attemptClientConnections() {
    // Guard against re-entries - unsure if it still needed but doesn't hurt
    if (this.suspended || this.destroyed) return

    const acquired = await this._drainLock.lock()
    if (!acquired) return

    for (const peerInfo of this.explicitPeers) {
      if (!this._shouldConnectExplicit()) break
      if (
        peerInfo.attempts >= 5 ||
        Date.now() - peerInfo.disconnectedTime < peerInfo.attempts * 1000
      ) {
        continue
      }

      this.replicator._connect(peerInfo, false)
        .catch(err => this.replicator.emit('error', err))
    }

    while (this._queue.length && this._shouldConnect()) {
      const peerInfo = this._queue.shift()
      peerInfo.queued = false

      this.replicator._connect(peerInfo, true)
        .catch(err => this.replicator.emit('error', err))
        .then(() => {
          this._flushMaybe()
          this._attemptClientConnections()
        })
    }

    this._drainLock.unlock()
    this._flushMaybe()
  }

  handleFirewall(remotePublicKey, payload) {
    let peerInfo = this.peers.get(toHex(remotePublicKey))
    if (peerInfo && peerInfo.banned) return true

    const firewalled = this._firewall(remotePublicKey, payload)

    if (firewalled) {
      if (!peerInfo) peerInfo = this.lookup(remotePublicKey)
      this._banPeer(peerInfo, true, new Error(ERR_FIREWALL))
    }

    return firewalled
  }

  _banPeer(peerInfo, banned, err) {
    peerInfo.ban(banned)
    this.replicator.stats.bannedPeers++
    this.replicator.emit('ban', peerInfo, err)
  }

  seen (dhtPeer, topic) {
    const { publicKey, relayAddresses } = dhtPeer
    const peerInfo = this.lookup(publicKey, relayAddresses)

    peerInfo._topic(topic)

    const rep = this.replicator

    if (!peerInfo || rep.hasConnection(peerInfo.publicKey)) return
    if (!peerInfo.prioritized || peerInfo.server) peerInfo._reset()
    if (peerInfo._updatePriority()) {
      this._enqueue(peerInfo)
    }
  }

  connectFailed(peerId, error) {
    const peerInfo = this.lookup(peerId)
    if (
      this.replicator.relayThrough &&
      shouldForceRelaying(error.code)
    ) {
      peerInfo.forceRelaying = true
      peerInfo.attempts = 0 // fastconnect to relay
    }

    if (shouldBan(error)) { // pre-init ban
      this._banPeer(peerInfo, true, error)
    }

    peerInfo.waiting = this._shouldRequeue(peerInfo) && this._timer.add(peerInfo)
    this._maybeDeletePeer(peerInfo)
  }

  _maybeDeletePeer(peerInfo) {
    if (peerInfo.shouldGC()) {
      this.peers.delete(toHex(peerInfo.publicKey))
    }
  }

  collectGarbage() {
    const sessions = this.replicator.sessions

    for (const [key, info] of this.peers) {
      let orphaned = true
      for (const hex of info._seenTopics) {
        if (!orphaned) break
        if (sessions.has(hex)) orphaned = false
      }

      if (orphaned || info.shouldGC()) {
        this.peers.delete(key)
      }
    }
  }

  async suspend({ log = noop } = {}) {
    this.suspended = true
    // pause timers
  }
  async resume({ log = noop } = {}) {
    this.suspended = false
    this._attemptClientConnections()
  }

  destroy() {
    this._drainLock.destroy()
    this._timer.destroy()
    this.destroyed = true
    this.peers.clear()
    while (this._flushes.length) this._flushes.pop()(false)
  }

  disconnected (peerId, error) {
    const peerInfo = this.lookup(peerId)

    peerInfo._disconnected()

    if (shouldBan(error)) {
      this._banPeer(peerInfo, true, error)
    }

    peerInfo.waiting = this._shouldRequeue(peerInfo) && this._timer.add(peerInfo)

    this._maybeDeletePeer(peerInfo)
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

module.exports = PeerStation
