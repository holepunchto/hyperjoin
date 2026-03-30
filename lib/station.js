const b4a = require('b4a')
const PeerInfo = require('./peer-info')
const RetryTimer = require('./retry-timer')
const MAX_PARALLEL = 3
// const MAX_CLIENT_CONNECTIONS = Infinity // TODO: Change
// const MAX_SERVER_CONNECTIONS = Infinity
const ERR_FIREWALL = 'Peer is firewalled'

class PeerStation {
  constructor(replicator, opts = {}) {
    this.replicator = replicator // TODO: remove direct reference
    const {
      firewall,
      suspended
    } = opts
    this._firewall = firewall

    this._timer = new RetryTimer(this._requeue.bind(this), {
      backoffs: opts.backoffs,
      jitter: opts.jitter
    })

    this.peers = new Map() // hex(peerId) -> PeerInfo
    this.suspended = suspended
    this.destroyed = false
  }

  _requeue() {
    // TODO
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
    if (peerInfo.explicit) return true

    for (const topic of peerInfo.topics) {
      const key = toHex(topic)
      if (this.replicator.sessions.has(key)) return true
    }

    return false
  }

  handleFirewall(remotePublicKey, payload) {
    let peerInfo = this.peers.get(b4a.toString(remotePublicKey, 'hex'))
    if (peerInfo && peerInfo.banned) return true

    const firewalled = this._firewall(remotePublicKey, payload)

    if (firewalled) {
      if (!peerInfo) peerInfo = this.lookup(remotePublicKey)
      this._banPeer(peerInfo, true, new Error(ERR_FIREWALL))

      // if (queued) this._flushMaybe(peerInfo)
    }

    return firewalled
  }

  _banPeer(peerInfo, banned, err) {
    peerInfo.ban(banned)
    // this.replicator.stats.bannedPeers++ // TODO: backport stats
    this.replicator.emit('ban', peerInfo, err)
  }

  seen (dhtPeer, topic) {
    const peerInfo = this.lookup(dhtPeer.publicKey, dhtPeer.relayAddresses)
    peerInfo._topic(topic)
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

    // pre-init requeue
    peerInfo.waiting = this._shouldRequeue(peerInfo) && this._timer.add(peerInfo)
    // this._maybeDeletePeer(peerInfo) // TODO: moved to _gc(); verify side-effects
  }

  collectGarbage() { // TODO: maybe restore the old auto
    for (const [key, info] of this.peers) {
      if (!info.shouldGC()) continue
      this.peers.delete(key)
    }
  }

  async suspend({ log = noop } = {}) {
    this.suspended = true
    // pause timers
  }
  async resume({ log = noop } = {}) {
    this.suspended = false
    // resume timers
  }

  destroy() {
    this._timer.destroy()
    this.destroyed = true
    this.peers.clear()
  }

  disconnected (peerId, error) {
    const info = this.lookup(peerId)

    info.waiting = this._shouldRequeue(info) && this._timer.add(info)

    if (shouldBan(error)) {
      this._banPeer(info, true, error)
    }

    // this._maybeDeletePeer(peerInfo) // moved to _gc()
    // this._attemptClientConnections() // TODO: not yet implemented

    info._disconnected()
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
