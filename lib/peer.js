const unslab = require('unslab')
const b4a = require('b4a')

const DIAL_ATTEMPTS_MAX = 3
const DIAL_ATTEMPTS_COOLDOWN = 1000 * 60
const DIAL_ATTEMPTS_RESET = 10 * 1000

const BAN_DURATION = 1000 * 60 * 30

// legacy announce-refresh is 10(+2) minutes
// https://github.com/holepunchto/replicator-discovery/blob/main/index.js#L7-L9
const EXPIRE_DURATION = 1000 * 60 * 15 // 15 min

class Peer {
  publicKey = null
  relayAddresses = []
  forceRelaying = false

  seenCount = 0

  lastSeenAt = -1
  attemptedAt = -1
  bannedAt = -1
  banReason = null
  connectedAt = -1
  disconnectedAt = -1

  timeConnected = 0
  _connectedCount = 0
  _disconnectedCount = 0

  _topics = new Map() // hex => seenAt

  _outboundAttempts = 0
  _outboundSucess = 0
  _outboundFailure = 0
  _inboundAttempts = 0
  _inboundSuccess = 0
  _inboundFailures = 0

  #created = Date.now()

  // dialer owned
  _refreshedAt = -1

  constructor(publicKey) {
    this.publicKey = unslab(publicKey)
  }

  shouldDial(now = Date.now()) {
    if (this.isBanned) return false

    // peer is not announcing
    if (this.lastSeenAt === -1) return false

    if (this.isDialing) return false

    if (this.isExpired(now)) return false

    // TODO: at least one of _topics() is active in replicator

    return this.retryAt <= now
  }

  get topics() {
    return Array.from(this._topics.keys())
  }

  get isBanned() {
    return this.bannedAt > 0
  }

  setBanned(reason = 'banned', now = Date.now()) {
    this.bannedAt = now
    this.banReason = reason
  }

  get expiresAt() {
    if (this.isBanned) {
      return this.bannedAt + BAN_DURATION
    }

    const lastActive = Math.max(this.lastSeenAt, this.disconnectedAt)

    return lastActive + EXPIRE_DURATION
  }

  isExpired(now = Date.now()) {
    if (this.connected) return false

    // In the unlikely event of an upsert w/o seen()
    if (!this.isBanned && this.lastSeenAt === -1 && this.disconnectedAt === -1) {
      return Date.now() - this.#created > 10_000
    }

    return now > this.expiresAt
  }

  get connected() {
    return this._connectedCount > this._disconnectedCount
  }

  get isDialing() {
    return this._isAttempting
  }

  get retryAt() {
    if (!this._outboundAttempts) return this.lastSeenAt

    let backoff = DIAL_ATTEMPTS_COOLDOWN

    const attempts = this._outboundAttempts

    if (attempts <= DIAL_ATTEMPTS_MAX) {
      // retry-burst: 1s, 2.5s, 6s, 15s, 36s ...
      backoff = Math.exp(attempts * 0.9027)
    }

    return this.attemptedAt + backoff
  }

  get _nextAt() {
    return Math.min(this.retryAt, this.expiresAt)
  }

  observe(topic, now = Date.now()) {
    this.seenCount++
    this.lastSeenAt = now

    const hex = b4a.toString(topic, 'hex')
    this._topics.set(hex, now)
  }

  updateRelays(relayAddresses) {
    this.relayAddresses = relayAddresses
  }

  isOrphaned(sessions) {
    for (const hex of this._topics.keys()) {
      if (sessions.has(hex)) return false
    }

    return true
  }

  // TODO: rework markAttempts api
  markAttempt(initiator, { canRelay = false, now = Date.now() } = {}) {
    this.attemptedAt = now
    this._isAttempting = true

    if (initiator) {
      this._outboundAttempts++
    } else {
      this._inboundAttempts++
    }

    let done = false
    return (failed = false, cause = null, now = Date.now()) => {
      if (done) throw new Error('assert fail: attempt already settled')
      done = true
      this._isAttempting = false

      if (failed) {
        // @since v0.5: cause normalized to 'string'
        cause = cause?.code || cause

        if (initiator) {
          this._outboundFailure++

          if (canRelay && !this.forceRelaying && shouldForceRelaying(cause)) {
            this.forceRelaying = true
            this._outboundAttempts = 0 // fastconnect to relay
          }
          const reason = shouldBan(cause)
          if (reason) {
            this.setBanned(reason, now)
            // TODO:
            // replicator.emit('ban', peer)
          }
        }

        // TODO: peer `expireAt` change; fast-gc
        // dialer._maybeDeletePeer(peer)
      } else {
        this._outboundSucess++
      }
    }
  }

  markConnected(initiator, now = Date.now()) {
    this.connectedAt = now
    this._connectedCount++
  }

  markDisconnected(initiator, cause, now = Date.now()) {
    this._disconnectedCount++
    const delta = now - this.connectedAt
    this.timeConnected += delta
    this.disconnectedAt = now

    // connection survived gc,
    // reset attempts after 10s connected time
    if (delta > DIAL_ATTEMPTS_RESET) {
      this._outboundAttempts = 0
    }
  }
}

function shouldBan(cause) {
  // TODO: detect incoming hammering
  // use outbound/inbound success/failure rate to open gate.
  return false
}

function shouldForceRelaying(code) {
  return (
    code === 'HOLEPUNCH_ABORTED' ||
    code === 'HOLEPUNCH_DOUBLE_RANDOMIZED_NATS' ||
    code === 'REMOTE_NOT_HOLEPUNCHABLE'
  )
}

module.exports = Peer
