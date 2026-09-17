const unslab = require('unslab')
const b4a = require('b4a')

const LADDER = [1_000, 5_000, 15_000] // Math.exp(attempts * 0.9027)
const COOLDOWN = 60_000 // RETIRED clock; the one open calibration knob

class Peer {
  publicKey = null
  relayAddresses = []
  forceRelaying = false

  lastSeenAt = -1
  bannedAt = -1
  connectedAt = -1
  disconnectedAt = -1
  attemptedAt = -1
  timeConnected = 0
  _connectedCount = 0
  _disconnectedCount = 0

  seenCount = 0

  _topics = new Map()

  _outboundAttempts = 0
  _outboundSucess = 0
  _outboundFailure = 0
  _inboundAttempts = 0
  _inboundSuccess = 0
  _inboundFailures = 0

  constructor (publicKey) {
    this.publicKey = unslab(publicKey)
  }

  shouldDial (now = Date.now()) {
    if (this.isDialing) return false

    // When does this occur??
    // if (this.lastSeenAt === -1 && !this.attempts) return true // born dialable: no clock has started

    return this.retryAt < now &&
      (now - this.lastSeenAt) < COOLDOWN * 2 &&
      !this.isExpired(now)
  }

  get isBanned () {
    return this.bannedAt > 0
  }

  setBanned (now = Date.now()) {
    this.bannedAt = now
  }

  get expiresAt () {
    if (this.isBanned) {
      return this.bannedAt + COOLDOWN * 30
    }

    return Math.max(this.lastSeenAt, this.disconnectedAt) + COOLDOWN * 3
  }

  isExpired (now = Date.now()) {
    // TODO: unslop (not correct)
    if (this.connected) return false
    if (this.lastSeenAt === -1 && this.disconnectedAt === -1 && !this.isBanned) return false // no clock started: orphan GC drops these, not staleness - a ban carries its own clock

    return now > this.expiresAt
  }

  get connected () {
    return this._connectedCount > this._disconnectedCount
  }

  get isDialing () {
    return this._isAttempting
  }

  get retryAt () {
    if (!this._outboundAttempts) return this.lastSeenAt

    let backoff = COOLDOWN

    if (this._outboundAttempts <= LADDER.length) {
      backoff = LADDER[this._outboundAttempts - 1]
    }

    return this.attemptedAt + backoff
  }

  observe(topic, now = Date.now()) {
    this.seenCount++
    this.lastSeenAt = now

    const hex = b4a.toString(topic, 'hex')
    this._topics.set(hex, now)

    // Legacy logic PeerInfo._reset() on
    // !prioritized : Attempts exhausted || stale (tried && !proven)
    // announcing : attempted to isolate inbound errors from outbound?

    // this.emit('topic', topic) <-- ?? dead signal?
  }

  updateRelays(relayAddresses) {
    this.relayAddresses = relayAddresses
  }

  isOrphaned (sessions) {
    for (const hex of this._topics.keys()) {
      if (sessions.has(hex)) return false
    }

    return true
  }

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

      if (failed) {
        cause = cause?.code || cause

        if (initiator) {
          this._outboundFailure++

          if (canRelay && !this.forceRelaying && shouldForceRelaying(cause)) {
            this.forceRelaying = true
            this._outboundAttempts = 0 // fastconnect to relay
          }

          if (shouldBan(cause)) {
            this.setBanned(now)
          }
        }

        // this._maybeDeletePeer(peer) // TODO: move to dialer gc
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

    if (delta > COOLDOWN / 2) {
      this._outboundAttempts = 0 // redeemed
    }
  }

}

function shouldBan(cause) {
  // TODO: detect incoming hammering
  // use outbound/inbound success/failure rate to open gate.
}

function shouldForceRelaying(code) {
  return (
    code === 'HOLEPUNCH_ABORTED' ||
    code === 'HOLEPUNCH_DOUBLE_RANDOMIZED_NATS' ||
    code === 'REMOTE_NOT_HOLEPUNCHABLE'
  )
}

module.exports = Peer
