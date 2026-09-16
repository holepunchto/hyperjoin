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
  dials = 0
  _dialsSuceeded = 0
  _dialsFailed = 0
  _isDialing = false

  _topics = new Set()

  _inboundAttempts = 0

  get connectedTime () {
    return this.connectedAt
  }

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
    if (this.connected) return false
    if (this.lastSeenAt === -1 && this.disconnectedAt === -1 && !this.isBanned) return false // no clock started: orphan GC drops these, not staleness - a ban carries its own clock

    return now > this.expiresAt
  }

  get connected () {
    return this._connectedCount > this._disconnectedCount
  }

  get isDialing () {
    return this._isDialing
  }

  get retryAt () {
    if (!this.dials) return this.lastSeenAt

    let backoff = COOLDOWN

    if (this.dials <= LADDER.length) {
      backoff = LADDER[this.dials - 1]
    }

    return this.attemptedAt + backoff
  }

  observe(now = Date.now(), topic, incoming = false) {
    this.seenCount++
    this.lastSeenAt = now

    const hex = b4a.toString(topic, 'hex')
    this._topics.set(hex, now)

    // this.emit('topic', topic) <-- ?? dead signal?
  }

  isOrphaned (sessions) {
    for (const hex of this._topics.keys()) {
      if (sessions.has(hex)) return false
    }

    return true
  }

  markAttempt(initiator, now = Date.now()) {
    if (initiator) {
      this.dials++
      this.attemptedAt = now
      this._isDialing = true
    } else {
      this._inboundAttempts++
    }

  }

  markAttemptDone(initiator, failed, now = Date.now()) {
    if (initiator) {
      if (failed) {
        this._dialsFailed++
      } else {
        this._dialsSuceeded++
      }
    }

    this._isDialing = false
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
      this.dials = 0 // redeemed
    }
  }

}

module.exports = Peer
