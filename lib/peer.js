const unslab = require('unslab')
const LADDER = [1_000, 5_000, 15_000] // S/M/L from retry-timer.js
const COOLDOWN = 60_000 // RETIRED clock; the one open calibration knob

class Peer {
  publicKey = null
  relayAddresses = []

  lastSeenAt = -1
  connectedAt = -1
  disconnectedAt = -1
  attemptedAt = -1
  _bannedAt = -1
  timeConnected = 0
  _connectedCount = 0
  _disconnectedCount = 0

  seenCount = 0
  attempts = 0
  _attemptsSuceeded = 0
  _attemptsFailed = 0
  _isAttempting = false

  constructor (publicKey) {
    this.publicKey = unslab(publicKey)
  }

  shouldDial (now = Date.now()) {
    return !this.isBanned &&
      this.retryAt < now &&
      (now - this.lastSeenAt) < COOLDOWN * 2 &&
      !this.isAttempting &&
      !this.isExpired(now)
  }

  isExpired (now = Date.now()) {
    const expiresAt = now - Math.max(this.lastSeenAt, this.disconnectedAt)

    if (this.isBanned) {
      // Max GC retain
      return expiresAt > COOLDOWN * 20
    }

    return expiresAt > COOLDOWN * 3 && !this.connected
  }

  get connected () {
    return this._connectedCount > this._disconnectedCount
  }

  get isAttempting () {
    return this._isAttempting
  }

  get retryAt () {
    if (!this.attempts) return this.lastSeenAt

    let backoff = COOLDOWN

    if (this.attempts <= LADDER.length) {
      backoff = LADDER[this.attempts - 1]
    }

    return this.attemptedAt + backoff
  }

  get isBanned () {
    return this._bannedAt > 0
  }

  setBan(now = Date.now()) {
    this._bannedAt = now
  }


  observe(now = Date.now()) {
    this.seenCount++
    const delta = now - this.lastSeenAt
    this.lastSeenAt = now
  }

  markAttempt(now = Date.now()) {
    this.attempts++
    this.attemptedAt = now
    this._isAttempting = true
  }

  markConnected(now = Date.now()) {
    this.connectedAt = now
    this._connectedCount++
  }

  markDisconnected(now = Date.now()) {
    this._disconnectedCount++
    const delta = now - this.connectedAt
    this.timeConnected += delta
    this.disconnectedAt = now

    if (delta > COOLDOWN / 2) {
      this.attempts = 0 // redeemed
    }
  }

  markAttemptDone(now = Date.now(), failed) {
    if (failed) {
      this._attemptsFailed++
    } else {
      this._attemptsSuceeded++
    }

    this._isAttempting = false

    const attemptDuration = now - this.attemptedAt

    return attemptDuration
  }
}

module.exports = Peer
