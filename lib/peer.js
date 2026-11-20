const unslab = require('unslab')
const b4a = require('b4a')

const DIAL_BACKOFF_MAX = 10 * 60 * 1000
const DIAL_ATTEMPTS_RESET = 60 * 1000
const HAMMERING_THRESHOLD = 10 * 1000

const BAN_DURATION = 1000 * 60 * 30
const BAN_HAMMER_ATTEMPTS = 20
const HAMMER_WINDOW = 10 * 60 * 1000 // short disconnects older than this are forgiven

const ERR_SUSPEND = 'SuspendDrop'

// legacy announce-refresh is 10(+2) minutes
// https://github.com/holepunchto/hyperannounce/blob/main/index.js#L7-L9
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
  scannedAt = -1 // last seen by a discovering session

  timeConnected = 0
  _connectedCount = 0
  _disconnectedCount = 0
  _shortDisconnects = [] // recent short inbound disconnects

  _topics = new Map() // hex => seenAt

  _outboundAttempts = 0
  _outboundSuccess = 0
  _outboundFailures = 0
  _inboundAttempts = 0
  _inboundSuccess = 0
  _inboundFailures = 0

  #created = Date.now()

  // dialer owned
  _refreshedAt = -1
  _banReported = false

  constructor(publicKey) {
    this.publicKey = unslab(publicKey)
  }

  shouldDial(now = Date.now()) {
    if (this.isBanned) return false

    // peer is not announcing
    if (this.lastSeenAt === -1) return false

    if (this.isDialing) return false

    if (this.isExpired(now)) return false

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

    // clear memory
    this._shortDisconnects = []
    this.relayAddresses = []
    this._topics.clear()
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

    const backoff = Math.min(Peer.backoffDuration(this._outboundAttempts), DIAL_BACKOFF_MAX)

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

  // TODO: rework dial-attempt api
  markAttempt(initiator, { canRelay = false, now = Date.now() } = {}) {
    const prevAttemptAt = this.attemptedAt
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
      this.attemptedAt = now // update

      if (failed) {
        // @since v0.5: cause normalized to 'string'
        cause = cause?.code || cause

        if (initiator) {
          this._outboundFailures++

          if (cause === ERR_SUSPEND) {
            if (this._outboundAttempts > 0) this._outboundAttempts--
            this._refreshedAt = -1
            this.attemptedAt = prevAttemptAt
          } else if (canRelay && !this.forceRelaying && shouldForceRelaying(cause)) {
            this.forceRelaying = true
            this._outboundAttempts = 0 // fastconnect to relay

            // reschedule
            this._refreshedAt = -1
            // this.lastSeenAt++
          }
        } else {
          this._inboundFailures++
        }

        // Process bans-preconnect
        const reason = this.shouldBan(initiator, cause, { prevAttemptAt })
        if (reason) {
          this.setBanned(reason, now)
        }
      } else {
        if (initiator) {
          this._outboundSuccess++
        } else {
          this._inboundSuccess++
        }
      }
    }
  }

  resetAttempts() {
    this._outboundAttempts = 0
    this._inboundAttempts = 0
    this._refreshedAt = -1 // let refresh() re-process
  }

  markConnected(initiator, now = Date.now()) {
    this.connectedAt = now
    this._connectedCount++
  }

  markDisconnected(initiator, cause, now = Date.now()) {
    // @since v0.5: cause normalized to 'string'
    cause = cause?.code || cause

    this._disconnectedCount++
    const delta = now - this.connectedAt
    this.timeConnected += delta
    this.disconnectedAt = now
    if (initiator) this.lastSeenAt = now

    if (!initiator && delta < HAMMERING_THRESHOLD) {
      this._shortDisconnects = this._shortDisconnects.filter((t) => t > now - HAMMER_WINDOW)
      this._shortDisconnects.push(now)
    }

    // Process bans-postconnect
    const reason = this.shouldBan(initiator, cause)
    if (reason) {
      this.setBanned(reason, now)
    }

    // connection survived gc,
    // reset attempts after 60s connected time
    if (delta > DIAL_ATTEMPTS_RESET) {
      this._outboundAttempts = 0
      this._inboundAttempts = 0
    }
  }

  shouldBan(initiator, cause /* opts = {} */) {
    if (initiator) {
      // outbound
    } else {
      // inbound

      if (this._shortDisconnects.length >= BAN_HAMMER_ATTEMPTS) return 'hammering'
    }

    return null
  }

  static backoffDuration(nAttempts) {
    // 1s, 2.5s, 6s, 15s, 36s ...
    return 1000 * Math.exp(nAttempts * 0.9027)
  }
}

function shouldForceRelaying(code) {
  return (
    code === 'HOLEPUNCH_ABORTED' ||
    code === 'HOLEPUNCH_DOUBLE_RANDOMIZED_NATS' ||
    code === 'REMOTE_NOT_HOLEPUNCHABLE'
  )
}

module.exports = Peer
