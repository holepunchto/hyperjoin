const unslab = require('unslab')
const b4a = require('b4a')

const DIAL_BACKOFF_MAX = 10 * 60 * 1000
const DIAL_ATTEMPTS_RESET = 60 * 1000
const HAMMERING_THRESHOLD = 10 * 1000

const BAN_DURATION = 1000 * 60 * 30
const BAN_HAMMER_ATTEMPTS = 20
const HAMMER_WINDOW = 10 * 60 * 1000 // short disconnects older than this are forgiven

// legacy announce-refresh is 10(+2) minutes
// https://github.com/holepunchto/replicator-discovery/blob/main/index.js#L7-L9
const EXPIRE_DURATION = 1000 * 60 * 15 // 15 min

// Disconnects we caused ourselves, these never count against the remote
const LOCAL_CAUSES = new Set([
  'SuspendDrop',
  'DuplicateConnection',
  'IdleConnection',
  'MaxPeersCap',
  'ReplicatorDestroyed',
  'suspending'
])

class Peer {
  publicKey = null
  relayAddresses = []
  forceRelaying = false

  seenCount = 0

  lastSeenAt = -1
  attemptedAt = -1 // last outbound attempt
  inboundAt = -1 // last inbound attempt
  bannedAt = -1
  banReason = null
  connectedAt = -1
  disconnectedAt = -1
  timeConnected = 0

  isDialing = false
  refreshedAt = -1 // owned by the dialer, last refresh() that visited this peer

  _connectedCount = 0
  _disconnectedCount = 0

  _shortDisconnects = [] // times of the recent short, remote-caused disconnects

  _joined = 0 // joinPeer() references
  _joinedAt = -1 // when it was first joined, schedules the first dial

  _topics = new Map() // hex => seenAt

  _outboundAttempts = 0
  _outboundSuccess = 0
  _outboundFailures = 0
  _inboundAttempts = 0
  _inboundSuccess = 0
  _inboundFailures = 0

  #created = Date.now()

  constructor(publicKey) {
    this.publicKey = unslab(publicKey)
  }

  // dialed by key (joinPeer) until every join is left
  get isJoined() {
    return this._joined > 0
  }

  join(now = Date.now()) {
    if (this._joined++ === 0) this._joinedAt = now
    this.refreshedAt = -1 // let refresh() re-process
  }

  leave() {
    if (this._joined > 0) this._joined--
  }

  shouldDial(now = Date.now()) {
    if (this.isBanned && !this._liftBan(now)) return false

    // peer is not announcing (joined peers are dialed by key)
    if (this.lastSeenAt === -1 && !this.isJoined) return false

    if (this.isDialing) return false

    if (this.isExpired(now)) return false

    return this.retryAt <= now
  }

  get topics() {
    return Array.from(this._topics.keys())
  }

  hasTopic(hex) {
    return this._topics.has(hex)
  }

  get isBanned() {
    return this.bannedAt > 0
  }

  setBanned(reason = 'banned', now = Date.now()) {
    this.bannedAt = now
    this.banReason = reason
  }

  // joined peers never expire, so their bans are lifted in place
  _liftBan(now) {
    if (!this.isJoined || now <= this.bannedAt + BAN_DURATION) return false
    this.bannedAt = -1
    this.banReason = null
    this._shortDisconnects = []
    return true
  }

  get expiresAt() {
    if (this.isBanned) {
      return this.bannedAt + BAN_DURATION
    }

    // joined peers stay until left, retryAt alone drives their schedule
    if (this.isJoined) return Infinity

    const lastActive = Math.max(this.lastSeenAt, this.disconnectedAt)

    return lastActive + EXPIRE_DURATION
  }

  isExpired(now = Date.now()) {
    if (this.connected) return false
    if (this.isJoined) return false

    // In the unlikely event of an upsert w/o seen()
    if (!this.isBanned && this.lastSeenAt === -1 && this.disconnectedAt === -1) {
      return Date.now() - this.#created > 10_000
    }

    return now > this.expiresAt
  }

  get connected() {
    return this._connectedCount > this._disconnectedCount
  }

  get retryAt() {
    if (!this._outboundAttempts) return this.isJoined ? this._joinedAt : this.lastSeenAt

    const backoff = Math.min(Peer.backoffDuration(this._outboundAttempts), DIAL_BACKOFF_MAX)

    return this.attemptedAt + backoff
  }

  get nextAt() {
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

  // Called when a dial or an inbound handshake actually begins.
  // Only outbound attempts move `attemptedAt`, so an inbound connection never
  // pushes our own retry schedule into the future.
  markAttempt(initiator, { canRelay = false, now = Date.now() } = {}) {
    this.isDialing = true

    if (initiator) {
      this.attemptedAt = now
      this._outboundAttempts++
    } else {
      this.inboundAt = now
      this._inboundAttempts++
    }

    let done = false
    return (failed = false, cause = null, now = Date.now()) => {
      if (done) return
      done = true
      this.isDialing = false
      if (initiator) this.attemptedAt = now

      if (!failed) {
        if (initiator) this._outboundSuccess++
        else this._inboundSuccess++
        return
      }

      cause = normalizeCause(cause)

      if (initiator) {
        this._outboundFailures++

        if (canRelay && !this.forceRelaying && shouldForceRelaying(cause)) {
          this.forceRelaying = true
          this._outboundAttempts = 0 // fastconnect to relay
          this.refreshedAt = -1 // reschedule
        }
      } else {
        this._inboundFailures++
      }
    }
  }

  resetAttempts() {
    this._outboundAttempts = 0
    this._inboundAttempts = 0
    this.refreshedAt = -1 // let refresh() re-process
  }

  markConnected(initiator, now = Date.now()) {
    this.connectedAt = now
    this._connectedCount++
  }

  markDisconnected(initiator, cause, now = Date.now()) {
    cause = normalizeCause(cause)

    this._disconnectedCount++
    const delta = now - this.connectedAt
    this.timeConnected += delta
    this.disconnectedAt = now
    if (initiator) this.lastSeenAt = now

    // connection survived long enough, reset attempts
    if (delta > DIAL_ATTEMPTS_RESET) this.resetAttempts()

    // we caused it (suspend, gc, dedupe, cap), never held against the remote
    if (LOCAL_CAUSES.has(cause)) return

    if (!initiator && delta < HAMMERING_THRESHOLD) {
      this._shortDisconnects.push(now)

      // only the last BAN_HAMMER_ATTEMPTS within the window can matter
      const recent = this._shortDisconnects
      while (recent.length > BAN_HAMMER_ATTEMPTS || recent[0] <= now - HAMMER_WINDOW) recent.shift()

      const reason = this.shouldBan(initiator, cause)
      if (reason) this.setBanned(reason, now)
    }
  }

  shouldBan(initiator, cause) {
    if (initiator) return null
    if (LOCAL_CAUSES.has(normalizeCause(cause))) return null

    // inbound peer repeatedly connecting and dropping within seconds
    if (this._shortDisconnects.length >= BAN_HAMMER_ATTEMPTS) return 'hammering'

    return null
  }

  static backoffDuration(nAttempts) {
    // 1s, 2.5s, 6s, 15s, 36s ...
    return 1000 * Math.exp(nAttempts * 0.9027)
  }
}

// @since v0.5: causes are compared as strings
function normalizeCause(cause) {
  if (!cause) return cause
  if (typeof cause === 'string') return cause
  return cause.code || cause.message || cause
}

function shouldForceRelaying(code) {
  return (
    code === 'HOLEPUNCH_ABORTED' ||
    code === 'HOLEPUNCH_DOUBLE_RANDOMIZED_NATS' ||
    code === 'REMOTE_NOT_HOLEPUNCHABLE'
  )
}

Peer.LOCAL_CAUSES = LOCAL_CAUSES

module.exports = Peer
