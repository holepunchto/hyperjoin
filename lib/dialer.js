const { toHex } = require('b4a')
const Peer = require('./peer')
const OrderedSet = require('./ordered-set')

const MAX_PARALLEL = 3
const MIN_REFRESH_INTERVAL = 1000

class Queue {
  constructor(isEligable) {
    this.peers = new OrderedSet((peer) => peer._nextAt)

    this._isEligable = isEligable

    // cursors
    this.processed = -1
    this.flushreqs = []
  }

  async flush(now) {
    let target = -1

    // TODO: O(1)
    for (const peer of this.peers) {
      if (!this._isEligable(peer, now)) continue

      if (peer.retryAt > target) target = peer.retryAt
    }

    if (target === -1) return

    if (this.flushreqs.length) {
      const { target: prevTarget, promise } = this.flushreqs[this.flushreqs.length - 1]

      // de-dupe prior work
      if (target <= prevTarget) return promise
    }

    if (target <= this.processed) return // no work, no-op

    // create snapshot of known work
    let done = null
    const promise = new Promise((resolve) => {
      done = resolve
    })

    this.flushreqs.push({ target, promise, done })

    return promise
  }

  settle(upto) {
    this.processed = Math.max(this.processed, upto)

    while (this.flushreqs.length) {
      const { target } = this.flushreqs[0]
      if (target > this.processed) break

      // defer to last
      if (target === this.processed) {
        let n = 0

        for (const peer of this.peers) {
          if (!this._isEligable(peer, this.processed)) continue

          if (peer.retryAt === this.processed) n++
        }

        if (n) return
      }

      // settle pending flush
      const { done } = this.flushreqs.shift()
      done()
    }
  }
}

class Dialer {
  constructor(handlers, opts = {}) {
    const {
      ondial = async (peer) => {},
      isConnected = (hex) => false,
      want = () => Infinity,
      canRelay = () => false,
      onban = (peer) => {},
      ondialerror = (err) => {
        throw err
      }
    } = handlers

    this._ondial = ondial
    this._ondialerror = ondialerror
    this._isConnected = isConnected
    this._canRelay = canRelay
    this._onban = onban
    this._want = want

    const { suspended = false, maxParallel = MAX_PARALLEL, jitter = 500 } = opts

    this._activeTopics = new Set()

    this.jitter = jitter

    this.peers = new Map() // hex(peerId) -> Peer
    this._isEligable = this._isEligable.bind(this)

    this._normal = new Queue(this._isEligable)
    this.suspended = suspended
    this.destroyed = false

    this._slots = new Semaphore(maxParallel)

    // cursors
    this._scheduled = -1
    this._inflightWork = new Set()
    this._externallyThrottled = false

    if (!this.suspended) this._start()
  }

  get inflight() {
    return this._inflightWork.size
  }

  reschedule(peer) {
    if (!this._normal.peers.has(peer)) return

    this._normal.peers.add(peer)

    if (!peer.isBanned || peer._banReported) return

    peer._banReported = true
    this._onban(peer)
  }

  lookup(publicKey, relayAddresses) {
    const key = toHex(publicKey)

    let peer = this.peers.get(key)
    if (!peer) {
      peer = new Peer(publicKey)
      this.peers.set(key, peer)
      this._normal.peers.add(peer)
    }

    // new is always better
    if (relayAddresses && !peer.isBanned) peer.updateRelays(relayAddresses)

    return peer
  }

  seen(dhtPeer, topic, now = Date.now()) {
    const { publicKey, relayAddresses } = dhtPeer
    const peer = this.lookup(publicKey, relayAddresses)

    peer.observe(topic, now)
    this.reschedule(peer)
    this.refresh(now)

    return peer
  }

  _start() {
    this.refresh()
  }

  flush(now = Date.now()) {
    const inflight = Promise.all(this._inflightWork)

    // dialer is externally held; resolve when inflights complete
    if (this._externallyThrottled) return inflight

    return Promise.all([inflight, this._normal.flush(now)])
  }

  refresh(now = Date.now()) {
    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    if (this.suspended) return false
    if (!this.peers.size) return false // queue empty

    // future|cancellable work is not scheduled
    const due = this._normal.peers.upTo(now)
    const pending = []

    for (const peer of due) {
      // collect garbage
      if (peer.isExpired(now)) {
        this.peers.delete(toHex(peer.publicKey))
        this._normal.peers.delete(peer)
        continue
      }

      pending.push(peer)
    }

    const wanted = this._want()

    for (const peer of pending) {
      // max inflight reached
      if (this._slots.isFull) {
        break
      }

      // max capacity achieved
      if (wanted - this._inflightWork.size < 1) {
        this._externallyThrottled = true

        // dump all flush requests
        const inflight = Promise.all(this._inflightWork)
        while (this._normal.flushreqs.length) {
          this._normal.flushreqs.shift().done(inflight)
        }

        break
      }
      this._externallyThrottled = false

      // not eligable
      if (!this._isEligable(peer, now)) continue

      // already queued
      const previousVisit = peer._refreshedAt
      peer._refreshedAt = now
      if (previousVisit >= peer._nextAt) continue

      // dial!
      const startedAt = Date.now()
      const attempt = this._dial(peer, now)
        .catch(this._ondialerror)
        .finally(() => {
          this._inflightWork.delete(attempt)
          this.refresh(now + (Date.now() - startedAt))
        })

      this._inflightWork.add(attempt)
    }

    this._scheduleRefresh(now) // auto-refresh
  }

  _isEligable(peer, now) {
    if (this.suspended) return false

    // already connected
    if (this._isConnected(peer.publicKey)) return false

    if (!this._hasActiveTopic(peer)) return false

    return peer.shouldDial(now)
  }

  _hasActiveTopic(peer) {
    for (const hex of peer._topics.keys()) {
      if (this._activeTopics.has(hex)) return true
    }

    return false
  }

  async _dial(peer, now = Date.now()) {
    const waitStart = Date.now()
    await this._slots.acquire() // MAX_PARALLEL

    if (this.jitter > 0) {
      await new Promise((resolve) => setTimeout(resolve, Math.random() * this.jitter))
    }

    let postWait = now + (Date.now() - waitStart)

    const { _nextAt: nextAt } = peer

    const actionComplete = () => {
      this._normal.settle(nextAt)
      this._slots.release()
    }

    if (!this._isEligable(peer, postWait) || this._want() < 1) {
      peer._refreshedAt = -1 // let refresh() re-process
      actionComplete()
      return
    }

    const attemptDone = peer.markAttempt(true, { canRelay: this._canRelay() })
    this.reschedule(peer)

    try {
      const dialFailed = await this._ondial(peer, postWait)
      postWait = now + (Date.now() - waitStart)
      attemptDone(!!dialFailed, dialFailed, postWait)
      this.reschedule(peer)
    } catch (err) {
      attemptDone(true)
      this.reschedule(peer)
      this._ondialerror(err)
    }

    postWait = now + (Date.now() - waitStart)

    actionComplete()
  }

  _scheduleRefresh(now = Date.now()) {
    if (this.suspended) return
    if (this._refreshTimer) return // already scheduled

    // earliest peer that is not yet due
    const peer = this._normal.peers.after(now)
    if (!peer) return

    const nextAt = peer._nextAt
    const delay = Math.min(nextAt - now, MIN_REFRESH_INTERVAL)

    this._refreshTimer = setTimeout(() => this.refresh(), delay)

    this._scheduled = nextAt
  }

  suspend() {
    this.suspended = true
    const suspended = this.flush()

    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    return suspended
  }

  resume(now = Date.now()) {
    this.suspended = false
    this.refresh(now)
  }

  resetBackoff(since = 0) {
    for (const peer of this.peers.values()) {
      if (Math.max(peer.attemptedAt, peer.disconnectedAt) < since) continue

      peer.resetAttempts()
      this.reschedule(peer)
    }
    this.refresh()
  }

  topicEnable(topic) {
    if (typeof topic !== 'string') topic = toHex(topic)
    this._activeTopics.add(topic)
  }

  topicDisable(topic) {
    if (typeof topic !== 'string') topic = toHex(topic)
    this._activeTopics.delete(topic)
  }

  destroy() {
    this.suspend()
  }
}

class Semaphore {
  constructor(permits = 3) {
    this.n = permits
    this.queue = []
  }

  get isFull() {
    return this.queue.length >= this.n
  }

  acquire() {
    if (this.n > 0) {
      this.n--
      return Promise.resolve()
    }
    return new Promise((resolve) => this.queue.push(resolve))
  }

  release() {
    const next = this.queue.shift()
    if (next) next()
    else this.n++
  }
}

module.exports = Dialer
