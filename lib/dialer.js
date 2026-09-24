const { toHex } = require('b4a')
const Peer = require('./peer')

const MAX_PARALLEL = 3

class Dialer {
  constructor(handlers, opts = {}) {
    const {
      ondial = async (peer) => {},
      isConnected = (hex) => false,
      ondialerror = (err) => {
        throw err
      }
    } = handlers

    this._ondial = ondial
    this._ondialerror = ondialerror
    this._isConnected = isConnected

    const { suspended = false, maxParallel = MAX_PARALLEL, backoffs, jitter = 500 } = opts

    if (backoffs) console.warn('deprecated backoffs', backoffs)

    this._activeTopics = new Set()

    this.jitter = jitter

    this.peers = new Map() // hex(peerId) -> Peer
    this.suspended = suspended
    this.destroyed = false

    this._slots = new Semaphore(maxParallel)

    // cursors
    this._scheduled = -1
    this._processed = -1
    this._flushreqs = []
    this._inflightWork = new Set()

    if (!this.suspended) this._start()
  }

  get inflight() {
    return this._inflightWork.size
  }

  // TODO: O(1)
  get _orderedPeers() {
    const peers = Array.from(this.peers.values())
    return peers.sort((a, b) => a._nextAt - b._nextAt)
  }

  lookup(publicKey, relayAddresses) {
    const key = toHex(publicKey)

    let peer = this.peers.get(key)
    if (!peer) {
      peer = new Peer(publicKey)
      this.peers.set(key, peer)
    }

    // new is always better
    if (relayAddresses) peer.updateRelays(relayAddresses)

    return peer
  }

  seen(dhtPeer, topic, now = Date.now()) {
    const { publicKey, relayAddresses } = dhtPeer
    const peer = this.lookup(publicKey, relayAddresses)

    peer.observe(topic, now)
    this.refresh(now)
  }

  _start() {
    this.refresh()
  }

  async flush(now = Date.now()) {
    const inflight = Promise.all(this._inflightWork)

    let target = -1

    // TODO: O(1)
    for (const peer of this.peers.values()) {
      if (!this._isEligable(peer, now)) continue
      if (peer.retryAt > target) target = peer.retryAt
    }

    if (target === -1) return inflight

    if (this._flushreqs.length) {
      const { target: prevTarget, promise } = this._flushreqs[this._flushreqs.length - 1]

      // de-dupe prior work
      if (target <= prevTarget) return promise
    }

    if (target <= this._processed) return inflight // no work, no-op

    // create snapshot of known work
    let done = null
    const promise = new Promise((resolve) => {
      done = resolve
    }).then(() => inflight)

    this._flushreqs.push({ target, promise, done })

    return promise
  }

  refresh(now = Date.now()) {
    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    if (this.suspended) return false
    if (!this.peers.size) return false // queue empty

    const sorted = this._orderedPeers

    for (const peer of sorted) {
      const key = toHex(peer.publicKey)

      // collect garbage
      if (peer.isExpired(now)) {
        this.peers.delete(key)
        continue
      }

      // Anything past this point must be considered flushed upon completion.

      // TODO: this.inflight unbounded;
      // Early aborting here would be a design flaw.
      // if (this._slots.isFull) break

      // do not schedule future|cancellable work
      if (peer._nextAt > now) {
        break
      }

      // not eligable
      if (!this._isEligable(peer, now)) continue

      // already queued
      const previousVisit = peer._refreshedAt
      peer._refreshedAt = now
      if (previousVisit >= peer._nextAt) continue

      // dial!
      const attempt = this._dial(peer, now)
        .catch(this._ondialerror)
        .finally(() => this._inflightWork.delete(attempt))

      this._inflightWork.add(attempt)
    }

    this._reschedule() // auto-refresh
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

    const { _nextAt: nextAt } = peer

    const actionComplete = () => {
      this._processed = Math.max(this._processed, nextAt)

      while (this._flushreqs.length) {
        if (this._flushreqs[0].target > this._processed) break

        // settle pending flushes
        const { done } = this._flushreqs.shift()
        done()
      }

      // TODO: ensure released on suspend
      this._slots.release()
      this._reschedule()
    }

    const postWait = now + (Date.now() - waitStart)

    if (!this._isEligable(peer, postWait)) {
      peer._refreshedAt = -1 // let refresh() re-process
      actionComplete()
      return
    }

    await this._ondial(peer, postWait).catch(this._ondialerror)

    actionComplete()
  }

  _reschedule(now = Date.now()) {
    if (this.suspended) return
    if (this._refreshTimer) return // already scheduled

    const sorted = this._orderedPeers
    if (!sorted.length) return

    const peer = sorted.find((p) => p._nextAt > now)
    if (!peer) return

    const nextAt = peer._nextAt
    const delay = nextAt - now

    this._refreshTimer = setTimeout(() => this.refresh(), delay)

    this._scheduled = nextAt
  }

  suspend() {
    this.suspended = true
    const suspended = this.flush()

    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    // TODO: abort all owned inflight, fast-settle suspended

    return suspended
  }

  resume(now = Date.now()) {
    this.suspended = false
    this.refresh(now)
  }

  topicEnable(topic) {
    if (typeof topic !== 'string') topic = toHex(topic)
    this._activeTopics.add(topic)
  }

  topicDisable(topic) {
    if (typeof topic !== 'string') topic = toHex(topic)
    this._activeTopics.delete(topic)
  }

  // TODO: redesign
  #activeBans = new Set()
  checkBans() {
    const banned = []
    const unbanned = []
    for(const [key, peer] of this.peers) {
      if (peer.isBanned && !this.#activeBans.has(key)) {
        this.#activeBans.add(key)
        banned.push(peer)
      }
    }

    for (const key of this.#activeBans) {
      if (!this.peers.get(key)?.isBanned) {
        this.#activeBans.delete(key)
        unbanned.push(this.peers.get(key))
      }
    }

    return { banned, unbanned }
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
