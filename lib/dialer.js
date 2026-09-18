const b4a = require('b4a')
const Peer = require('./peer')
// const ScopeLock = require('scope-lock')

const MAX_PARALLEL = 3
const ERR_FIREWALL = 'Peer is firewalled'

class Dialer {
  // cursors
  _scheduled = -1
  _processed = -1

  constructor(handlers, opts = {}) {
    const {
      ondial = async (peer) => {},
      ondialerror = (err) => { throw err },
      isConnected = (hex) => false,
      isSelf = (publicKey) => false
    } = handlers

    this._ondial = ondial
    this._ondialerror = ondialerror
    this._isConnected = isConnected
    this._isSelf = isSelf // TODO: necessary? / belongs to rep+peer.js

    const {
      firewall,
      suspended = false,
      maxParallel = MAX_PARALLEL,
      backoffs,
      jitter = 500
    } = opts
    if (backoffs) console.warn('deprecated backoffs', backoffs)

    this._firewall = firewall
    this.maxParallel = maxParallel
    this.jitter = jitter

    this.peers = new Map() // hex(peerId) -> Peer
    this.suspended = suspended
    this.destroyed = false

    this._slots = new Semaphore(MAX_PARALLEL)
    this._flushreqs = []

    if (!this.suspended) this._start()
  }

  lookup(publicKey, relayAddresses) {
    if (this._isSelf(publicKey)) return null

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
    if (!peer) return // ignore self

    peer.observe(topic, now)

    this.refresh(now)
  }

  _start() {
    this.refresh()
  }

  async flush(now = Date.now()) {
    if (!this.peers.size) return

    const peers = this._orderedPeers
    const target = peers[peers.length - 1]._nextAt

    if (this._flushreqs.length) {
      const {
        target: prevTarget,
        promise
      } = this._flushreqs[this._flushreqs.length - 1]

      if (target <= prevTarget) return promise
    }

    let done = null
    const promise = new Promise((resolve) => { done = resolve })

    this._flushreqs.push({ target, promise, done })

    return promise
  }

  // TODO: O(n)
  get _orderedPeers () {
    const peers = Array.from(this.peers.values())
    return peers.sort((a, b) => a._nextAt - b._nextAt)
  }

  async refresh(now = Date.now()) {
    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    if (this.suspended) return false
    if (!this.peers.size) return false  // queue empty

    // const past = sorted.filter(o => o._nextAt < now)
    // TODO: validate past
    // yes; revive past dials aborted due to suspend()
    //
    // const future = sorted.filter(o => now <= o._nextAt)

    const sorted = this._orderedPeers

    let pending = []

    for (const peer of sorted) {
      const key = toHex(peer.publicKey)

      // collect garbage
      if (peer.isExpired(now)) {
        this.peers.delete(key)
        continue
      }

      // do not fire future|cancellable work
      if (peer._nextAt > now) {
        break
      }

      // already connected
      if (this._isConnected(key)) continue

      // already queued
      // TODO: use cursor
      // if (this.timers.has(key)) continue

      // not eligable
      if (!peer.shouldDial(now)) continue

      pending.push(this._dial(peer, now))
    }

    this._reschedule() // auto-refresh

    return Promise.all(pending)
  }

  async _dial(peer, now = Date.now()) {
    await this._slots.acquire()

    if (this.jitter > 0) {
      await new Promise((resolve) => setTimeout(resolve, Math.random() * this.jitter))
    }

    const { _nextAt: nextAt } = peer

    const actionComplete = () => {
      this._slots.release()
      this._processed = Math.max(this._processed, nextAt)

      while (this._flushreqs.length) {
        if (this._flushreqs[0].target > this._processed) break

        const { done } = this._flushreqs.shift()
        done()
      }

      this._reschedule()
    }

    // still dialable
    if (!peer.shouldDial(now) || this._isConnected(peer.publicKey)) {
      actionComplete()
      return
    }

    this._ondial(peer)
      .catch(this._ondialerror)
      .then(actionComplete)
  }

  _reschedule (now = Date.now()) {
    if (this._refreshTimer) return // already scheduled

    const sorted = this._orderedPeers
    if (!sorted.length) return

    const peer = sorted.find(p => p._nextAt > now)
    if (!peer) return

    const { _nextAt: nextAt } = peer
    const delay = nextAt - now

    this._refreshTimer = setTimeout(() => {
      this.refresh().catch(this._ondialerror)
    }, delay)

    this._scheduled = nextAt
  }

  suspend() {
    if (this._refreshTimer) clearTimeout(this._refreshTimer)
    this._refreshTimer = null

    this.suspended = true
  }

  resume() {
    this.suspended = false
    return this.refresh()
  }

  destroy() {
    this.suspend()
  }
}

class Semaphore {
  constructor (permits = 3) {
    this.n = permits
    this.queue = []
  }

  acquire () {
    if (this.n > 0) {
      this.n--
      return Promise.resolve()
    }
    return new Promise(resolve => this.queue.push(resolve))
  }

  release () {
    const next = this.queue.shift()
    if (next) next()
    else this.n++
  }
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

module.exports = Dialer
