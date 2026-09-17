const b4a = require('b4a')
const Peer = require('./peer')
const spq = null // require('shuffled-priority-queue')
const ScopeLock = require('scope-lock')

const MAX_PARALLEL = 3
const ERR_FIREWALL = 'Peer is firewalled'

class Dialer {
  constructor(replicator, opts = {}) {
    this.replicator = replicator

    const { firewall, suspended = false, maxParallel = MAX_PARALLEL, jitter = 500 } = opts
    this._firewall = firewall
    this.maxParallel = maxParallel
    this.jitter = jitter

    this.peers = new Map() // hex(peerId) -> Peer
    this.suspended = suspended
    this.destroyed = false

    this.timers = new Map() // hex -> Handle
    this._flushes = []

    this._drainLock = new ScopeLock({ debounce: true })

    this._running = false
    if (!this.suspended) this._start()
  }

  // update-insert operation
  lookup(publicKey, relayAddresses) {
    if (b4a.equals(publicKey, this.replicator.peerId)) return null

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

  async refresh(now = Date.now()) {
    if (!this._running) return false

    // TODO: O(n)
    const sorted = Array.from(this.peers.values())
      .map(peer => ({
        nextAt: Math.min(peer.retryAt, peer.expiresAt),
        peer
      }))

    sorted.sort((a, b) => a.nextAt - b.nextAt)


    // const past = sorted.filter(o => o.nextAt < now)
    // TODO: validate past
    // yes; revive past dials aborted due to suspend()

    const future = sorted.filter(o => now <= o.nextAt)
    let flushAt = now

    for (const { nextAt, peer } of future) {
      const delta = nextAt - now
      const key = toHex(peer.publicKey)

      // Garbage collect
      if (peer.isExpired(now)) {
        this.peers.delete(key)
        continue
      }

      // already queued
      if (this.timers.has(key)) continue

      // already connected
      if (this.replicator.hasConnection(key)) continue

      // not eligable
      if (!peer.shouldDial(now)) continue // todo: !inFlight

      const fireIn = delta + Math.random() * this.jitter
      const fireAt = now + delta
      flushAt = Math.max(flushAt, fireAt)

      const timer = setTimeout(this._fire.bind(this, fireAt, peer, 'dial'), fireIn)
      this.timers.set(key, timer)
    }

    return new Promise((resolve) => {
      // TODO: ordered insert
      this._flushes.push({ flushAt, resolve })
    })
  }

  _start() {
    this._running = true
    this.refresh()
  }

  _fire(at, peer, action, now = Date.now()) {
    if (action !== 'dial') throw new Error('todo')


    const actionComplete = () => {
      this.timers.delete(toHex(peer.peerId))

      //TODO: ordered insert
      this._flushes.sort((a, b) => a.flushAt - b.flushAt)

      while (this._flushes.length) {
        if (this._flushes[0].flushAt <= at) {
          this._flushes[0].resolve(true)
          this._flushes.shift()
        } else {
          break
        }
      }

      this.refresh()
    }

    // TODO: bound outgoing MAX_PARALLEL
    if (!peer.shouldDial(now)) {
      actionComplete()
      return
    }

    this.replicator
      ._connect(peer)
      .catch((err) => this.replicator.emit('error', err))
      .then(actionComplete)
  }

  suspend() {
    for (const handle of this.timers.values()) {
      clearTimeout(handle)
    }
    this.timers.clear()
    this._running = false

    while (this._flushes.length) {
      this._flushes.shift().resolve(false)
    }
  }

  resume() {
    this._running = true
    this.refresh()
  }

  destroy() {
    this.suspend()
  }
}

function toHex(buffer) {
  return b4a.toString(buffer, 'hex')
}

module.exports = Dialer
