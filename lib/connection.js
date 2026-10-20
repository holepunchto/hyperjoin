const Hypercore = require('hypercore')
const { getStreamError } = require('streamx')
const { toHex } = require('b4a')

const REPLICATION_GRACE = 6000 // let a new connection start replicating before judging it

module.exports = class Connection {
  constructor(replicator, stream, peerId, initiator) {
    const { promise, resolve } = Promise.withResolvers()

    this.replicator = replicator
    this.initiator = initiator
    this.stream = stream
    this.peerId = peerId
    this.evicted = false // set by the replicator gc
    this.lastActivity = Date.now()
    this.survived = 0 // gc rounds survived, incremented by the replicator
    this.replicating = promise

    this._setReplicating = resolve
    this._onclose = this._onclose.bind(this)
    this._onchannelopen = this._onchannelopen.bind(this)
    this._ondata = this._ondata.bind(this)

    stream.on('close', this._onclose)

    // Activity is measured on decrypted messages. secret-stream drops the
    // empty keep-alive frames before emitting 'data', so unlike the raw byte
    // counters this only moves when a protocol actually sends something.
    stream.on('data', this._ondata)

    this.info.markConnected(this.initiator)
  }

  get protomux() {
    return Hypercore.getProtocolMuxer(this.stream)
  }

  get info() {
    return this.replicator.dialer.lookup(this.peerId)
  }

  get connectedAt() {
    return this.info.connectedAt
  }

  isIdle(now = Date.now()) {
    // Prevent GC before replicating
    if (now - this.connectedAt < REPLICATION_GRACE) return false

    // no protocol traffic observed
    if (now - this.lastActivity > this.replicator.connectionIdleTimeout) return true

    // all-channels closed on local & remote
    if (this.protomux.isIdle()) return true

    return false
  }

  _ondata() {
    this.lastActivity = Date.now()
  }

  _onclose() {
    const rep = this.replicator

    const err = getStreamError(this.stream)
    this.stream.removeListener('data', this._ondata)

    const key = toHex(this.peerId)
    if (this.initiator) {
      if (rep.outbound.get(key) === this) rep.outbound.delete(key)
      rep.stats.connects.client.closed++
    } else {
      if (rep.inbound.get(key) === this) rep.inbound.delete(key)
      rep.stats.connects.server.closed++
    }

    if (this._setReplicating) {
      this._setReplicating() // settle
      this._setReplicating = null
    }

    this.info.markDisconnected(this.initiator, err)
    rep.dialer.refresh() // reschedule fast-reconnects

    rep.emit('disconnected', this)
    rep.emit('update')
  }

  watchChannels() {
    this.protomux.watch({ protocol: 'hypercore/alpha' }, this._onchannelopen)
  }

  _onchannelopen() {
    try {
      if (this._setReplicating) {
        this._setReplicating()
        this._setReplicating = null
      }

      this.protomux.unwatch({ protocol: 'hypercore/alpha' }, this._onchannelopen)
    } catch (err) {
      // noop
    }
  }

  destroy(err) {
    this.stream.destroy(err)
  }
}
