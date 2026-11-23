const Hypercore = require('hypercore')
const { getStreamError } = require('streamx')
const b4a = require('b4a')
const { toHex } = b4a

const CONNECTION_IDLE_TIMEOUT = 3 * 60 * 1000 // 3 Minutes

module.exports = class Connection {
  constructor(replicator, stream, peerId, initiator) {
    this.replicator = replicator
    this.initiator = initiator
    this.stream = stream
    this.peerId = peerId
    this._onclose = this._onclose.bind(this)
    this._onchannelopen = this._onchannelopen.bind(this)
    stream.on('close', this._onclose)

    this.bytesRead = -1
    this.bytesWritten = -1
    this.lastActivity = -1
    this.refresh()
    this._survived = 0 // incremented on gc

    this.info.markConnected(this.initiator)

    this._setReplicating = null
    this.replicating = new Promise((resolve) => {
      this._setReplicating = resolve
    })

    this.destroyed = new Promise((resolve) => {
      if (stream.destroyed) resolve()
      else stream.once('close', resolve)
    })
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
    // Prevent GC before replicating upto 6s
    if (this._setReplicating && now - this.connectedAt < 6000) return false

    if (this._protomuxSessions() === 0) return true

    // no traffic observed
    if (now - this.lastActivity > CONNECTION_IDLE_TIMEOUT) return true

    // all-channels closed on local & remote
    if (this.protomux.isIdle()) return true

    return false
  }

  _protomuxSessions() {
    const mux = this.protomux

    let opened = 0
    for (const channel of mux) {
      if (channel.protocol !== 'hypercore/alpha') continue
      // BUG! channel lingers in stream
      // see details in test/connection.js

      const topic = this.replicator._topicOf(channel.id)
      if (!topic) continue

      const session = this.replicator.sessions.get(toHex(topic))
      if (session && session.active) opened++
    }

    return opened
  }

  _onclose() {
    const rep = this.replicator

    const err = getStreamError(this.stream)

    const key = toHex(this.peerId)
    if (this.initiator) {
      rep.outbound.delete(key)
      rep.stats.connects.client.closed++
    } else {
      rep.inbound.delete(key)
      rep.stats.connects.server.closed++
    }

    if (this._setReplicating) {
      this._setReplicating() // settle
      this._setReplicating = null
    }

    this.info.markDisconnected(this.initiator, err)
    rep.dialer.reschedule(this.info)
    rep.dialer.refresh() // reschedule fast-reconnects

    rep.emit('disconnected', this)
    rep.emit('update')
  }

  _watchChannels() {
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

  refresh(now = Date.now()) {
    // secret-stream bytes written;
    // should exclude heartbeat
    if (
      this.bytesRead === this.stream.rawBytesRead &&
      this.bytesWritten === this.stream.rawBytesWritten
    ) {
      return
    }

    this.bytesRead = this.stream.rawBytesRead
    this.bytesWritten = this.stream.rawBytesWritten
    this.lastActivity = now
  }

  get destroying() {
    return this.stream.destroying
  }

  destroy(err) {
    this.stream.destroy(err)
  }
}
