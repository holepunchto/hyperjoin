const Hypercore = require('hypercore')
const { getStreamError } = require('streamx')
const b4a = require('b4a')
const { toHex } = b4a

module.exports = class Connection {
  constructor(replicator, stream, peerId, initiator) {
    this.replicator = replicator
    this.initiator = initiator
    this.stream = stream
    this.peerId = peerId
    this._onclose = this._onclose.bind(this)
    this._onchannelopen = this._onchannelopen.bind(this)
    stream.on('close', this._onclose)

    this._evicted = false
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
    if (this._setReplicating && now - this.connectedAt < 5000) return false

    // return !this.replicator.coredex.isActive(this)
    if (this._protomuxSessions() === 0) return true

    return false
  }

  // @deprecated - reference
  _protomuxSessions() {
    const mux = this.protomux

    let opened = 0
    console.log(this.replicator.debugInstanceId, '_protomuxSessions start()', 'mux.isIdle()', mux.isIdle())
    for (const ch of mux) {
      if (ch.protocol !== 'hypercore/alpha') continue

      // BUG! need to know if ch.opened on remote
      // all local known channels are eagerly opened
      // on corestore.replicate() and never closed
      // or marked as in-use.

      const dkeystr = toHex(ch.id)

      for (const session of this.replicator.sessions.values()) {
        if (!session.active) continue

        if (session.hasCore(dkeystr)) {
          console.log('_protomuxSessions', dkeystr.slice(0, 6), ch.opened, ch.closed, ch.drained)
          opened++
          break
        }
      }
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
    rep.dialer.refresh() // reschedule fast-reconnects

    rep.emit('disconnected', this)
    rep.emit('update')
  }

  _watchChannels() {
    this.protomux.watch({ protocol: 'hypercore/alpha' }, this._onchannelopen)
  }

  _onchannelopen(channel) {
    const channelId = toHex(channel.id)

    channel.fullyClosed().then(this._onchannelclose.bind(this, channelId))

    console.log(this.replicator.debugInstanceId, 'ch-open', channelId.slice(0, 6))
    this.replicator.coredex.opened(this, channelId)

    if (this._setReplicating) {
      this._setReplicating()
      this._setReplicating = null
    }
  }

  _onchannelclose(channelId) {
    this.replicator.coredex.closed(this, channelId)
    // console.log('channel-closed', discoveryKey)
  }

  refresh(now = Date.now()) {
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

  destroy(err) {
    this.stream.destroy(err)
  }
}
