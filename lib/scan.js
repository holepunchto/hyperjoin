const Discovery = require('replicator-discovery')

module.exports = class Scanner {
  constructor(
    dht,
    topic,
    {
      limit = 1,
      delay = 0,
      timeout = 60000,
      ready,
      getKeyPair,
      suspended = false,
      onpeer = noop,
      onconnected = noop,
      onlookup = noop,
      onerror = noop
    } = {}
  ) {
    this.dht = dht
    this.topic = topic

    this._delay = delay
    this._timeout = timeout
    this._ready = ready
    this._getKeyPair = getKeyPair
    this._onerror = onerror
    this._onconnected = onconnected

    this.startedAt = -1
    this.expiresAt = -1

    this.started = false
    this.destroyed = false

    this.suspended = suspended

    this.discovery = null
    this._opts = { limit, onpeer, onlookup }

    this.complete = new Promise((resolve) => {
      this._resolve = resolve
    })
  }

  start() {
    if (this.started) return this.complete
    this.started = true

    this._starting = this._start()

    return this.complete
  }

  async _start() {
    try {
      await this._ready
      if (this.destroyed) return

      const { limit, onpeer, onlookup } = this._opts
      this.discovery = new Discovery(this.dht, this.topic, {
        announce: false,
        limit,
        suspended: this.suspended,
        keyPair: this._getKeyPair(),
        onpeer,
        onstats: onlookup,
        onerror: this._onerror
      })

      this._setTimeout(this._timeout)

      let delay = 0

      if (this._delay > 0) {
        delay = approximateFuture(this._delay)
      }

      if (delay > 0) this._delayTimer = setTimeout(() => this._run(), delay)
      else this._run()
    } catch (err) {
      this._onerror(err)
      this.destroy()
    }
  }

  expire(timeout) {
    this._setTimeout(timeout)
  }

  // lunte-disable-next-line require-await
  async flush() {
    if (this._starting) await this._starting
    if (!this.discovery) return
    return this.discovery.flush()
  }

  async suspend({ log = noop } = {}) {
    if (this.suspended || this.destroyed) return
    this.suspended = true
    if (this._starting) await this._starting
    if (!this.discovery) return
    await this.discovery.suspend({ log })
  }

  resume() {
    if (!this.suspended || this.destroyed) return
    this.suspended = false
    if (!this.discovery) return
    this.discovery.resume()
  }

  connected(connection) {
    if (this.destroyed) return

    try {
      this._onconnected(this, connection)
    } catch (err) {
      this._onerror(err)
    }
  }

  _setTimeout(timeout) {
    if (this.destroyed) return

    if (this._timeoutTimer) clearTimeout(this._timeoutTimer)
    this._timeoutTimer = null

    this.startedAt = Date.now()
    this.expiresAt = Date.now() + timeout

    if (timeout > 0) {
      this._timeoutTimer = setTimeout(() => this.destroy(), timeout)
    } // TODO: else destroy now
  }

  async _run() {
    this._delayTimer = null
    if (this.destroyed) return
    if (!this.discovery) return

    try {
      this.discovery.refresh()
      await this.discovery.flush()
    } catch (err) {
      this._onerror(err)
      this.destroy()
    }
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    if (this._delayTimer) clearTimeout(this._delayTimer)
    if (this._timeoutTimer) clearTimeout(this._timeoutTimer)

    if (this.discovery) this.discovery.destroy().catch(this._onerror)
    this._resolve()
  }
}

function approximateFuture(delay) {
  const jitter = Math.floor(Math.random() * 0.5 * delay)
  return delay + jitter
}

function noop() {}
