const Discovery = require('replicator-discovery')

module.exports = class Scanner {
  constructor(
    dht,
    store,
    topic,
    {
      limit = 1,
      delay = 0,
      timeout = 60000,
      updatedTimeout = 10000,
      keyPair,
      suspended = false,
      onpeer = noop,
      onlookup = noop,
      onerror = noop
    } = {}
  ) {
    this.dht = dht
    this.store = store
    this.topic = topic

    this._delay = delay
    this._timeout = timeout
    this._updatedTimeout = updatedTimeout
    this._onerror = onerror
    this._onstoreclose = this.destroy.bind(this)

    this.started = false
    this.destroyed = false

    this.suspended = suspended // TODO; support suspension

    this.discovery = new Discovery(dht, topic, {
      announce: false,
      limit,
      suspended,
      keyPair,
      onpeer,
      onstats: onlookup,
      onerror
    })

    this.complete = new Promise((resolve) => {
      this._resolve = resolve
    })
  }

  start() {
    if (this.started) return this.complete
    this.started = true

    this.store.once('close', this._onstoreclose)

    this._setTimeout(this._timeout)

    let delay = 0

    if (this._delay > 0) {
      delay = approximateFuture(this._delay)
    }

    if (delay > 0) this._delayTimer = setTimeout(() => this._run(), delay)
    else this._run()

    return this.complete
  }

  expire() {
    this._setTimeout(this._updatedTimeout)
  }

  // lunte-disable-next-line require-await
  async flush() {
    return this.discovery.flush()
  }

  _setTimeout(timeout) {
    if (this.destroyed) return

    if (this._timeoutTimer) clearTimeout(this._timeoutTimer)
    this._timeoutTimer = null

    if (timeout > 0) {
      this._timeoutTimer = setTimeout(() => this.destroy(), timeout)
    }
  }

  async _run() {
    this._delayTimer = null
    if (this.destroyed) return

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

    this.store.removeListener('close', this._onstoreclose)

    if (this._delayTimer) clearTimeout(this._delayTimer)
    if (this._timeoutTimer) clearTimeout(this._timeoutTimer)

    this.discovery.destroy().catch(this._onerror)
    this._resolve()
  }
}

function approximateFuture(delay) {
  const jitter = Math.floor(Math.random() * 0.5 * delay)
  return delay + jitter
}

function noop() {}
