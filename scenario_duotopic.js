const Replicator = require('.')
const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const createTestnet = require('hyperdht/testnet')
const TMP = require('test-tmp')
const Corestore = require('corestore')
const Autobase = require('autobase')
const ScopeLock = require('scope-lock')

const randomBytes = require(globalThis.Bare ? 'bare-crypto' : 'node:crypto').randomBytes

const LOG_HUMAN = false

const N_GROUPS = 6
const N_PEERS = 3
const TARGET_VALUE = 50

const GLOBAL_TOPIC = Buffer.alloc(32).fill(0)
GLOBAL_TOPIC.set(Buffer.from('common topic'))
const peerids = new Map() // 'hex' -> number

async function main() {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const [path, rmdir] = await tmpdir()

  const peers = []

  let baseKey = null

  for (let group = 0; group < N_GROUPS; group++) {
    for (let i = 0; i < N_PEERS /* per group */; i++) {
      const peer = new Peer(path, bootstrap, baseKey, group)

      if (!baseKey) {
        await peer.ready()
        baseKey = peer.counter.key
      }

      peers.push(peer)
    }
  }
  await Promise.all(peers.map((p) => p.ready()))

  for (const peer of peers) {
    peerids.set(toHex(peer.replicator.peerId), peer.id)
  }

  // yolo propagate
  const allWriters = peers.slice(1).map((p) => p.counter.writerKey)
  for (const key of allWriters) {
    await peers[0].counter.addWriter(key)
  }

  // run scenario
  await Promise.all(peers.map((p) => p.run()))

  // teardown
  await Promise.all(peers.map((p) => p.destroy()))
  await testnet.destroy()
  await rmdir()
  console.log('teardown complete')
}

main()

let _peerctr = 0

class Peer extends ReadyResource {
  constructor(prefix, bootstrap, remoteKey = null, group = null) {
    super()
    this._detailed = true
    this.id = _peerctr++
    this.group = group

    const path = `${prefix}/${this.id}`
    this.root = new Corestore(path)

    this.store = this.root.namespace('counter')
    this.counter = new CounterCore(this.store, remoteKey, this._onremoteupdate.bind(this))

    this.dht = new DHT({ bootstrap })

    this.replicator = new Replicator(this.root, {
      dht: this.dht,
      manual: true,
      slots: 1,
      minPeers: 0,
      maxPeers: 3,
      gcIdleTime: 5 * 1000,
      minActiveTime: 5 * 1000
      /*
      defaultDiscoveryTimeout: 15 * 1000,
      updatedDiscoveryTimeout: 3 * 1000,
      discoveryStartupDelay: 0,
      autoStartupDelay: 1000
      */
    })

    this._globSess = null

    this._dummyTopic = randomBytes(32) // should not connect
    this._dummyStore = this.root.namespace('dummy')
    this._dummySess = null

    const lookupId = (pubkey) => peerids.get(toHex(pubkey))

    this.replicator.on('connected', (connection) =>
      this.log('connected', { remote: lookupId(connection.peerId) })
    )
    this.replicator.on('disconnected', (connection) =>
      this.log('disconnected', { remote: lookupId(connection.peerId) })
    )

    this.replicator.on('session-state', this._logSessionUpdate.bind(this))

    const refreshInterval = 1000
    // manual refresh/gc
    setTimeout(() => {
      this._refresher = setInterval(async () => {
        if (this.replicator.closed) return
        await this.replicator.refresh()
        this.log('auto-refresh')
      }, refreshInterval)
      this._refresher.unref()

      this.replicator.on('close', () => clearInterval(this._refresher))
    }, refreshInterval * Math.random() /* startup jitter */)
  }

  log(event, payload = {}) {
    if (LOG_HUMAN) {
      // readable
      console.log('id', this.id, 'group', this.group, 'event', event, 'view', this.counter.last)
    } else {
      // renderable
      const writer = toHex(this.counter.writerKey).slice(0, 4)
      console.log(
        JSON.stringify({
          event,
          id: this.id,
          group: this.group,
          ...payload,
          now: Date.now(),
          writer,
          inbound: this.replicator.inbound.size,
          outbound: this.replicator.outbound.size,
          inflight: this.replicator._inflight.size,
          connections: this.replicator.connectionCount
        })
      )
    }
  }

  _logSessionUpdate(session) {
    if (session === this._dummySess) return // ignore dummy events

    this.log('session-updated', {
      topic: toHex(session.topic).slice(0, 4),
      state: session.state,
      seen: session.seen
    })
  }

  async _open() {
    // initialize namespace & writer
    await this.counter.ready()
    await this.replicator.ready()

    /** one store, two topics
     * Each peer pin own group.
     * When new update detect on grouptopic
     * Then seed global topic.
     */

    // pinned group (ex. own devices)
    if (this.group !== null) {
      const topic = Buffer.alloc(32).fill(this.group)
      this._groupSess = this.replicator.join(this.store, topic)
      this._groupSess.pin()
      this._groupSess.on('bump', () => {
        this._logSessionUpdate(this._groupSess)
        // TODO:
        // if (this._globSess.seen < value) this._seedGlobal()
      })
    }

    this._joinGlobal() // re-joined after evict + bump detection
    this._joinDummy() // required to pressure eviction policies

    this.log('opened')
  }

  _joinGlobal(discovery = false) {
    if (this._globSess) return // exists

    this._globSess = this.replicator.join(this.store, GLOBAL_TOPIC)
    this._globSess.on('bump', () => this._logSessionUpdate(this._globSess))
    // on evict
    this._globSess.on('close', () => {
      this._globSess = null

      // rejoin in discovery mode
      setTimeout(() => this._joinGlobal(true), 1000 + Math.random() * 3000)
    })

    if (discovery) {
      this._globSess.discover()
    }
  }

  _joinDummy() {
    if (this._dummySess) return // exists
    this._dummySess = this.replicator.join(this._dummyStore, this._dummyTopic)
    this._dummySess.on('close', () => {
      this._dummySess = null
      this._joinDummy() // auto-rejoin, just keep it in pool
    })
  }

  async run() {
    while ((await this.counter.value()) < TARGET_VALUE) {
      const chosen = !(this.id % 5)

      if (chosen && this.counter.writable) {
        await this.counter.bump()
        this.log('written')
      }

      await this.replicator.refresh()

      const delay = 5000 + Math.random() * 15000
      await new Promise((resolve) => setTimeout(resolve, delay))
    }

    this.log('complete')
  }

  _onremoteupdate(value) {
    this.log('update', { value })

    for (const session of this.replicator.sessions.values()) {
      if (session === this._dummySess) continue // skip

      session.bump(value)
    }

    // Rejoin common global on bump
    this._joinGlobal()
  }

  async _close() {
    await this.root.close()
    await this.replicator.destroy()
    await this.dht.destroy()
    this.log('closed')
  }

  destroy() {
    return this.close()
  }
}

class CounterCore {
  constructor(store, key = null, onvalue) {
    this._onvalue = onvalue
    this.last = -1
    this.store = store
    this.base = new Autobase(store, key, {
      open,
      apply,
      valueEncoding: 'json'
    })

    function open(store) {
      return store.get('value-view', { valueEncoding: 'json' })
    }

    async function apply(nodes, view, host) {
      for (const node of nodes) {
        const { type } = node.value

        switch (type) {
          case 'value':
            await view.append(node.value.value)
            break
          case 'writer':
            await host.addWriter(fromHex(node.value.key), { isIndexer: false })
            break
        }
      }
    }

    this._lock = new ScopeLock({ debounce: true })
    this.base.on('update', this._onupdate.bind(this))
  }

  async bump(value = null) {
    if (value === null) {
      const current = await this.value()
      value = current + 1
    }
    await this.base.append({ type: 'value', value })
    await this.base.update()
    this.last = value
    return value
  }

  async addWriter(writerKey) {
    await this.base.append({ type: 'writer', key: toHex(writerKey) })
    await this.base.update()
  }

  async update() {
    await this.base.update()
  }

  async value() {
    if (!this.base.view || this.base.view.length === 0) {
      return -1
    }

    const value = await this.base.view.get(this.base.view.length - 1)
    return value
  }

  async untilWritable() {
    if (this.base.writable) return

    return new Promise((resolve, reject) => {
      const { base } = this

      base.once('update', onUpdate)
      base.once('writable', onUpdate)
      base.once('error', onError)

      onUpdate()

      function onUpdate() {
        if (!base.writable) return
        resolve()
      }

      function onError(err) {
        reject(err)
      }
    })
  }

  async _onupdate() {
    const locked = await this._lock.lock()
    if (!locked) return

    this.last = await this.value()
    this._lock.unlock()

    this._onvalue(this.last)
  }

  get key() {
    return this.base.key
  }

  get discoveryKey() {
    return this.base.discoveryKey
  }

  get writerKey() {
    return this.base.local.key
  }

  get writable() {
    return this.base.writable
  }

  async ready() {
    await this.base.ready()
  }

  async close() {
    await this.base.close()
  }
}

function toHex(b) {
  if (!Buffer.isBuffer(b)) throw new Error('buffer')
  return b.toString('hex')
}

function fromHex(str) {
  if (typeof str !== 'string') throw new Error('string')
  return Buffer.from(str, 'hex')
}

async function tmpdir() {
  let removeTmp
  const path = await TMP({
    teardown: (cb) => {
      removeTmp = cb
    }
  })
  return [path, removeTmp]
}
