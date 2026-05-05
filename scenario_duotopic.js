const Replicator = require('.')
const ReadyResource = require('ready-resource')
const DHT = require('hyperdht')
const createTestnet = require('hyperdht/testnet')
const TMP = require('test-tmp')
const Corestore = require('corestore')
const Autobase = require('autobase')
// const attachMembershipRPC = require('./rpc')

const GLOBAL_TOPIC = Buffer.alloc(32).fill(0xa)

async function main() {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const [path, rmdir] = await tmpdir()

  const peers = []

  let baseKey = null

  for (let group = 0; group < 5; group++) {
    for (let i = 0; i < 3 /* peers per group */; i++) {
      const peer = new Peer(path, bootstrap, baseKey, group)

      if (!baseKey) {
        await peer.ready()
        baseKey = peer.counter.key
      }

      peers.push(peer)
    }
  }
  await Promise.all(peers.map(p => p.ready()))

  // run scenario
  await Promise.all(peers.map(p => p.run()))

  // teardown
  await Promise.all(peers.map(p => p.destroy()))
  await testnet.destroy()
  await rmdir()
  console.log('teardown complete')
}

main()

let _peerctr = 0

class Peer extends ReadyResource {
  constructor(prefix, bootstrap, remoteKey = null, group = null) {
    super()
    this.id = _peerctr++
    this.group = group

    const path = `${prefix}/${this.id}`
    this.root = new Corestore(path)

    this.store = this.root.session()
    this.counter = new CounterCore(this.store, remoteKey)

    this.dht = new DHT({ bootstrap })

    this.replicator = new Replicator(this.root, {
      dht: this.dht,
      manual: false
    })
  }

  async _open() {
    // initialize namespace & writer
    await this.counter.ready()

    // dual topic
    this.replicator.join(this.store, GLOBAL_TOPIC)

    if (this.group !== null) {
      const topic = Buffer.alloc(32).fill(this.group)
      this.replicator.join(this.store, topic)
    }

    console.log(this.id, 'opened')
  }

  async run() {
    await new Promise(resolve => setTimeout(resolve, 3000))
    console.log(this.id, 'run complete')
  }

  async _close() {
    await this.root.close()
    await this.replicator.destroy()
    await this.dht.destroy()
  }

  destroy() {
    return this.close()
  }
}

class CounterCore {
  constructor(store, key = null) {
    this.store = store
    this.base = new Autobase(store, key, {
      open,
      apply,
      valueEncoding: 'json'
    })

    function open(store) {
      return store.get('counter-view', { valueEncoding: 'json' })
    }

    async function apply(nodes, view, host) {
      for (const node of nodes) {
        const { type } = node.value
        switch (type) {
          case 'value':
            await view.append(node.value.value)
            break
          case 'writer':
            await host.addWriter(fromHex(node.value.key),  { isIndexer: false })
            break
        }
      }
    }
  }

  async bump(value = null) {
    if (value === null) {
      const current = await this.value()
      value = current + 1
    }
    await this.base.append({ type: 'value', value })
    await this.base.update()
    return value
  }

  async addWriter(writerKey) {
    await this.base.append({ type: 'writer', key: toHex(writerKey) })
    await this.base.update()
  }

  async update() {
    await this.base.update()
  }

  // TODO: autogerenated glop
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

function fromHex(str){
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
