const test = require('brittle')
const createTestnet = require('hyperdht/testnet')
const { spawnPeer } = require('./helpers')

test.skip('autoswarm: two new instances do not swarm (sanity test)', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer({ autoswarm: true })
  const p2 = await swarm.spawnPeer({ autoswarm: false })

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  await swarm.destroy()
})

test.skip('autoswarm: active instances', async (t) => {
  const swarm = await spawnSwarm(t)

  const p1 = await swarm.spawnPeer()
  const p2 = await swarm.spawnPeer()
  p1.repSession.startActive()
  p2.repSession.startActive()

  await new Promise((resolve) => setTimeout(resolve, 2000))

  t.is(p1.replicator.connectionCount, 1)
  t.is(p2.replicator.connectionCount, 1)

  await swarm.destroy()
})

test.skip('autoswarm: can enable discovery swarming', async (t) => {
  const swarm = await spawnSwarm(t)

  const updatedDiscoveryTimeout = 500
  const p1 = await swarm.spawnPeer({
    refreshInterval: 50, // TODO: not implemented
    gcIdleTime: 100, // TODO: not implemented
    updatedDiscoveryTimeout,
  })
  const p2 = await swarm.spawnPeer()

  const { topic } = swarm

  t.is(p1.replicator.connectionCount, 0)
  t.is(p2.replicator.connectionCount, 0)

  const session1 = p1.repSession
  session1.startDiscovery()
  await p1.replicator.refresh()

  t.is(session1.announce, false)
  t.is(session1.discoveryTimeout, discoveryTimeout)

  p2.repSession.startActive()
  await p2.replicator.refresh()

  // discovery window is shortened on successfull replication

  session1.notifyDiscoveryUpdate()
  await p1.replicator.refresh()

  t.is(p1.replicator.sessions.get(toHex(topic)), session1, 'still open')
  t.is(session1.announce, false, 'still not listening')
  t.is(session1.discoveryTimeout, updatedDiscoveryTimeout, 'shortened')

  // discovery should deactivate

  await new Promise((resolve) => setTimeout(resolve, updatedDiscoveryTimeout * 5))
  await p1.replicator.refresh()

  t.absent(p1.replicator.sessions.get(toHex(topic)))

  await swarm.destroy()
})

test.skip('autoswarm: replicator activates fallbacks', (t) => {})

async function spawnSwarm(t) {
  const topic = Buffer.alloc(32, 0xAA)
  const testnet = await createTestnet()
  const { bootstrap } = testnet

  const peers = []
  let commonCores = null

  return {
    topic,
    async spawnPeer(opts) {
      const peer = await spawnPeer(t, {
        bootstrap,
        autoswarm: true,
        ...opts
      })

      if (!commonCores) {
        commonCores = peer.cores.map(c => c.key)
      } else {
        // refresh session
        await peer.session.close()
        peer.session = peer.store.session()

        peer.cores = commonCores.map((key) => peer.session.get({ key }))
      }

      peers.push(peer)

      const repSession = await peer.replicator.join(peer.session, topic)

      return { ...peer, repSession }
    },
    async destroy() {
      await Promise.all(peers.map(p => p.destroy()))
      await testnet.destroy()
    }
  }
}

function toHex(buffer) {
  return buffer.toString('hex')
}
