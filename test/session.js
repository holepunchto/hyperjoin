const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Replicator = require('..')
const { unpromise } = require('./helpers/backports')
const { toHex } = b4a

test('join() waits for a prior leave() on slow unannounce', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await replicator.ready()

  const session1 = replicator.join(topic, { pin: true })
  replicator.refresh()
  await replicator.flush()

  t.is(replicator.stats.dht.announce, 1, 'first session announced')

  replicator.leave(topic)
  const closing = session1.closing

  const session2 = replicator.join(topic, { pin: true }) // re-join same topic
  replicator.refresh()

  t.not(session2, session1, 'join() returns a fresh session')
  t.absent(session1.closed, 'first session is still closing')

  t.is(replicator.sessions.get(toHex(topic)), session2, 'replacement is tracked')
  t.absent([...replicator.sessions.values()].includes(session1), 'first session is removed')

  t.is(session2.discovery._wait, closing, 'replacement waits for leave')

  await closing
  await replicator.flush()

  t.ok(session1.closed, 'first session closed')
  t.is(replicator.stats.dht.announce, 2, 'replacement announced after leave')

  await replicator.close()
  await testnet.destroy()
})

test('replicator.close() waits for a replaced closing session', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await replicator.ready()

  const session1 = replicator.join(topic, { pin: true })
  replicator.refresh()
  await replicator.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = replicator.dht.unannounce.bind(replicator.dht)
  replicator.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  replicator.leave(session1.topic)
  replicator.join(session1.topic, { pin: true })

  let closed = false
  const closing = replicator.close().then(() => {
    closed = true
  })

  await unannouncing
  t.absent(session1.closed, 'replaced session is still closing')
  t.is(closed, false, 'not yet closed')

  release()

  await closing
  t.is(closed, true, 'replicator closed')

  t.ok(session1.closed, 'replaced session closed')

  await testnet.destroy()
})

test('replicator.suspend() waits for a replaced closing session', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await replicator.ready()

  const session1 = replicator.join(topic, { pin: true })
  replicator.refresh()
  await replicator.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = replicator.dht.unannounce.bind(replicator.dht)
  replicator.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  replicator.leave(session1.topic)
  replicator.join(session1.topic, { pin: true })

  let suspended = false
  const suspending = replicator.suspend().then(() => {
    suspended = true
  })

  await unannouncing
  t.absent(session1.closed, 'replaced session is still closing')
  t.absent(suspended, 'suspension waits for the replaced session')

  release()
  await suspending

  t.ok(session1.closed, 'replaced session closed')

  await replicator.resume()
  await replicator.close()
  await testnet.destroy()
})

test('closing a replacement session waits for the session it replaced', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const replicator = new Replicator(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await replicator.ready()

  const session1 = replicator.join(topic, { pin: true })
  replicator.refresh()
  await replicator.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = replicator.dht.unannounce.bind(replicator.dht)
  replicator.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  replicator.leave(session1.topic)
  const replacement = replicator.join(session1.topic, { pin: true })

  let closed = false
  const closing = replacement.close().then(() => {
    closed = true
  })

  await unannouncing
  t.absent(session1.closed, 'replaced session is still closing')
  t.absent(closed, 'replacement waits for the replaced session')

  release()
  await closing

  t.ok(session1.closed, 'replaced session closed')

  await replicator.close()
  await testnet.destroy()
})
