const test = require('brittle')
const b4a = require('b4a')
const createTestnet = require('hyperdht/testnet')
const Hyperjoin = require('..')
const { unpromise } = require('./helpers/backports')
const { toHex } = b4a

test('join() waits for a prior leave() on slow unannounce', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await hyperjoin.ready()

  const session1 = hyperjoin.join(topic, { pin: true })
  hyperjoin.refresh()
  await hyperjoin.flush()

  t.is(hyperjoin.stats.dht.announce, 1, 'first session announced')

  hyperjoin.leave(topic)
  const closing = session1.closing

  const session2 = hyperjoin.join(topic, { pin: true }) // re-join same topic
  hyperjoin.refresh()

  t.not(session2, session1, 'join() returns a fresh session')
  t.absent(session1.closed, 'first session is still closing')

  t.is(hyperjoin.sessions.get(toHex(topic)), session2, 'replacement is tracked')
  t.absent([...hyperjoin.sessions.values()].includes(session1), 'first session is removed')

  t.is(session2.discovery._wait, closing, 'replacement waits for leave')

  await closing
  await hyperjoin.flush()

  t.ok(session1.closed, 'first session closed')
  t.is(hyperjoin.stats.dht.announce, 2, 'replacement announced after leave')

  await hyperjoin.close()
  await testnet.destroy()
})

test('hyperjoin.close() waits for a replaced closing session', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await hyperjoin.ready()

  const session1 = hyperjoin.join(topic, { pin: true })
  hyperjoin.refresh()
  await hyperjoin.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = hyperjoin.dht.unannounce.bind(hyperjoin.dht)
  hyperjoin.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  hyperjoin.leave(session1.topic)
  hyperjoin.join(session1.topic, { pin: true })

  let closed = false
  const closing = hyperjoin.close().then(() => {
    closed = true
  })

  await unannouncing
  t.absent(session1.closed, 'replaced session is still closing')
  t.is(closed, false, 'not yet closed')

  release()

  await closing
  t.is(closed, true, 'hyperjoin closed')

  t.ok(session1.closed, 'replaced session closed')

  await testnet.destroy()
})

test('hyperjoin.suspend() waits for a replaced closing session', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await hyperjoin.ready()

  const session1 = hyperjoin.join(topic, { pin: true })
  hyperjoin.refresh()
  await hyperjoin.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = hyperjoin.dht.unannounce.bind(hyperjoin.dht)
  hyperjoin.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  hyperjoin.leave(session1.topic)
  hyperjoin.join(session1.topic, { pin: true })

  let suspended = false
  const suspending = hyperjoin.suspend().then(() => {
    suspended = true
  })

  await unannouncing
  t.absent(session1.closed, 'replaced session is still closing')
  t.absent(suspended, 'suspension waits for the replaced session')

  release()
  await suspending

  t.ok(session1.closed, 'replaced session closed')

  await hyperjoin.resume()
  await hyperjoin.close()
  await testnet.destroy()
})

test('closing a replacement session waits for the session it replaced', async (t) => {
  const testnet = await createTestnet()
  const { bootstrap } = testnet
  const hyperjoin = new Hyperjoin(
    () => {},
    () => null,
    { bootstrap, manual: true }
  )
  const topic = b4a.alloc(32, 1)

  await hyperjoin.ready()

  const session1 = hyperjoin.join(topic, { pin: true })
  hyperjoin.refresh()
  await hyperjoin.flush()

  const [unannouncing, start] = unpromise()
  const [released, release] = unpromise()

  const unannounce = hyperjoin.dht.unannounce.bind(hyperjoin.dht)
  hyperjoin.dht.unannounce = async (...args) => {
    start()
    await released
    return unannounce(...args)
  }

  hyperjoin.leave(session1.topic)
  const replacement = hyperjoin.join(session1.topic, { pin: true })

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

  await hyperjoin.close()
  await testnet.destroy()
})
