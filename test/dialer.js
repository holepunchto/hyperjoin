const test = require('brittle')
const b4a = require('b4a')
const Dialer = require('../lib/dialer')

const T0 = Date.now()
const TOPIC = b4a.alloc(32, 1)
const SELF = b4a.alloc(32, 0)
const asKey = (n) => b4a.alloc(32, n)
const { toHex } = b4a

test('fires in order', async (t) => {
  const ctx = spawnDialer(t, { suspended: true })
  const { dialer, dialed } = ctx

  const T1 = T0 + 1000
  const T2 = T0 + 2000
  const T3 = T0 + 3000

  spawnPeer(ctx, 1, T1)
  spawnPeer(ctx, 3, T3)
  spawnPeer(ctx, 2, T2)

  dialer.resume(T3) // launch
  const done = dialer.flush(T3)

  await done

  t.alike(
    dialed.map((p) => p.publicKey[0]),
    [1, 2, 3]
  )
})

test('maxParallel dials capped to 3', async (t) => {
  const ctx = spawnDialer(t, { suspended: true })
  const { dialer, dialed } = ctx

  for (let n = 1; n <= 10; n++) {
    spawnPeer(ctx, n)
  }

  dialer.resume() // start
  await dialer.flush()

  t.is(ctx.maxInflight(), 3, 'in-flight bound to MAX_PARALLEL')
  t.is(dialed.length, 10, 'all 10 peers dialed')
})

test('refresh() garbage collects expired entries', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer } = ctx

  const peer = spawnPeer(ctx, 1, T0)

  dialer.refresh(T0)
  t.is(dialer.peers.size, 1, 'peer retained')

  dialer.refresh(peer.expiresAt)
  t.is(dialer.peers.size, 1, 'still retained')

  dialer.refresh(peer.expiresAt + 1)
  t.is(dialer.peers.size, 0, 'peer garbage collected')
})

test('does not garbage collect connected peers', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  const peer = spawnPeer(ctx, 1, T0)
  dialer.refresh(T0)

  await dialer.flush(T0)
  t.is(dialed.length, 1, 'dial fired')

  t.ok(peer.connected, 'settled dial is marked connected')

  dialer.refresh(peer.expiresAt + 1)
  t.is(dialer.peers.size, 1, 'connected peer exempt from gc')
  t.is(dialed.length, 1, 'no re-dial for connected peer')
})

test('dialer.flush() snapshots known work', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  const T1 = T0 + 1000
  const T2 = T0 + 6000

  const peer1 = spawnPeer(ctx, 1, T1)
  dialer.refresh(T1)

  let settled = false
  const snapshot = dialer.flush(T1)
  snapshot.then(() => {
    settled = true
  })

  spawnPeer(ctx, 2, T2)
  dialer.refresh(T2)

  t.is(settled, false, 'snapshot still pending')

  await snapshot

  t.is(settled, true, 'snapshot settled')
  t.is(dialed.filter((p) => p === peer1).length, 1, 'dial 1 fired once')
  t.is(dialed.length, 2, 'later work admitted after the snapshot')

  await dialer.flush(T2)
})

test('suspend/resume', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer } = ctx

  spawnPeer(ctx, 1)

  dialer.refresh()

  const suspended = dialer.suspend()
  t.ok(dialer.suspended, 'flag set')
  t.absent(dialer._refreshTimer, 'refreshTimer cleared')

  // TODO: Aborts inflight
  //
  // await ctx.nextDial
  // t.is(dialer.inflight, 1)

  await suspended // inflight work settled

  t.is(dialer.inflight, 0)
  t.is(ctx.dialed.length, 0, 'aborted before it fired')

  dialer.resume()

  t.is(dialer.suspended, false, 'flag cleared')

  await dialer.flush()

  t.is(ctx.dialed.length, 1, 'aborted dial resumed')
  t.is(ctx.connected.size, 1, 'peer connected')
  t.ok(dialer._refreshTimer, 'refreshTimer re-armed')
})

test('Dialer errors are routed to `ondialerror`', async (t) => {
  const ctx = spawnDialer(t, { autoSettle: false })
  const { dialer, dialed, errors } = ctx

  ctx.fail(asKey(1))
  const peer = spawnPeer(ctx, 1, T0)

  dialer.refresh(T0)
  const snapshot = dialer.flush(T0) // drains on peer's rejection
  await snapshot

  t.is(dialed.length, 1, 'failing dial fired')
  t.is(errors.length, 1, 'rejection re-routed to ondialerror')
  t.is(dialed.filter((p) => p === peer).length, 1, 'fired exactly once')
})

// TODO: reproduce in basic.js with a full replicator stack
test('peer.retryAt increased on failure and is re-dialed', async (t) => {
  const ctx = spawnDialer(t, { autoSettle: false })
  const { dialer, dialed, errors, fail, pass } = ctx

  fail(asKey(1))

  const peer = spawnPeer(ctx, 1)
  dialer.refresh()

  const { retryAt } = peer

  await dialer.flush()

  t.is(dialed.length, 1, 'one attempt')
  t.is(errors.length, 1, 'one failed')
  t.ok(peer.retryAt > retryAt, 'retryAt increased')

  pass(asKey(1))

  const retried = dialer.flush(peer.retryAt)

  dialer.refresh(peer.retryAt) // launch dial

  await retried

  t.is(errors.length, 1, 'still one failed')
  t.is(dialed.length, 2, 'two attempts')

  t.is(ctx.connected.size, 1, 'one successful')
})

test('dialer does not dial peers without an active topic', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed, errors } = ctx

  dialer.topicDisable(TOPIC)

  const peer = spawnPeer(ctx, 1, T0)

  dialer.refresh()
  await dialer.flush()

  t.is(dialed.length, 0, 'topic disabled: no dials')
  t.is(errors.length, 0)

  dialer.topicEnable(TOPIC)

  dialer.refresh()
  await dialer.flush() // drains on peer's rejection

  t.is(dialed.length, 1, 'topic enabled: dials')
  t.is(errors.length, 0)
  t.is(dialed.filter((p) => p === peer).length, 1, 'fired exactly once')
})

test('A peer that holds a stable connection and then drops is re-dialed', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  const peer = spawnPeer(ctx, 1, T0 - 5000)

  dialer.refresh()
  await dialer.flush()

  t.is(dialed.length, 1, 'first dial fired')
  t.ok(peer.connected, 'connected')

  const disconnectedAt = Date.now() + 10 * 1000 + 1

  ctx.connected.delete(toHex(peer.publicKey))
  peer.markDisconnected(true, null, disconnectedAt)

  t.is(peer._outboundAttempts, 0, 'stable session reset the ladder')
  t.ok(peer.shouldDial(disconnectedAt), 'peer says re-dial me')

  dialer.refresh(disconnectedAt)
  await dialer.flush(disconnectedAt)

  t.is(dialed.length, 2, 're-dialed after the drop')
})

function spawnDialer(
  t,
  { maxParallel = 3, jitter = 0, suspended = false, autoSettle = true } = {}
) {
  const dialed = []
  const errors = []
  const connected = new Set()
  const allowed = new Set()
  const blocked = new Set()
  let settled = 0
  let maxInflight = 0

  const handlers = {
    ondial: (peer, now) => {
      dialed.push(peer)
      const startedAt = Date.now()
      const completeAttempt = peer.markAttempt(true, { now })

      maxInflight = Math.max(maxInflight, dialed.length - settled)

      const attemptSuccess = () => {
        settled++
        const doneAt = now + Date.now() - startedAt
        completeAttempt(false, null, doneAt)
        connected.add(toHex(peer.publicKey))
        peer.markConnected(true, doneAt) // Connection.on('open')
      }

      const attemptFailed = (err) => {
        settled++
        const doneAt = now + Date.now() - startedAt
        completeAttempt(true, err, doneAt)
        throw err // rethrow
      }

      const key = toHex(peer.publicKey)

      if (autoSettle || allowed.delete(key)) {
        return Promise.resolve().then(attemptSuccess)
      }

      // reject
      if (blocked.delete(key)) {
        return Promise.reject(new Error('blocked')).catch(attemptFailed)
      }
    },
    ondialerror: (err) => errors.push(err),
    isConnected: (k) => connected.has(k),
    isSelf: (publicKey) => b4a.equals(publicKey, SELF)
  }

  const dialer = new Dialer(handlers, { maxParallel, jitter, suspended })
  dialer.topicEnable(TOPIC)

  t.teardown(() => dialer.destroy())

  return {
    dialer,
    dialed,
    errors,
    connected,
    maxInflight: () => maxInflight,
    fail(publicKey) {
      blocked.add(toHex(publicKey))
    },
    pass(publicKey) {
      allowed.add(toHex(publicKey))
    }
  }
}

function spawnPeer(ctx, n, lastSeenAt) {
  ctx.dialer.seen({ publicKey: asKey(n) }, TOPIC, lastSeenAt)
  return ctx.dialer.lookup(asKey(n))
}
