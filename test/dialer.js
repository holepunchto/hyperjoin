const test = require('brittle')
const b4a = require('b4a')
const Dialer = require('../lib/dialer')
const Peer = require('../lib/peer')

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
  t.is(dialer._slots.n, 2, 'dial slots in use')

  const suspended = dialer.suspend()
  t.ok(dialer.suspended, 'flag set')
  t.absent(dialer._refreshTimer, 'refreshTimer cleared')

  // TODO: Aborts inflight
  //
  // await ctx.nextDial
  // t.is(dialer.inflight, 1)

  await suspended // inflight work settled

  t.is(dialer.inflight, 0)
  t.is(dialer._slots.n, 3, 'slots released')
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

test('a retry due during a dial is re-scheduled', async (t) => {
  const backoffDuration = Peer.backoffDuration
  Peer.backoffDuration = () => 5
  t.teardown(() => {
    Peer.backoffDuration = backoffDuration
  })

  let settle = null
  const done = new Promise((resolve) => {
    settle = resolve
  })

  let dials = 0

  const dialer = new Dialer(
    {
      async ondial(peer, now) {
        const complete = peer.markAttempt(true, { now })

        if (dials++ === 0) {
          await new Promise((resolve) => setTimeout(resolve, 20))
          complete(true)
        } else {
          complete()
          settle()
        }
      },
      ondialerror: () => {},
      isConnected: () => false
    },
    { jitter: 0 }
  )
  t.teardown(() => dialer.destroy())

  dialer.topicEnable(TOPIC)
  dialer.seen({ publicKey: asKey(1) }, TOPIC)

  await dialer.flush()
  t.is(dials, 1, 'first retry')
  t.ok(dialer._refreshTimer, 'due retry is re-scheduled')

  await done
  t.is(dials, 2, 'second retry')
  t.absent(dialer._refreshTimer, 'no more work scheduled')
})

test('Dialer throttles when host is at max capacity', async (t) => {
  let cap = 2
  let calls = 0
  const want = () => {
    calls++
    return cap
  }
  const onsettle = () => cap--

  const ctx = spawnDialer(t, {
    handlers: { want },
    onsettle
  })

  const { dialer, dialed } = ctx

  for (let n = 1; n <= 60; n++) {
    spawnPeer(ctx, n)
  }

  dialer.refresh()

  await dialer.flush()

  t.is(dialed.length, 2, 'dials upto capacity')

  cap = 2 // refill

  dialer.refresh()
  await dialer.flush()

  t.is(dialed.length, 4, 'refilled')
})

test('Dialer throttles when max inflight is at cap', async (t) => {
  const N_PEERS = 80
  const ctx = spawnDialer(t)
  const { dialer, dialed, maxInflight } = ctx

  for (let n = 1; n <= N_PEERS; n++) {
    spawnPeer(ctx, n)
  }

  // spam refresh every 500ms for 1 minute

  const now = Date.now()

  for (let t = 0; t < 60_000; t += 500) {
    console.log('T=', t, Date.now() - now, dialed.length)
    dialer.refresh(now + t)
    await dialer.flush(now + t)

    if (dialed.length === N_PEERS) break
  }

  console.log(dialed.length)

  t.is(dialed.length, 80, 'all peers dialed')
  t.is(maxInflight(), 3, 'inflight is bounded')
})

test('Completed dial automatically calls refresh()', async (t) => {
  const ctx = spawnDialer(t, { autoSettle: false, maxParallel: 1 })
  const { dialer, dialed, pass } = ctx

  const first = spawnPeer(ctx, 1)
  const second = spawnPeer(ctx, 2)

  pass(first.publicKey)
  pass(second.publicKey)

  await dialer.flush()

  t.is(dialed.length, 2, 'slot release starts the deferred peer without another refresh')
})

function spawnDialer(
  t,
  {
    maxParallel = 3,
    jitter = 0,
    suspended = false,
    autoSettle = true,
    handlers: _handlers = {},
    onsettle = () => {}
  } = {}
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
        onsettle(true, settled)
      }

      const attemptFailed = (err) => {
        settled++
        const doneAt = now + Date.now() - startedAt
        completeAttempt(true, err, doneAt)
        onsettle(false, settled)
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
    isConnected: (k) => connected.has(toHex(k)),
    isSelf: (publicKey) => b4a.equals(publicKey, SELF),

    ..._handlers
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
