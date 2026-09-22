const test = require('brittle')
const b4a = require('b4a')
const Dialer = require('../lib/dialer')

const T0 = Date.now()
const TOPIC = b4a.alloc(32, 1)
const SELF = b4a.alloc(32, 0)
const key = (n) => b4a.alloc(32, n)
const { toHex } = b4a

function spawnDialer (t, { maxParallel = 3, jitter = 0, suspended = false, autoSettle = false } = {}) {
  const dialed = []
  const errors = []
  const pending = []
  const connected = new Set()
  const blocked = new Set()
  let settled = 0
  let maxInflight = 0

  const handlers = {
    ondial: (peer) => {
      dialed.push(peer)

      const completeAttempt = peer.markAttempt(true)

      maxInflight = Math.max(maxInflight, dialed.length - settled)

      const attemptSuccess = () => {
        settled++
        completeAttempt()
        connected.add(toHex(peer.publicKey))
        peer.markConnected(true) // Connection.on('open')
      }

      const attemptFailed = (err) => {
        settled++
        completeAttempt(true, err)
        // peer.markDisconnected(true, err) // Connection.on('close')
        throw err // rethrow
      }

      if (autoSettle) {
        return Promise.resolve()
          .then(attemptSuccess)
      }

      // reject
      if (blocked.delete(toHex(peer.publicKey))) {
        return Promise.reject(new Error('blocked'))
          .catch(attemptFailed)
      }

      // settle later
      return new Promise((resolve) => pending.push({ peer, resolve }))
        .then(attemptSuccess)
        .catch(attemptFailed)
    },
    ondialerror: (err) => errors.push(err),
    isConnected: (k) => connected.has(k),
    isSelf: (publicKey) => b4a.equals(publicKey, SELF)
  }

  const dialer = new Dialer(handlers, { maxParallel, jitter, suspended })

  t.teardown(() => dialer.destroy())

  return {
    dialer,
    dialed,
    pending,
    errors,
    maxInflight: () => maxInflight,
    fail (publicKey) {
      blocked.add(toHex(publicKey))
    },
    settleOne () {
      const entry = pending.shift()
      if (!entry) throw new Error('nothing to settle')
      entry.resolve()
    },
    settle () {
      const batch = pending.splice(0)
      for (const { resolve } of batch) resolve()
    }
  }
}

function spawnPeer (ctx, n, lastSeenAt) {
  ctx.dialer.seen({ publicKey: key(n) }, TOPIC, lastSeenAt)
  return ctx.dialer.lookup(key(n))
}

test('fires in order', async (t) => {
  const ctx = spawnDialer(t, { suspended: true })
  const { dialer, dialed } = ctx

  const T1 = T0 + 1000
  const T2 = T0 + 2000
  const T3 = T0 + 3000

  spawnPeer(ctx, 1, T1)
  spawnPeer(ctx, 3, T3)
  spawnPeer(ctx, 2, T2)

  await dialer.resume(T3) // launch

  t.alike(
    dialed.map((p) => p.publicKey[0]),
    [1, 2, 3]
  )

  ctx.settle()
})

test('Max parallel dials capped to 3', async (t) => {
  const ctx = spawnDialer(t, { autoSettle: true, suspended: true })
  const { dialer, dialed } = ctx

  for (let n = 1; n <= 5; n++) spawnPeer(ctx, n, T0 - n * 1000)

  await dialer.resume(T0 + 10_000)

  t.is(ctx.maxInflight(), 3, 'in-flight capped at MAX_PARALLEL')
  t.is(dialed.length, 5, 'queue drains one admission per release')
})

test('garbage collects expired entries', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer } = ctx

  const peer = spawnPeer(ctx, 1, T0)

  await dialer.refresh(T0)
  t.is(dialer.peers.size, 1, 'peer retained')

  await dialer.refresh(peer.expiresAt)
  t.is(dialer.peers.size, 1, 'still retained')

  await dialer.refresh(peer.expiresAt + 1)
  t.is(dialer.peers.size, 0, 'peer garbage collected')
})

test('does not garbage collect connected peers', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  const peer = spawnPeer(ctx, 1, T0)
  await dialer.refresh(T0)
  t.is(dialed.length, 1, 'dial fired')

  ctx.settle()
  await dialer.flush(T0)

  t.ok(peer.connected, 'settled dial is marked connected')

  await dialer.refresh(peer.expiresAt + 1)
  t.is(dialer.peers.size, 1, 'connected peer exempt from gc')
  t.is(dialed.length, 1, 'no re-dial for connected peer')
})

test.solo('dialer.flush() snapshots known work', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed, pending } = ctx

  const T1 = T0 + 1000
  const T2 = T0 + 2000

  const peer1 = spawnPeer(ctx, 1, T1)
  await dialer.refresh(T1)
  t.is(dialed.length, 1, 'dial 1 in flight')

  let snapshotSettled = false
  const snapshot = dialer.flush(T1)
  snapshot.then(() => { snapshotSettled = true })

  spawnPeer(ctx, 2, T2)
  await dialer.refresh(T2)

  t.is(dialed.filter((p) => p === peer1).length, 1, 'in-flight dial not re-fired')
  t.is(dialed.length, 2, 'later work admitted after the snapshot')

  t.is(snapshotSettled, false, 'snapshot still pending')

  ctx.settleOne() // settle 1st dial

  await snapshot
  t.is(snapshotSettled, true, 'snapshot settled')

  t.is(pending.length, 1, 'peer 2 still pending')

  ctx.settle() // settle remaining work
  t.is(pending.length, 0, 'no work')
})

test('suspend/resume stops and restarts internal timer', async (t) => {
  const ctx = spawnDialer(t, { suspended: true })
  const { dialer, dialed } = ctx

  const T1 = T0 + 1000

  const peer = spawnPeer(ctx, 1, T1)
  t.ok(dialer.suspended, 'spawned parked')

  await dialer.refresh(T1)
  t.is(dialed.length, 0, 'does not dial while suspended')
  t.absent(dialer._refreshTimer, 'timer not armed')
  t.is(dialer.peers.size, 1, 'seen() still records — work is deferred, not dropped')

  await dialer.resume()
  t.is(dialer.suspended, false, 'resume clears suspended flag')
  t.is(dialed.length, 0, 'resume does not mis-fire work due in the future')
  t.ok(dialer._refreshTimer, 'internal timer re-armed')

  // manual refresh
  await dialer.refresh(T1)

  t.is(dialed.filter((p) => p === peer).length, 1, 'due work fires once the clock reaches the horizon')

  // drain work
  const snapshot = dialer.flush(T1)
  ctx.settle()
  await snapshot
})

test('Dialer errors are routed to `ondialerror`', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed, errors } = ctx

  const T1 = T0 + 1000

  ctx.fail(key(1))
  const peer = spawnPeer(ctx, 1, T1)

  dialer.refresh(T1)
  const snapshot = dialer.flush(T1) // drains on peer's rejection
  await snapshot

  t.is(dialed.length, 1, 'failing dial fired')
  t.is(errors.length, 1, 'rejection re-routed to ondialerror')
  t.is(dialed.filter((p) => p === peer).length, 1, 'fired exactly once')
})

// TODO: additionally reproduced in basic.js with a full replicator stack
test('peer.retryAt increased on failure and is re-dialed', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  const T1 = T0 + 1000

  ctx.fail(key(1))
  const peer = spawnPeer(ctx, 1, T1)

  const tick = dialer.refresh(T1)
  const bound = dialer.flush(T1)
  await bound
  await tick

  t.is(ctx.errors.length, 1, 'first attempt failed')
  t.ok(peer.retryAt > T1, 'failure pushed retryAt past the horizon')

  await dialer.refresh(T1 + 500) // inside the backoff window
  t.is(dialed.filter((p) => p === peer).length, 1, 'not re-dialed inside the window')

  ctx.fail(key(1)) // re-arm — the harness consumed the first flag on the failed dial
  await dialer.refresh(peer.retryAt)
  t.is(dialed.filter((p) => p === peer).length, 2, 're-dialed at the pushed horizon')

  ctx.settle()
})
