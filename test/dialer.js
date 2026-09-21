const test = require('brittle')
const b4a = require('b4a')
const Dialer = require('../lib/dialer')

const T0 = Date.now()
const EXPIRE_DURATION = 60_000 * 15 // mirror of lib/peer.js (module-private)
const SELF = b4a.alloc(32, 0)
const key = (n) => b4a.alloc(32, n)
const hex = (buf) => b4a.toString(buf, 'hex')

function spawnDialer (t, { maxParallel = 3, jitter = 0, suspended = false, autoSettle = false } = {}) {
  const dialed = []
  const errors = []
  const pending = []
  const connected = new Set()
  const failing = new Set()
  let settled = 0
  let maxInflight = 0

  const dialer = new Dialer({
    ondial: (peer) => {
      dialed.push(peer)
      maxInflight = Math.max(maxInflight, dialed.length - settled)
      if (autoSettle) {
        settled++
        return Promise.resolve()
      }
      if (failing.delete(hex(peer.publicKey))) return Promise.reject(new Error('connect failed'))
      return new Promise((resolve) => pending.push({ peer, resolve }))
    },
    ondialerror: (err) => errors.push(err),
    isConnected: (k) => connected.has(k),
    isSelf: (publicKey) => b4a.equals(publicKey, SELF)
  }, { maxParallel, jitter, suspended })

  t.teardown(() => dialer.destroy())

  return {
    dialer,
    dialed,
    pending,
    errors,
    maxInflight: () => maxInflight,
    fail (publicKey) {
      failing.add(hex(publicKey))
    },
    settleOne () {
      const entry = pending.shift()
      if (!entry) return
      connected.add(hex(entry.peer.publicKey))
      entry.resolve()
      settled++
    },
    settle () {
      const batch = pending.splice(0)
      for (const { peer, resolve } of batch) {
        connected.add(hex(peer.publicKey))
        resolve()
        settled++
      }
    }
  }
}

function seed (ctx, n, lastSeenAt) {
  const peer = ctx.dialer.lookup(key(n))
  peer.lastSeenAt = lastSeenAt
  return peer
}

test('fires in order', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed } = ctx

  seed(ctx, 1, T0 - 1000)
  seed(ctx, 3, T0 - 3000)
  seed(ctx, 2, T0 - 2000)

  await dialer.refresh(T0)

  t.alike(
    dialed.map((p) => hex(p.publicKey)),
    [3, 2, 1].map((n) => hex(key(n)))
  )

  ctx.settle()
})

test('dials capped to 3 (MAX_PARALLEL)', async (t) => {
  const ctx = spawnDialer(t, { autoSettle: true })
  const { dialer, dialed } = ctx

  for (let n = 1; n <= 5; n++) seed(ctx, n, T0 - n * 1000)

  const flushing = dialer.refresh(T0)
  const bound = dialer.flush() // bound to max _nextAt — the last queued dial

  await bound
  t.is(ctx.maxInflight(), 3, 'in-flight capped at MAX_PARALLEL')
  t.is(dialed.length, 5, 'queue drains one admission per release')

  await flushing
})

test('garbage collects expired entries', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer } = ctx

  seed(ctx, 1, T0)
  seed(ctx, 2, T0 - EXPIRE_DURATION - 1)

  await dialer.refresh(T0)

  t.is(dialer.peers.has(hex(key(2))), false, 'expired entry dropped')
  t.ok(dialer.peers.has(hex(key(1))), 'live entry retained')
  t.is(dialer.peers.size, 1)
})

test('dialer.flush() returns bound promise to currently known dials', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed, pending } = ctx

  seed(ctx, 1, T0 - 1000)
  await dialer.refresh(T0)
  t.is(dialed.length, 1, 'dial in flight')

  const bound = dialer.flush()

  seed(ctx, 2, T0 - 1000)
  await dialer.refresh(T0 + 1)
  t.is(dialed.length, 2, 'in-flight dial not re-fired; new dial fired') // not implemented

  ctx.settleOne()
  await bound
  t.is(ctx.pending.length, 1, 'flush did not wait for the later dial')

  ctx.settle()
})

test('suspend/resume stops and restarts internal timer', async (t) => {
  const ctx = spawnDialer(t, { suspended: true })
  const { dialer, dialed } = ctx

  seed(ctx, 1, T0 - 1000)

  t.ok(dialer.suspended, 'spawned parked')
  await dialer.refresh(T0)
  t.is(dialed.length, 0, 'no dial fired while stopped')

  await dialer.resume()
  t.is(dialer.suspended, false, 'resume restarts the internal timer')

  const bound = dialer.flush()
  ctx.settle()
  await bound
  t.is(dialed.length, 1, 'due dial fires after resume')
})

test('failed connect does not abort interal timer', async (t) => {
  const ctx = spawnDialer(t)
  const { dialer, dialed, errors } = ctx

  ctx.fail(key(1))
  seed(ctx, 1, T0 - 1000)
  seed(ctx, 2, T0 - 1000)

  await dialer.refresh(T0)
  t.is(dialed.length, 2, 'both dials fired')
  t.is(errors.length, 1, 'failure routed to ondialerror, not thrown')

  ctx.settle()

  seed(ctx, 3, T0 - 1000)
  await dialer.refresh(T0 + 1)
  t.is(dialed.length, 3, 'timer admits work after a failure — failed peer backed off, settled peer connected') // not implemented
  ctx.settle()
})
