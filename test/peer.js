const test = require('brittle')
const b4a = require('b4a')
const Peer = require('../lib/peer')

// Finite-time unit suite for lib/peer.js — no dialer, no wall clock: every
// method takes `now`. Contract: markAttempt(initiator, { now, canRelay }) arms dials/
// attemptedAt/_isDialing and returns the one-shot settler
// done(failed, cause, now); the settler branches the dial counters, clears
// _isDialing, and on a failed holepunch-refusal cause arms forceRelaying and
// resets dials for an instant relay redial. A session must outlive
// COOLDOWN / 2 to redeem the ladder (markDisconnected). No jitter
// (dialer-side concern) — every assert is exact.

const T0 = 10_000 // clock origin
const LADDER = [1_000, 5_000, 15_000] // mirror of lib/peer.js (module-private)
const COOLDOWN = 60_000
const TOPIC = b4a.alloc(32, 1)

const key = (n) => b4a.alloc(32, n)

// Unit isolation: the live dialer owns relayThrough; standalone peers get the
// minimal shape the failed-settle guard reads.
const peer = (n) => new Peer(key(n))

test('birth: fresh peer is dialable, due since "last seen"', (t) => {
  const p = peer(1)

  t.alike(p.publicKey, key(1), 'key carried')
  t.is(p.retryAt, -1, 'due time = lastSeenAt sentinel')
  t.ok(p.shouldDial(T0), 'fresh peer is dialable')
  t.ok(!p.connected, 'not connected')
  t.is(p.dials, 0)
  t.is(p._dialsSuceeded, 0) // sic — field name as placed
  t.is(p._dialsFailed, 0)
  t.is(p.timeConnected, 0)

  p.observe(TOPIC, T0)
  t.is(p.retryAt, T0, 'empty ladder tracks lastSeenAt')
})

test('observe books sightings', (t) => {
  const p = peer(1)

  p.observe(TOPIC, T0)
  t.is(p.seenCount, 1)
  t.is(p.lastSeenAt, T0)

  p.observe(TOPIC, T0 + 750)
  t.is(p.seenCount, 2)
  t.is(p.lastSeenAt, T0 + 750)
})

test('sightings never move a laddered due time', (t) => {
  const p = peer(1)

  const done = p.markAttempt(true, { now: T0 })
  done(true, null, T0 + 5) // dials = 1

  p.observe(TOPIC, T0 + 6)
  p.observe(TOPIC, T0 + 7)

  t.is(p.dials, 1, 'dials survive sightings')
  t.is(p.retryAt, T0 + LADDER[0], 'due time anchored to attemptedAt')
})

test('refusal: dial with no connection rides leg 1, anchored at attempt start', (t) => {
  const p = peer(1)

  const done = p.markAttempt(true, { now: T0 })
  done(true, null, T0 + 5)

  t.is(p.dials, 1)
  t.is(p._dialsFailed, 1)
  t.is(p.retryAt, T0 + LADDER[0], 'leg 1, no jitter')
  t.ok(!p.shouldDial(T0 + LADDER[0]), 'cooling at the boundary (strict)')
  t.ok(p.shouldDial(T0 + LADDER[0] + 1), 'dialable one tick past')
})

test('young deaths climb the ladder', (t) => {
  const p = peer(1)
  let at = T0
  let spent = 0

  for (let i = 1; i <= 3; i++) {
    const done = p.markAttempt(true, { now: at })
    p.markConnected(true, at + 5)
    p.markDisconnected(true, null, at + 15) // lived 10ms — young
    done(true, null, at + 20)
    spent += 10

    t.is(p.dials, i, `dial ${i} recorded`)
    t.is(p.retryAt, at + LADDER[i - 1], `leg ${i} from attempt start`)
    t.ok(!p.shouldDial(at + LADDER[i - 1]), `cooling through leg ${i}`)
    t.ok(p.shouldDial(at + LADDER[i - 1] + 1), `dialable past leg ${i}`)

    at += LADDER[i - 1] + 1 // ride out the leg before the next dial
  }

  t.is(p.timeConnected, spent, 'young sessions still accrue')
})

test('fourth young death retires for the cooldown', (t) => {
  const p = peer(1)
  let at = T0

  for (let i = 1; i <= 4; i++) {
    const done = p.markAttempt(true, { now: at })
    p.markConnected(true, at + 5)
    p.markDisconnected(true, null, at + 15)
    done(true, null, at + 20)
    at += 20
  }

  t.is(p.dials, 4, 'four young deaths')
  t.is(p.retryAt, T0 + 60 + COOLDOWN, 'cooldown leg past the ladder')
  t.ok(!p.shouldDial(T0 + 60 + COOLDOWN), 'quiet through the cooldown')
  t.ok(p.shouldDial(T0 + 60 + COOLDOWN + 1), 're-engages after')
})

test('a connection that sticks forgives the ladder', (t) => {
  const p = peer(1)

  // two young deaths
  const d1 = p.markAttempt(true, { now: T0 })
  p.markConnected(true, T0 + 5)
  p.markDisconnected(true, null, T0 + 15)
  d1(true, null, T0 + 20)
  const d2 = p.markAttempt(true, { now: T0 + 1020 })
  p.markConnected(true, T0 + 1025)
  p.markDisconnected(true, null, T0 + 1035)
  d2(true, null, T0 + 1040)

  t.is(p.dials, 2, 'ladder carried into the third dial')
  t.is(p._dialsFailed, 2)

  // a dial that sticks
  const d3 = p.markAttempt(true, { now: T0 + 6040 })
  p.markConnected(true, T0 + 6440) // 400ms dial
  p.markDisconnected(true, null, T0 + 36441) // lived 30s+1ms — past the redemption bar

  t.is(p.timeConnected, 30021, 'spent time across all sessions')
  d3(false, null, T0 + 36441)
  t.is(p._dialsSuceeded, 1)
  t.is(p.dials, 0, 'ladder forgotten')
  t.ok(p.shouldDial(T0 + 36441), 'immediately eligible again')
})

test('dial latency reads off the timestamps', (t) => {
  const p = peer(1)

  const done = p.markAttempt(true, { now: T0 })
  t.ok(p.isDialing, 'in-flight once the dial begins')
  p.markConnected(true, T0 + 420)

  t.is(p.connectedAt - p.attemptedAt, 420, 'stamp delta')
  done(false, null, T0 + 420)
  t.is(p._dialsSuceeded, 1)
  t.ok(!p.isDialing, 'clear at the verdict')

  const q = peer(2)
  const qdone = q.markAttempt(true, { now: T0 })
  qdone(true, null, T0 + 90)
  t.is(q._dialsFailed, 1)
})

test('session counters and stamps', (t) => {
  const p = peer(1)

  t.ok(!p.connected, 'birth')
  p.markConnected(true, T0)
  t.ok(p.connected, 'connected after connect')
  t.is(p.connectedAt, T0)
  p.markDisconnected(true, null, T0 + 20000)
  t.ok(!p.connected, 'disconnected after disconnect')
  t.is(p.disconnectedAt, T0 + 20000)

  p.markConnected(true, T0 + 30000)
  p.markDisconnected(true, null, T0 + 30500)

  t.is(p.timeConnected, 20500, 'accrues across sessions')
  t.is(p._connectedCount, 2)
  t.is(p._disconnectedCount, 2)
})

test('expiry: never-seen, sighted, connected, disconnected clocks', (t) => {
  const q = peer(9)
  t.ok(!q.isExpired(T0 + 100_000_000), 'never-seen: no clock started, no staleness')

  const p = peer(1)
  p.observe(TOPIC, T0)
  t.ok(!p.isExpired(T0 + COOLDOWN * 3), 'sighted: boundary strict')
  t.ok(p.isExpired(T0 + COOLDOWN * 3 + 1), 'sighted: expires past 3x')

  const done = p.markAttempt(true, { now: T0 + 1 })
  p.markConnected(true, T0 + 2)
  done(false, null, T0 + 2)
  t.ok(!p.isExpired(T0 + 10_000_000), 'connected peers never expire')

  p.markDisconnected(true, null, T0 + 5000)
  t.ok(!p.isExpired(T0 + 5000 + COOLDOWN * 3), 'clock restarts at disconnect')
  t.ok(p.isExpired(T0 + 5000 + COOLDOWN * 3 + 1), 'expires from its own clock')
})

test('shouldDial goes quiet between 2x freshness and 3x expiry', (t) => {
  const p = peer(1)

  p.observe(TOPIC, T0)
  t.ok(p.shouldDial(T0 + COOLDOWN * 2 - 1), 'dialable while fresh')
  t.ok(!p.shouldDial(T0 + COOLDOWN * 2), 'quiet at the freshness boundary')
  t.ok(!p.isExpired(T0 + COOLDOWN * 2), 'but not yet forgettable')
})

test('redemption bar: a session must outlive COOLDOWN / 2 to forgive', (t) => {
  const p = peer(1)
  const done = p.markAttempt(true, { now: T0 })
  p.markConnected(true, T0 + 5)
  p.markDisconnected(true, null, T0 + 5 + COOLDOWN / 2) // delta == bar — strict comparison
  t.is(p.dials, 1, 'boundary session does not redeem')

  const q = peer(2)
  const qdone = q.markAttempt(true, { now: T0 })
  q.markConnected(true, T0 + 5)
  q.markDisconnected(true, null, T0 + 5 + COOLDOWN / 2 + 1) // one tick past the bar
  qdone(false, null, T0 + 5 + COOLDOWN / 2 + 1) // attempt closes at hand-off in the live wiring
  t.is(q.dials, 0, 'one tick past the bar forgives')
  t.ok(q.shouldDial(T0 + 5 + COOLDOWN / 2 + 1), 'and is immediately dialable')
})

test('orphan verdict (the GC contract Peer owns)', (t) => {
  const p = peer(1)
  t.ok(p.isOrphaned(new Set(['cafe'])), 'no seen topics -> orphaned')
  p.observe(TOPIC, T0)
  const live = new Set([b4a.toString(TOPIC, 'hex')])
  t.ok(!p.isOrphaned(live), 'a live session topic keeps the record')
  t.ok(p.isOrphaned(new Set(['beef'])), 'when the last session dies, orphaned again')
})

test('ban retention: the 30x Max-GC-retain statement lives in expiresAt', (t) => {
  const p = peer(3)
  t.ok(!p.isBanned, 'fresh record is not banned')
  p.setBanned(T0)
  t.ok(p.isBanned, 'setBanned marks the record')
  t.is(p.expiresAt, T0 + COOLDOWN * 30, 'banned: Max GC retain at 30x')
  t.ok(!p.isExpired(T0 + COOLDOWN * 30), 'retained through the boundary')
  t.ok(p.isExpired(T0 + COOLDOWN * 30 + 1), 'expires past the retain')

  const r = peer(4)
  r.lastSeenAt = T0
  t.is(r.expiresAt, T0 + COOLDOWN * 3, 'unbanned: staleness expiry at 3x')

  const q = peer(5)
  q.setBanned(T0) // never-seen: the firewall-mint shape
  t.ok(!q.isExpired(T0), 'ban clock runs without a sighting clock')
  t.ok(q.isExpired(T0 + COOLDOWN * 30 + 1), 'and still expires at 30x')
})

test('attempt failed w/ HOLEPUNCH_ABORTED: forceRelaying armed, attempts reset', (t) => {
  const p = peer(9)
  p.observe(TOPIC, T0)

  // climb the ladder first: three failed dials
  for (let i = 0; i < 3; i++) {
    const done = p.markAttempt(true, { now: T0 + i * 1000 })
    done(true, null, T0 + i * 1000)
  }
  t.is(p.dials, 3, 'laddered before the refusal')
  t.is(p.forceRelaying, false, 'relay not armed yet')
  t.ok(!p.shouldDial(T0 + 3000), 'cooling on leg 3')

  // a plain failure: neither arms the relay nor forgives the ladder
  const plain = p.markAttempt(true, { now: T0 + 4000 })
  plain(true, new Error('plain dial failure'), T0 + 4000)
  t.is(p.forceRelaying, false, 'plain failure: relay stays off')
  t.is(p.dials, 4, 'plain failure: ladder intact')

  // the holepunch refusal verdict
  const done = p.markAttempt(true, { now: T0 + 5000, canRelay: true })
  done(
    true,
    Object.assign(new Error('holepunch aborted'), { code: 'HOLEPUNCH_ABORTED' }),
    T0 + 5000
  )

  t.is(p.forceRelaying, true, 'relay fast-connect armed')
  t.is(p.dials, 0, 'attempts reset for the instant relay redial')
  t.ok(p.shouldDial(T0 + 5000), 'instantly dialable: retryAt falls back to lastSeenAt')

  // the flag outlives the redial — the relay selector consumes it
  p.markAttempt(true, { now: T0 + 5001 })
  t.is(p.forceRelaying, true, 'flag survives the redial')
})
