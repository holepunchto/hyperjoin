const test = require('brittle')
const b4a = require('b4a')
const Peer = require('../lib/peer')

// Finite-time unit suite for lib/peer.js — no dialer, no wall clock: every
// method takes `now`. Contract as on the board: retryAt is a getter anchored
// to attemptedAt (+ leg, or COOLDOWN past the ladder); an empty ladder is
// "due since last seen"; connected()/disconnected() stamp their clocks;
// attemptDone branches the counters and returns the attempt span. No jitter
// (dialer-side concern) — every assert is exact.
//
// REFEREE: connected() still zeroes attempts, so young deaths can never
// climb — attempts snaps to 0, retryAt falls back to lastSeenAt, backoff
// evaporates after every session, retirement is unreachable. The red tests
// below stay red until that stone moves to attemptDone(!failed).

const T0 = 10_000 // clock origin
const LADDER = [1_000, 5_000, 15_000] // mirror of lib/peer.js (module-private)
const COOLDOWN = 60_000

const key = (n) => b4a.alloc(32, n)

test('birth: fresh peer is dialable, due since "last seen"', (t) => {
  const p = new Peer(key(1))

  t.alike(p.publicKey, key(1), 'key carried')
  t.is(p.retryAt, -1, 'due time = lastSeenAt sentinel')
  t.ok(p.shouldDial(T0), 'fresh peer is dialable')
  t.ok(!p.connected, 'not connected')
  t.is(p.attempts, 0)
  t.is(p._attemptsSuceeded, 0) // sic — field name as placed
  t.is(p._attemptsFailed, 0)
  t.is(p.timeConnected, 0)

  p.observe(T0)
  t.is(p.retryAt, T0, 'empty ladder tracks lastSeenAt')
})

test('observe books sightings', (t) => {
  const p = new Peer(key(1))

  p.observe(T0)
  t.is(p.seenCount, 1)
  t.is(p.lastSeenAt, T0)

  p.observe(T0 + 750)
  t.is(p.seenCount, 2)
  t.is(p.lastSeenAt, T0 + 750)
})

test('sightings never move a laddered due time', (t) => {
  const p = new Peer(key(1))

  p.markAttempt(T0)
  p.markAttemptDone(T0 + 5, true) // attempts = 1

  p.observe(T0 + 6)
  p.observe(T0 + 7)

  t.is(p.attempts, 1, 'attempts survive sightings')
  t.is(p.retryAt, T0 + LADDER[0], 'due time anchored to attemptedAt')
})

test('refusal: dial with no connection rides leg 1, anchored at attempt start', (t) => {
  const p = new Peer(key(1))

  p.markAttempt(T0)
  t.is(p.markAttemptDone(T0 + 5, true), 5, 'returns the attempt span')

  t.is(p.attempts, 1)
  t.is(p._attemptsFailed, 1)
  t.is(p.retryAt, T0 + LADDER[0], 'leg 1, no jitter')
  t.ok(!p.shouldDial(T0 + LADDER[0]), 'cooling at the boundary (strict)')
  t.ok(p.shouldDial(T0 + LADDER[0] + 1), 'dialable one tick past')
})

test('young deaths climb the ladder', (t) => { // REFEREE — red until stone B
  const p = new Peer(key(1))
  let at = T0
  let spent = 0

  for (let i = 1; i <= 3; i++) {
    p.markAttempt(at)
    p.markConnected(at + 5)
    p.markDisconnected(at + 15) // lived 10ms — young
    p.markAttemptDone(at + 20, true)
    spent += 10

    t.is(p.attempts, i, `attempt ${i} recorded`)
    t.is(p.retryAt, at + LADDER[i - 1], `leg ${i} from attempt start`)
    t.ok(!p.shouldDial(at + LADDER[i - 1]), `cooling through leg ${i}`)
    t.ok(p.shouldDial(at + LADDER[i - 1] + 1), `dialable past leg ${i}`)

    at += LADDER[i - 1] + 1 // ride out the leg before the next dial
  }

  t.is(p.timeConnected, spent, 'young sessions still accrue')
})

test('fourth young death retires for the cooldown', (t) => { // REFEREE
  const p = new Peer(key(1))
  let at = T0

  for (let i = 1; i <= 4; i++) {
    p.markAttempt(at)
    p.markConnected(at + 5)
    p.markDisconnected(at + 15)
    p.markAttemptDone(at + 20, true)
    at += 20
  }

  t.is(p.attempts, 4, 'four young deaths')
  t.is(p.retryAt, T0 + 60 + COOLDOWN, 'cooldown leg past the ladder')
  t.ok(!p.shouldDial(T0 + 60 + COOLDOWN), 'quiet through the cooldown')
  t.ok(p.shouldDial(T0 + 60 + COOLDOWN + 1), 're-engages after')
})

test('a connection that sticks forgives the ladder', (t) => {
  const p = new Peer(key(1))

  // two young deaths
  p.markAttempt(T0)
  p.markConnected(T0 + 5)
  p.markDisconnected(T0 + 15)
  p.markAttemptDone(T0 + 20, true)
  p.markAttempt(T0 + 1020)
  p.markConnected(T0 + 1025)
  p.markDisconnected(T0 + 1035)
  p.markAttemptDone(T0 + 1040, true)

  t.is(p.attempts, 2, 'ladder carried into the third dial') // REFEREE
  t.is(p._attemptsFailed, 2)

  // a dial that sticks
  p.markAttempt(T0 + 6040)
  p.markConnected(T0 + 6440) // 400ms dial
  p.markDisconnected(T0 + 36441) // lived 30s+1ms — past the redemption bar

  t.is(p.timeConnected, 30021, 'spent time across all sessions')
  t.is(p.markAttemptDone(T0 + 36441, false), 30401, 'attempt span returned')
  t.is(p._attemptsSuceeded, 1)
  t.is(p.attempts, 0, 'ladder forgotten')
  t.ok(p.shouldDial(T0 + 36441), 'immediately eligible again')
})

test('dial latency reads off the timestamps, both readings agree', (t) => {
  const p = new Peer(key(1))

  p.markAttempt(T0)
  p.markConnected(T0 + 420)

  t.is(p.connectedAt - p.attemptedAt, 420, 'stamp delta')
  t.is(p.markAttemptDone(T0 + 420, false), 420, 'returned span')
  t.is(p._attemptsSuceeded, 1)

  const q = new Peer(key(2))
  q.markAttempt(T0)
  t.is(q.markAttemptDone(T0 + 90, true), 90, 'failed attempt span')
  t.is(q._attemptsFailed, 1)
})

test('session counters and stamps', (t) => {
  const p = new Peer(key(1))

  t.ok(!p.connected, 'birth')
  p.markConnected(T0)
  t.ok(p.connected, 'connected after connect')
  t.is(p.connectedAt, T0)
  p.markDisconnected(T0 + 20000)
  t.ok(!p.connected, 'disconnected after disconnect')
  t.is(p.disconnectedAt, T0 + 20000)

  p.markConnected(T0 + 30000)
  p.markDisconnected(T0 + 30500)

  t.is(p.timeConnected, 20500, 'accrues across sessions')
  t.is(p._connectedCount, 2)
  t.is(p._disconnectedCount, 2)
})

test('expiry: never-seen, sighted, connected, disconnected clocks', (t) => {
  const q = new Peer(key(9))
  t.ok(!q.isExpired(COOLDOWN * 3 - 1), 'never-seen lives on the wall clock')
  t.ok(q.isExpired(COOLDOWN * 3), 'never-seen expires at 3x')

  const p = new Peer(key(1))
  p.observe(T0)
  t.ok(!p.isExpired(T0 + COOLDOWN * 3), 'sighted: boundary strict')
  t.ok(p.isExpired(T0 + COOLDOWN * 3 + 1), 'sighted: expires past 3x')

  p.markAttempt(T0 + 1)
  p.markConnected(T0 + 2)
  t.ok(!p.isExpired(T0 + 10_000_000), 'connected peers never expire')

  p.markDisconnected(T0 + 5000)
  t.ok(!p.isExpired(T0 + 5000 + COOLDOWN * 3), 'clock restarts at disconnect')
  t.ok(p.isExpired(T0 + 5000 + COOLDOWN * 3 + 1), 'expires from its own clock')
})

test('shouldDial goes quiet between 2x freshness and 3x expiry', (t) => {
  const p = new Peer(key(1))

  p.observe(T0)
  t.ok(p.shouldDial(T0 + COOLDOWN * 2 - 1), 'dialable while fresh')
  t.ok(!p.shouldDial(T0 + COOLDOWN * 2), 'quiet at the freshness boundary')
  t.ok(!p.isExpired(T0 + COOLDOWN * 2), 'but not yet forgettable')
})

test('redemption bar: a session must outlive COOLDOWN / 2 to forgive', (t) => {
  const p = new Peer(key(1))
  p.markAttempt(T0)
  p.markConnected(T0 + 5)
  p.markDisconnected(T0 + 5 + COOLDOWN / 2) // delta == bar — strict comparison
  t.is(p.attempts, 1, 'boundary session does not redeem')

  const q = new Peer(key(2))
  q.markAttempt(T0)
  q.markConnected(T0 + 5)
  q.markDisconnected(T0 + 5 + COOLDOWN / 2 + 1)
  t.is(q.attempts, 0, 'one tick past the bar forgives')
  t.ok(q.shouldDial(T0 + 5 + COOLDOWN / 2 + 1), 'and is immediately dialable')
})
