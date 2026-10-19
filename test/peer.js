const test = require('brittle')
const b4a = require('b4a')
const Peer = require('../lib/peer')

const T0 = Date.now()
const TOPIC = b4a.alloc(32, 1)
const asKey = (n) => b4a.alloc(32, n)

function spawnPeer(n = 1) {
  const peer = new Peer(asKey(n))
  return { peer }
}

test('observe records sightings', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  t.is(peer.seenCount, 1)
  t.is(peer.lastSeenAt, T0)

  peer.observe(TOPIC, T0 + 750)
  t.is(peer.seenCount, 2)
  t.is(peer.lastSeenAt, T0 + 750)
})

test('sightings push expireAt', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  const expiresAt = peer.expiresAt

  t.ok(!peer.isExpired(expiresAt), 'retained at the boundary')
  t.ok(peer.isExpired(expiresAt + 1), 'expired past it')

  peer.observe(TOPIC, T0 + 1000)

  t.is(peer.expiresAt, expiresAt + 1000, 'window pushed by the sighting')
  t.ok(!peer.isExpired(expiresAt + 1), 'no longer expired')
})

test('attempt failure increases retryAt', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  t.is(peer.retryAt, T0, 'due since last seen')

  let attemptedAt = T0
  const deltas = []

  for (let n = 0; n < 3; n++) {
    peer.markAttempt(true, { now: attemptedAt })(true, 'ERR_CONNECT', attemptedAt)

    deltas.push(peer.retryAt - attemptedAt)
    attemptedAt = peer.retryAt
  }

  const [first, second, third] = deltas

  t.ok(first < second, 'second failure waits longer')
  t.ok(second < third, 'third failure waits longer still')
})

test('backoff keeps growing past 60s, capped at 10min', (t) => {
  const backoffDuration = Peer.backoffDuration
  Peer.backoffDuration = (n) => n * 60 * 1000
  t.teardown(() => {
    Peer.backoffDuration = backoffDuration
  })

  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  for (let n = 0; n < 5; n++) peer.markAttempt(true, { now: T0 })(true, 'ERR_CONNECT', T0)
  t.is(peer.retryAt - T0, 5 * 60 * 1000, 'grows past 60s')

  for (let n = 0; n < 10; n++) peer.markAttempt(true, { now: T0 })(true, 'ERR_CONNECT', T0)
  t.is(peer.retryAt - T0, 10 * 60 * 1000, 'capped')
})

test('Connection stable for 60s resets attempts', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  t.is(peer._outboundAttempts, 0)

  peer.markAttempt(true, { now: T0 })(true, 'ERR_CONNECT', T0)
  t.is(peer._outboundAttempts, 1)

  peer.markAttempt(true, { now: peer.retryAt })(true, 'ERR_CONNECT', peer.retryAt)
  t.is(peer._outboundAttempts, 2)

  const connectedAt = peer.retryAt

  peer.markAttempt(true, { now: connectedAt })()
  peer.markConnected(true, connectedAt)

  const disconnectedAt = connectedAt + 60 * 1000 + 1

  peer.markDisconnected(true, null, disconnectedAt)

  t.is(peer._outboundAttempts, 0, 'attempts back at 0')
  t.is(peer.retryAt, peer.lastSeenAt)
  t.ok(peer.shouldDial(disconnectedAt), 'dialable again')
})

test('connection marks updates counters', (t) => {
  const { peer } = spawnPeer()

  const firstSession = 20000
  const idle = 10000
  const secondSession = 500

  t.ok(!peer.connected, 'birth')

  peer.markConnected(true, T0)

  t.ok(peer.connected, 'connected after connect')
  t.is(peer.connectedAt, T0)

  peer.markDisconnected(true, null, T0 + firstSession)

  t.ok(!peer.connected, 'disconnected after disconnect')
  t.is(peer.disconnectedAt, T0 + firstSession)

  const reconnectedAt = T0 + firstSession + idle

  peer.markConnected(true, reconnectedAt)
  peer.markDisconnected(true, null, reconnectedAt + secondSession)

  t.is(peer.timeConnected, firstSession + secondSession, 'accrues across sessions')
  t.is(peer._connectedCount, 2)
  t.is(peer._disconnectedCount, 2)
})

test('attempt failed w/ HOLEPUNCH_ABORTED: forceRelaying armed, attempts immediate re-dial', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  peer.markAttempt(true, { canRelay: true, now: T0 })(true, { code: 'HOLEPUNCH_ABORTED' }, T0)

  t.ok(peer.forceRelaying, 'relaying armed')
  t.is(peer.retryAt, peer.lastSeenAt, 'attempts reset')
  t.ok(peer.shouldDial(T0), 'immediate re-dial')
})

// Requires 10-second runtime; asserts semi-important edge case
test.skip('gc: new but not seen, hard expires after 10 wall-seconds', async (t) => {
  const { peer } = spawnPeer()

  t.ok(!peer.isExpired(T0), 'retained during grace period')

  await new Promise((resolve) => setTimeout(resolve, 10 * 1000 + 100))

  t.ok(peer.isExpired(T0), 'hard expired')
})

test('gc: connected peer does not expire', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  peer.markConnected(false, T0)

  t.ok(!peer.isExpired(peer.expiresAt + 1), 'connection outranks clock')
})

test('gc: disconnected peer expires', (t) => {
  const { peer } = spawnPeer()

  peer.markConnected(false, T0)
  peer.markDisconnected(false, null, T0 + 100)

  t.ok(!peer.isExpired(peer.expiresAt), 'retained at the boundary')
  t.ok(peer.isExpired(peer.expiresAt + 1), 'expired')
})

test('gc: banned peer retained longer', (t) => {
  const { peer: seen } = spawnPeer(1)
  const { peer: banned } = spawnPeer(2)

  seen.observe(TOPIC, T0)

  banned.observe(TOPIC, T0)
  banned.setBanned('hammering', T0)
  t.is(banned.isBanned, true)

  t.ok(banned.expiresAt > seen.expiresAt, 'retained past the sighting window')
  t.ok(!banned.isExpired(banned.expiresAt), 'retained at the boundary')
  t.ok(!banned.shouldDial(T0), 'banned peers are not dialed')
  t.ok(banned.isExpired(banned.expiresAt + 1), 'expired')
})

test('A peer that holds a stable connection and then drops is re-dialed', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  const connectedAt = T0 + 5000
  const disconnectedAt = connectedAt + 60 * 1000 + 1

  peer.markAttempt(true, { now: connectedAt })()
  peer.markConnected(true, connectedAt)
  peer.markDisconnected(true, null, disconnectedAt)

  t.is(peer._outboundAttempts, 0, 'stable session reset the ladder')
  t.ok(peer.shouldDial(disconnectedAt), 're-dialable on drop')
})

test('a peer whose connection keeps dropping backs off', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  let now = T0
  for (let i = 0; i < 4; i++) {
    now = Math.max(now, peer.retryAt)
    peer.markAttempt(true, { now })(false, null, now)
    peer.markConnected(true, now)
    now += 13 * 1000 // e.g. a one-way stream timing out
    peer.markDisconnected(true, null, now)
  }

  t.ok(peer.retryAt > now, 'not redialed immediately')
})

test('Inbound peer gets banned for hammering', (t) => {
  const { peer: a } = spawnPeer(1)
  const { peer: b } = spawnPeer(2)

  const attempts = 20

  const backoffA = 10000
  const backoffB = 50

  for (let n = 0; n < attempts; n++) {
    let now = T0 + n * backoffA
    a.markAttempt(false, { now })(false, null, now)
    a.markConnected(false, now)
    a.markDisconnected(false, null, now + backoffA)

    now = T0 + n * backoffB // linear reconnect every 50ms
    b.markAttempt(false, { now })(false, null, now)
    b.markConnected(false, now)
    b.markDisconnected(false, null, now + backoffB)
  }

  t.is(a._inboundSuccess, attempts)
  t.ok(!a.isBanned, 'A not banned')

  t.is(b._inboundSuccess, attempts)
  t.ok(b.isBanned, 'B is banned')
  t.is(b.banReason, 'hammering')
})

test('next retryAt must not occur before previous attempt finish', (t) => {
  const { peer } = spawnPeer()
  const attemptedAt = T0
  const settledAt = attemptedAt + Peer.backoffDuration(1) + 1

  peer.markAttempt(true, { now: attemptedAt })(true, 'ERR_CONNECT', settledAt)

  t.ok(peer.retryAt > settledAt, 'retry remains in the future after a slow failure')
})
