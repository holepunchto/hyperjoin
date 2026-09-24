const test = require('brittle')
const b4a = require('b4a')
const Peer = require('../lib/peer')

const T0 = Date.now()
const TOPIC = b4a.alloc(32, 1)
const asKey = (n) => b4a.alloc(32, n)

// mirrors of lib/peer.js (module-private)
const LADDER = [1_000, 5_000, 15_000]
const COOLDOWN = 1000 * 60
const BAN_DURATION = 1000 * 60 * 30
const EXPIRE_DURATION = 1000 * 60 * 15

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
  t.is(peer.expiresAt, T0 + EXPIRE_DURATION)
  t.ok(peer.isExpired(T0 + EXPIRE_DURATION + 1), 'expired past the window')

  peer.observe(TOPIC, T0 + 1000)
  t.is(peer.expiresAt, T0 + 1000 + EXPIRE_DURATION, 'window pushed')
  t.ok(!peer.isExpired(T0 + EXPIRE_DURATION + 1), 'no longer expired')
})

test('attempt failure increases retryAt', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  t.is(peer.retryAt, T0, 'due since last seen')

  const done = peer.markAttempt(true, { now: T0 + 100 })
  done(true, 'ERR_CONNECT', T0 + 200)

  t.is(peer.retryAt, T0 + 100 + LADDER[0], 'anchored at attempt start')
  t.ok(!peer.shouldDial(T0 + 100 + LADDER[0] - 1), 'cooling')
  t.ok(peer.shouldDial(T0 + 100 + LADDER[0]), 'dialable at the leg')
})

test('3 failed attempts set a 60s cooldown, then resets', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)

  let at = T0
  for (const leg of LADDER) {
    peer.markAttempt(true, { now: at })(true, 'ERR_CONNECT', at)
    t.is(peer.retryAt, at + leg, 'leg ' + leg)
    at = peer.retryAt
  }

  peer.markAttempt(true, { now: at })(true, 'ERR_CONNECT', at)
  t.is(peer.retryAt, at + COOLDOWN, 'ladder exhausted')
})

test('Connection of 30s resets attempts', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  peer.markAttempt(true, { now: T0 })(true, 'ERR_CONNECT', T0)
  t.is(peer.retryAt, T0 + LADDER[0], 'laddered')

  const connectedAt = T0 + LADDER[0]
  const disconnectedAt = connectedAt + COOLDOWN / 2 + 1

  peer.markConnected(true, connectedAt)
  peer.markDisconnected(true, null, disconnectedAt)

  t.is(peer.retryAt, peer.lastSeenAt, 'attempts reset')
  t.ok(peer.shouldDial(disconnectedAt), 'dialable again')
})

test('connection marks updates counters', (t) => {
  const { peer } = spawnPeer()

  t.ok(!peer.connected, 'birth')
  peer.markConnected(true, T0)
  t.ok(peer.connected, 'connected after connect')
  t.is(peer.connectedAt, T0)
  peer.markDisconnected(true, null, T0 + 20000)
  t.ok(!peer.connected, 'disconnected after disconnect')
  t.is(peer.disconnectedAt, T0 + 20000)

  peer.markConnected(true, T0 + 30000)
  peer.markDisconnected(true, null, T0 + 30500)

  t.is(peer.timeConnected, 20500, 'accrues across sessions')
  t.is(peer._connectedCount, 2)
  t.is(peer._disconnectedCount, 2)
})

test('bans are retained for 30minutes', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  peer.setBanned('hammering', T0)

  t.ok(peer.isBanned)
  t.is(peer.expiresAt, T0 + BAN_DURATION, 'ban carries its own clock')
  t.ok(!peer.shouldDial(T0), 'banned peers are not dialed')
  t.ok(!peer.isExpired(T0 + BAN_DURATION), 'retained for the ban')
  t.ok(peer.isExpired(T0 + BAN_DURATION + 1), 'dropped after')
})

test('attempt failed w/ HOLEPUNCH_ABORTED: forceRelaying armed, attempts immediate re-dial', (t) => {
  const { peer } = spawnPeer()

  peer.observe(TOPIC, T0)
  peer.markAttempt(true, { canRelay: true, now: T0 })(true, { code: 'HOLEPUNCH_ABORTED' }, T0)

  t.ok(peer.forceRelaying, 'relaying armed')
  t.is(peer.retryAt, peer.lastSeenAt, 'attempts reset')
  t.ok(peer.shouldDial(T0), 'immediate re-dial')
})

test('gc expires a peer by its own clock', (t) => {
  const { peer: fresh } = spawnPeer(1)
  t.ok(!fresh.isExpired(T0), 'no clock: retained while young')

  const { peer: live } = spawnPeer(2)
  live.markConnected(false, T0)
  t.ok(!live.isExpired(T0 + EXPIRE_DURATION + 1), 'connected peers are retained')

  const { peer: dropped } = spawnPeer(3)
  dropped.markConnected(false, T0)
  dropped.markDisconnected(false, null, T0 + 100)
  t.ok(!dropped.isExpired(T0 + 100 + EXPIRE_DURATION), 'disconnect clock retained')
  t.ok(dropped.isExpired(T0 + 101 + EXPIRE_DURATION), 'disconnect clock expired')

  const { peer: banned } = spawnPeer(4)
  banned.observe(TOPIC, T0)
  banned.setBanned('hammering', T0)
  t.ok(!banned.isExpired(T0 + BAN_DURATION), 'ban clock retained')
  t.ok(banned.isExpired(T0 + BAN_DURATION + 1), 'ban clock expired')
})
