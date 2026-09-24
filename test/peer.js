const test = require('brittle')
const b4a = require('b4a')
const Peer = require('../lib/peer')


const T0 = Date.now()
const TOPIC = b4a.alloc(32, 1)

function spawnPeer() {
  const peer = new Peer()
  return { peer }
}

test('observe records sightings', (t) => {
  const p = peer(1)

  p.observe(TOPIC, T0)
  t.is(p.seenCount, 1)
  t.is(p.lastSeenAt, T0)

  p.observe(TOPIC, T0 + 750)
  t.is(p.seenCount, 2)
  t.is(p.lastSeenAt, T0 + 750)
})

test('sightings push expireAt', (t) => {
  // TODO: implement
})

test('attempt failure increases retryAt', (t) => {
  // TODO: implement
})

test('3 failed attempts set a 60s cooldown, then resets', (t) => {
  // TODO: implement
})


test('Connection of 30s resets attempts', (t) => {
})

test('connection marks updates counters', (t) => {
  // TODO rewrite this test; it's bad

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

test('bans are retained for 30minutes', (t) => {
  // TODO: implement
})

test('attempt failed w/ HOLEPUNCH_ABORTED: forceRelaying armed, attempts immediate re-dial', (t) => {
  // TODO: implement
})
