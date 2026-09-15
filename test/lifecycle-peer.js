const test = require('brittle')
const b4a = require('b4a')
const Dialer = require('../lib/dialer')
const PeerInfo = require('../lib/peer-info')

// Spec test: the legacy dialer.seen() heuristics move into PeerInfo._seen().
// Stage 1 (green): exact behavioral equivalence with HEAD seen().
// Stage 2 flips are marked inline: sighting booking (counters/timestamps),
// the waiting gate (sightings stop bypassing the ladder) and rebirth durability.

const TOPIC = b4a.alloc(32, 0x42)
const hex = (b) => b4a.toString(b, 'hex')
const tick = () => new Promise((resolve) => setTimeout(resolve, 10))

function createDialer() {
  const dials = []
  const connected = new Set()
  const sessions = new Map() // hex(topic) -> live session

  const replicator = {
    peerId: b4a.alloc(32, 0xff),
    sessions,
    stats: { bannedPeers: 0 },
    hasConnection: (k) => connected.has(b4a.isBuffer(k) ? hex(k) : k), // buffer or hex key
    connecting: 0,
    connectionCount: 0,
    outbound: new Map(),
    isPresuspended: false,
    _maxPeers: 40,
    _connect: async (peerInfo) => {
      dials.push(peerInfo)
    },
    emit: () => {}
  }

  const dialer = new Dialer(replicator)
  return { dialer, dials, connected, sessions }
}

function requireContract(t) {
  if (typeof PeerInfo.prototype._seen !== 'function') {
    t.fail('lib contract missing: PeerInfo._seen')
    return false
  }
  return true
}

test('peer is created when seen', (t) => {
  const { dialer } = createDialer()
  t.teardown(() => dialer.destroy())
  if (!requireContract(t)) return

  const pk = b4a.alloc(32, 1)
  const t0 = Date.now()

  dialer.seen({ publicKey: pk }, TOPIC)

  const info = dialer.peers.get(hex(pk))
  t.ok(info instanceof PeerInfo, 'registry holds a PeerInfo')
  t.alike(info.publicKey, pk)
  t.ok(info.createdAt >= t0, 'createdAt stamped on first sighting')
  t.ok(info.lastSeenAt >= t0, 'lastSeenAt stamped')
  // stage 2: seenCount === 1 once sighting booking lands
})

test('peer is updated when seen', (t) => {
  const { dialer } = createDialer()
  t.teardown(() => dialer.destroy())
  if (!requireContract(t)) return

  const pk = b4a.alloc(32, 2)
  const relays = [{ host: 'relay-2' }]

  dialer.seen({ publicKey: pk }, TOPIC)
  const info = dialer.peers.get(hex(pk))
  t.is(info.relayAddresses, undefined, 'no relayAddresses advertised yet')
  t.alike(info.topics.map(hex), [hex(TOPIC)], 'topic registered')

  const seenAt = info.lastSeenAt
  dialer.seen({ publicKey: pk, relayAddresses: relays }, TOPIC)

  t.ok(info.lastSeenAt >= seenAt, 'lastSeenAt refreshed')
  t.is(info.relayAddresses, relays, 'new relayAddresses win')

  dialer.seen({ publicKey: pk, relayAddresses: relays }, TOPIC)
  t.alike(info.topics.map(hex), [hex(TOPIC)], 'topic registered once')
  // stage 2: seenCount === 3 once sighting booking lands
})

test('peer reports eligible', async (t) => {
  const { dialer, dials, connected } = createDialer()
  t.teardown(() => dialer.destroy())
  if (!requireContract(t)) return

  const pk = b4a.alloc(32, 3)

  dialer.seen({ publicKey: pk }, TOPIC)
  await tick()
  t.is(dials.length, 1, 'fresh peer is eligible and dialed')

  connected.add(hex(pk)) // connection established
  dialer.seen({ publicKey: pk }, TOPIC)
  await tick()
  t.is(dials.length, 1, 'connected peer is not dialed')
})

test('peer is not attempted after 3 failed connections', async (t) => {
  const { dialer, dials, sessions } = createDialer()
  t.teardown(() => dialer.destroy())
  if (!requireContract(t)) return
  sessions.set(hex(TOPIC), {}) // topic stays announced

  const pk = b4a.alloc(32, 4)
  dialer.seen({ publicKey: pk }, TOPIC)
  await tick()
  t.is(dials.length, 1, 'initial dial')

  const info = dialer.peers.get(hex(pk))
  info.client = true // production: dialer.connected() sets this on the outbound link

  for (let i = 1; i <= 3; i++) {
    info._connected()
    dialer.disconnected(pk) // evicted < 15s -> attempts++
    t.is(info.attempts, i, `failed connection ${i} escalates`)
    t.ok(info.waiting, `ladder leg ${i} pending`)
    // stage 2: a sighting here must NOT dial once the waiting gate lands;
    // the legacy baseline redials (bypass) and is covered by rookie test 1
  }

  info._connected()
  dialer.disconnected(pk) // attempts = 4 -> ladder retires
  t.is(info.attempts, 4)
  t.is(info.waiting, false, 'no ladder leg left')
  t.ok(dialer.peers.get(hex(pk)) === undefined, 'retired record released')

  // legacy baseline: the next sighting rebirths the record at attempts=0 and
  // dials (the amnesia trigger). Flip these when the rebirth fix lands.
  dialer.seen({ publicKey: pk }, TOPIC)
  await tick()
  const reborn = dialer.peers.get(hex(pk))
  t.ok(reborn !== undefined, 'legacy baseline: sighting rebirths the record')
  t.is(reborn.attempts, 0)
  t.is(dials.length, 2, 'legacy baseline: rebirth dials')
})

test('peer is garbage collected on expiry', async (t) => {
  {
    const { dialer, connected, sessions } = createDialer()
    t.teardown(() => dialer.destroy())
    if (!requireContract(t)) return
    sessions.set(hex(TOPIC), {})

    const pk = b4a.alloc(32, 5)
    dialer.seen({ publicKey: pk }, TOPIC)
    await tick()
    connected.add(hex(pk)) // connection established

    dialer.collectGarbage()
    t.ok(dialer.peers.has(hex(pk)), 'live connection holds the record')

    connected.delete(hex(pk))
    dialer.collectGarbage()
    t.ok(dialer.peers.get(hex(pk)) === undefined, 'untracked record is collected')
  }
  {
    const { dialer, sessions } = createDialer()
    t.teardown(() => dialer.destroy())
    if (!requireContract(t)) return

    const pk = b4a.alloc(32, 6)
    sessions.set(hex(TOPIC), {})
    dialer.seen({ publicKey: pk }, TOPIC)
    await tick()
    const info = dialer.peers.get(hex(pk))
    info.client = true // production: dialer.connected() on the outbound link
    info._connected()
    dialer.disconnected(pk) // waiting=true (ladder leg)

    dialer.collectGarbage()
    t.ok(dialer.peers.has(hex(pk)), 'pending backoff holds the record')

    sessions.delete(hex(TOPIC)) // topic expires
    dialer.collectGarbage()
    t.ok(dialer.peers.get(hex(pk)) === undefined, 'expired topic releases the record')
  }
})
