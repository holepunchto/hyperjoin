const test = require('brittle')
const { sortConnections } = require('../lib/heuristics')

const T0 = Date.now()

function stub(id, props = {}) {
  // prettier-ignore
  const {
    survived = 0,
    connectedAt = T0,
    lastActivity = T0,
    scannedAt = -1
  } = props

  return { id, _survived: survived, connectedAt, lastActivity, info: { scannedAt } }
}

function ids(connections) {
  return connections.map((c) => c.id)
}

test('rookie sorted before established', (t) => {
  // prettier-ignore
  const sorted = sortConnections([
    stub(1, { survived: 1 }),
    stub(2, { survived: 0 })
  ])

  t.alike(ids(sorted), [2, 1])
})

test('newest rookie sorted first', (t) => {
  const sorted = sortConnections([
    stub(1, { connectedAt: T0 }),
    stub(2, { connectedAt: T0 + 1000 })
  ])

  t.alike(ids(sorted), [2, 1])
})

test('stalest established sorted first', (t) => {
  const sorted = sortConnections([
    stub(1, { survived: 1, lastActivity: T0 + 1000 }),
    stub(2, { survived: 1, lastActivity: T0 })
  ])

  t.alike(ids(sorted), [2, 1])
})

test('unscanned sorted before scanned', (t) => {
  // prettier-ignore
  const sorted = sortConnections([
    stub(1, { scannedAt: T0 }),
    stub(2)
  ], T0)

  t.alike(ids(sorted), [2, 1])
})

test('established sorted before scanned rookie', (t) => {
  // prettier-ignore
  const sorted = sortConnections([
    stub(1, { scannedAt: T0 }),
    stub(2, { survived: 1 })
  ], T0)

  t.alike(ids(sorted), [2, 1])
})

test('scan expires after a minute', (t) => {
  // prettier-ignore
  const sorted = sortConnections([
    stub(1, { survived: 1 }),
    stub(2, { scannedAt: T0 - 60000 })
  ], T0)

  t.alike(ids(sorted), [2, 1])
})

test('full eviction order', (t) => {
  const sorted = sortConnections(
    [
      stub(1, { survived: 1, scannedAt: T0 }),
      stub(2, { survived: 0, scannedAt: T0 }),
      stub(3, { survived: 1 }),
      stub(4, { survived: 0 })
    ],
    T0
  )

  t.alike(ids(sorted), [4, 3, 2, 1])
})
