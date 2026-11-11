const test = require('brittle')
const { sortConnections } = require('../lib/heuristics')

const T0 = Date.now()

function stub(id, props = {}) {
  const { survived = 0, connectedAt = T0, lastActivity = T0, initiator = true } = props

  return { id, _survived: survived, connectedAt, lastActivity, initiator }
}

function ids(connections) {
  return connections.map((c) => c.id)
}

test('rookie sorted before established', (t) => {
  const sorted = sortConnections([stub(1, { survived: 1 }), stub(2, { survived: 0 })])

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

test('inbound rookie sorted before outbound rookie', (t) => {
  const sorted = sortConnections([stub(1, { initiator: true }), stub(2, { initiator: false })])

  t.alike(ids(sorted), [2, 1])
})

test('inbound established sorted before outbound established', (t) => {
  const sorted = sortConnections([
    stub(1, { survived: 1, initiator: true }),
    stub(2, { survived: 1, initiator: false })
  ])

  t.alike(ids(sorted), [2, 1])
})

test('established inbound sorted before rookie outbound', (t) => {
  const sorted = sortConnections([
    stub(1, { survived: 0, initiator: true }),
    stub(2, { survived: 1, initiator: false })
  ])

  t.alike(ids(sorted), [2, 1])
})

test('full eviction order', (t) => {
  const sorted = sortConnections([
    stub(1, { survived: 1, initiator: true }),
    stub(2, { survived: 0, initiator: true }),
    stub(3, { survived: 1, initiator: false }),
    stub(4, { survived: 0, initiator: false })
  ])

  t.alike(ids(sorted), [4, 3, 2, 1])
})
