// returns sorted array
// lowest index equals least priority
function sortConnections(connections) {
  return connections.sort(tieredSort)

  function score(connection, other) {
    let w = 0

    // Keep outbound
    if (connection.initiator) w += 8

    // Keep established
    if (connection._survived > 0) w += 4

    // Keep recently active
    if (connection.lastActivity > other.lastActivity) w += 2

    // Keep oldest
    if (connection.connectedAt < other.connectedAt) w += 1

    // TODO: keep
    // - from pinned topic
    // - good throughput

    return w
  }

  function tieredSort(a, b) {
    const weightA = score(a, b)
    const weightB = score(b, a)
    return weightA - weightB
  }
}

module.exports = {
  sortConnections
}
