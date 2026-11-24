// returns sorted array
// lowest index equals least priority
function sortConnections(connections, now = Date.now()) {
  return connections.sort(tieredSort)

  function score(connection, other) {
    let w = 0

    const peer = connection.info

    // Keep explicit scans
    if (peer.isScanning(now)) w += 1 << 4

    // Keep established
    if (connection._survived > 0) w += 1 << 3

    // Keep recently active
    if (connection.lastActivity > other.lastActivity) w += 1 << 2

    // Keep oldest
    if (connection.connectedAt < other.connectedAt) w += 1 << 1

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
