const { toHex } = require('b4a')

module.exports = class CoreIndex {
  constructor() {
    // active
    this.sessions = new Set() // TODO: maybe use rep.sessions
    this.cores = new Map() // coreId => IndexEntry{ }
  }

  subscribe(dk, session, dbgId) {
    console.log(dbgId, 'coredex-sub', session.store.cores.size /*global*/)
    this.sessions.add(session)

    const logger = (core) => console.log(dbgId, 'store-open', toHex(core.discoveryKey).slice(0, 6))
    /* global */
    session.store.watch(logger)
  }

  unsubscribe(dk, session, dbgId) {
    this.sessions.delete(session)
  }

  opened(connection, dk) {
    let entry = this.cores.get(dk)
    if (!entry) {
      entry = { used: 0, connections: [] }
      this.cores.set(dk, entry)
    }

    let known = false

    for (const session of this.sessions.values()) {
      if (!session.active) throw new Error('assert fail 1')

      if (!session.active) continue
      if (!session.hasCore(dk)) continue

      entry.used++
      known = true
      break
    }

    if (!known) throw new Error('assert fail 2')

    if (!known) return -1

    return entry.used
  }

  closed(connection, dk) {
    const entry = this.cores.get(dk)
    if (!entry) return -1

    if (!--entry.used) {
      this.cores.delete(dk)
      this._oncorezero(entry)
    }

    return entry.used
  }

  _oncorezero() {
    // TODO: signal upstream "session-flushed"
    // let GC re-score non-pinned slots
  }

  isActive(connection) {
  }
}
