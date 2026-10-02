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

  repStart(coreId) {
    let entry = this.cores.get(coreId)
    if (!entry) {
      entry = { used: 0 }
      this.cores.set(coreId, entry)
    }

    entry.used++

    return entry

    let known = false
    for (const session of this.sessions.values()) {
      if (!session.active) continue
      if (!session.hasCore(coreId)) continue

      known = true
      break
    }

    if (!known) console.log('repl-start', known, used)
    if (!known) return -1

    return used
  }

  repEnd(coreId) {
    const entry = this.cores.get(coreId)
    if (!entry) return -1

    if (!--entry.used) {
      this.cores.delete(coreId)
      this._oncorezero(entry)
    }

    return entry.used
  }

  _oncorezero() {
    // TODO: signal upstream "session-flushed"
    // let GC re-score non-pinned slots
  }
}
