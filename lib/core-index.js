module.exports = class CoreIndex {
  constructor() {
    // active
    this.sessions = new Set() // TODO: maybe use rep.sessions
    this.cores = new Map() // coreId => IndexEntry{ }
  }

  subscribe(dk, session) {
    this.sessions.add(session)
  }

  unsubscribe(dk, session) {
    this.sessions.delete(session)
  }

  repStart(coreId) {
    let used = 0
    let known = false
    for (const session of this.sessions.values()) {
      if (!session.active) continue
      if (!session.hasCore(coreId)) continue

      let entry = this.cores.get(coreId)
      if (!entry) {
        entry = { used: 0, session /* agent: no interact */ }
        this.cores.set(coreId, entry)
      }

      used = ++entry.used
      known = true
      break
    }

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
