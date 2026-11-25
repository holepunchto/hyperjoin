const CODE_SUSPENDED = 'HYPERJOIN_SUSPENDED'
const CODE_DESTROYED = 'HYPERJOIN_DESTROYED'
const CODE_MAXCAP = 'MAXCAP'
const CODE_DUPLICATE = 'DUPLICATE'
const CODE_IDLE = 'IDLE'

class ReplicatorError extends Error {
  constructor(msg, code, fn = ReplicatorError) {
    super(`${code}: ${msg}`)
    this.code = code

    if (Error.captureStackTrace) Error.captureStackTrace(this, fn)
  }

  get name() {
    return 'ReplicatorError'
  }

  static ASSERTION(msg) {
    // bypass safetyCatch
    return new ReplicatorError(msg, 'ERR_ASSERTION', ReplicatorError.ASSERTION)
  }

  static SUSPENDED(msg = 'Replicator suspended') {
    return new ReplicatorError(msg, CODE_SUSPENDED, ReplicatorError.SUSPENDED)
  }

  static DESTROYED(msg = 'Replicator has been destroyed') {
    return new ReplicatorError(msg, CODE_DESTROYED, ReplicatorError.DESTROYED)
  }

  static DUPLICATE(msg = 'Duplicate connection') {
    return new ReplicatorError(msg, CODE_DUPLICATE, ReplicatorError.DUPLICATE)
  }

  static MAXCAP(msg = 'Capacity exhausted') {
    return new ReplicatorError(msg, CODE_MAXCAP, ReplicatorError.MAXCAP)
  }

  static IDLE(msg = 'Idle eviction') {
    return new ReplicatorError(msg, CODE_IDLE, ReplicatorError.IDLE)
  }

  static TOPIC_EXISTS(msg = 'Topic exists') {
    return new ReplicatorError(msg, 'TOPIC_EXISTS', ReplicatorError.TOPIC_EXISTS)
  }
}

module.exports = {
  ReplicatorError,
  CODE_SUSPENDED,
  CODE_DESTROYED,
  CODE_MAXCAP,
  CODE_DUPLICATE,
  CODE_IDLE
}
