// A set kept sorted by `orderOf(item)`, lowest first.
//
// Contract:
// - call add(item) after every write that can change orderOf(item)
// - call delete(item) to release an item, the set holds its items strongly
module.exports = class OrderedSet {
  constructor(orderOf = (item) => -1) {
    this._orderOf = orderOf
    this._orders = new WeakMap() // item -> order it is sorted under
    this._sorted = []
  }

  get size() {
    return this._sorted.length
  }

  has(item) {
    return this._orders.has(item)
  }

  // insert, or move to where orderOf(item) now puts it
  add(item) {
    const order = this._orderOf(item)

    if (this._orders.has(item)) {
      if (this._orders.get(item) === order) return this
      this._sorted.splice(this._indexOf(item), 1)
    }

    this._orders.set(item, order)
    this._sorted.splice(this._upperBound(order), 0, item)

    return this
  }

  delete(item) {
    if (!this._orders.has(item)) return false

    this._sorted.splice(this._indexOf(item), 1)
    this._orders.delete(item)

    return true
  }

  clear() {
    this._orders = new WeakMap()
    this._sorted = []
  }

  // lowest ordered item
  first() {
    return this._sorted[0]
  }

  // copy of the items ordered at or below `order`
  upTo(order) {
    return this._sorted.slice(0, this._upperBound(order))
  }

  // first item ordered above `order`
  after(order) {
    return this._sorted[this._upperBound(order)]
  }

  // highest ordered item accepted by `test`
  findLast(test) {
    return this._sorted.findLast(test)
  }

  [Symbol.iterator]() {
    return this._sorted[Symbol.iterator]()
  }

  _indexOf(item) {
    let i = this._lowerBound(this._orders.get(item))
    while (this._sorted[i] !== item) i++
    return i
  }

  // first index ordered at or above `order`
  _lowerBound(order) {
    let lo = 0
    let hi = this._sorted.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this._orders.get(this._sorted[mid]) < order) lo = mid + 1
      else hi = mid
    }

    return lo
  }

  // first index ordered above `order`
  _upperBound(order) {
    let lo = 0
    let hi = this._sorted.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this._orders.get(this._sorted[mid]) <= order) lo = mid + 1
      else hi = mid
    }

    return lo
  }
}
