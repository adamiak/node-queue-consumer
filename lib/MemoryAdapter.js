'use strict'

const Deferred = require('./Deferred')

module.exports = class MemoryAdapter {
  constructor () {
    this._queue = []
    this._borrowed = new Set()
    this._requests = []
  }

  push (message) {
    this._addMessage(message)
  }

  _addMessage (message) {
    if (this._requests.length < 1) {
      this._queue.push(message)
      return
    }
    const d = this._requests.shift()
    d.resolve(message)
  }

  fetch () {
    if (this._queue.length < 1) {
      const p = new Deferred()
      this._requests.push(p)
      return p
    }

    const message = this._queue.shift()
    this._borrowed.add(message)
    return Promise.resolve(message)
  }

  delete (message) {
    if (this.borrowed.delete(message)) {
      return Promise.resolve()
    }

    return Promise.reject(new Error('Unknown message'))
  }

  release (message) {
    if (this.borrowed.delete(message)) {
      this._addMessage(message)
      return Promise.resolve()
    }

    return Promise.reject(new Error('Unknown message'))
  }

}
