'use strict'

const Deferred = require('./Deferred')

module.exports = class MessageLoan extends Deferred {
  constructor (message) {
    super()
    this.message = message
  }
}
