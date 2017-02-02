'use strict'

/**
 * A basic abstraction over queues intended for use by consumers
 * - this file is mostly an interface (in a language that does not have them - lol)
 */

module.exports = class QueueConsumer {

  constructor (adapter) {
    this._adapter = adapter
  }

  /**
   * Initialises the underlying adapter
   * @return {Promise} [description]
   */
  start () {
    if (typeof this._adapter.start === 'function') {
      return this._adapter.start()
    }
    return Promise.resolve()
  }

  /**
   * stops the underlying adapter
   * adapters should stop gracefully...
   * @return {Promise} [description]
   */
  stop () {
    if (typeof this._adapter.stop === 'function') {
      return this._adapter.stop()
    }
    return Promise.resolve()
  }

  /**
   * request a message from the queue.
   * Generally it blocks until a message is ready
   * @return {Promise} either resolves to message or null, or rejects with error
   */
  fetch () {
    return this._adapter.fetch()
  }

  /**
   * delete a message from the queue
   * @param  {[type]} message [description]
   * @return {Promise}         [description]
   */
  delete (message) {
    return this._adapter.delete(message)
  }

  /**
   * release a message back to the queue
   * @param  {[type]} message [description]
   * @return {Promise}         resolves once the message is deleted
   */
  release (message) {
    return this._adapter.release(message)
  }

}
