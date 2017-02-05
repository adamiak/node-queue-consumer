'use strict'

const EventEmitter = require('events')

const Deferred = require('./Deferred')
const MessageLoan = require('./MessageLoan')

const DEFAULT_WAIT = 20
const DEFAULT_VISIBILITY_TIMEOUT = 30
// most SQS batch operations are limited to 10 items
const DEFAULT_SQS_MAX_BATCH_SIZE = 10

/**
 * SQS Required Permissions
 * - sqs:GetQueueUrl
 * - sqs:GetQueueAttributes
 * - sqs:ReceiveMessage
 * - sqs:DeleteMessageBatch
 * // TODO:
 * - sqs:ChangeMessageVisibilityBatch
 */

module.exports = class SQSAdapter extends EventEmitter {

  constructor (queueName, sqsClient) {
    super()

    if (typeof queueName !== 'string') {
      throw new Error('Missing SQS consumer option [ queueName ].')
    }

    if (!sqsClient) {
      throw new Error('Missing SQS consumer option [ sqsClient ].')
    }

    /**
     * The queue name as known to AWS
     * @type {String}
     */
    this._queueName = queueName

    /**
     * AWS-SDK SQS instance
     * @type {[type]}
     */
    this._client = sqsClient

    /**
     * resolved URL to the queue
     * filled in at start time via API call
     * @type {String || null}
     */
    this._queueUrl = null

    /**
     * resolved queue attributes
     * filled in at start time via API call
     * @type {Object || null}
     */
    this._queueAttributes = null

    /**
     * Are we running?
     * @type {Boolean}
     */
    this._running = false

    /**
     * holder for _run setImmediate handle
     * @type {Immediate || null}
     */
    this._scheduledRun = null

    /**
     * Are we going through a cool down
     * @type {Boolean}
     */
    this._coolDown = false

    /**
     * Any recieveMessage requests in progress
     * - use this to cancel the requests
     * @type {AWS.Request || null}
     */
    this._receiveRequests = new Set()

    /**
     * Promise representing an outstanding receiveMessage operation
     * @type {Promise || null}
     */
    this._receiveOperations = new Set()

    /**
     * A deleteMessage requests in progress
     * - use this to cancel the requests
     * @type {AWS.Request || null}
     */
    this._deleteRequests = new Set()

    /**
     * Promise representing a delete message operation
     * @type {Promise || null}
     */
    this._deleteOperations = new Set()

    /**
     * Store for any messages we currently have regardless of state
     * @type {Set}
     */
    this._messages = new Set()

    /**
     * Store for any message we currently have loaned out
     * @type {Set}
     */
    this._borrowedMessages = new Set()

    /**
     * Store for any messages that are awaiting deletion
     * @type {Set}
     */
    this._messagesToBeDeleted = new Set()

    /**
     * Store for any messages that are currently being deleted
     * @type {Set}
     */
    this._messagesInDeletion = new Set()

    /**
     * Messages awaiting dispatch to fetch callers
     * @type {Set}
     */
    this._newMessages = new Set()

    /**
     * a reverse lookup of parsed message body -> SQSMessage
     * @type {WeakMap}
     */
    this._bodyToMessageLookup = new WeakMap()

    /**
     * MessageLoan keyed by the deserialised message body
     * @type {Map}
     */
    this._messageLoans = new Map()

    /**
     * requests for messages from userland
     * @type {Array}
     */
    this._waitingFetchCalls = []
  }

  /**
   * Initialise the consumer
   * @return {Promise} resolves when queue is ready, rejects if unable to start
   */
  start () {
    if (this._running === true) {
      return
    }

    return this._getQueueUrl()
    .then(() => {
      return this._getQueueAttributes()
    })
    .then(() => {
      this._running = true
    })
  }

  /**
   * Stops the transport
   * @return {Promise} resolves once the transport has finished any outstanding work
   */
  stop () {
    this._running = false

    // Wait for all operations to drain out...
    // - cancel outstanding aws-recieve calls
    // - reject outstanding fetch calls
    // - wait for outstanding loans to finish
    // - wait for deleted message to deleted from sqs
    // - resolve
    //
    // TODO: what about any outstanding scheduledRun?

    this._receiveRequests.forEach((req) => {
      req.abort()
    })

    this._waitingFetchCalls.forEach((req) => {
      req.reject(new Error('Consumer is stopping'))
    })

    return Promise.resolve()
    .then(() => {
      const recievePromises = Array.from(this._receiveOperations.values()).map(reflect)
      return Promise.all(recievePromises)
    })
    .then(() => {
      const loanPromises = Array.from(this._messageLoans.values()).map((loan) => loan.promise)
      return Promise.all(loanPromises)
    })
    .then(() => {
      const deletePromises = Array.from(this._deleteOperations.values()).map(reflect)
      return Promise.all(deletePromises)
    })
  }

  fetch () {
    if (this._running === false) {
      return Promise.reject(new Error('Consumer is not running'))
    }

    const r = new Deferred()
    this._waitingFetchCalls.push(r)

    this._scheduleRun()

    return r.promise
  }

  delete (messageBody) {
    if (this._running === false) {
      return Promise.reject(new Error('Consumer is not running'))
    }

    const loan = this._messageLoans.get(messageBody)

    if (loan === undefined) {
      return Promise.reject(new Error('Unknown message'))
    }

    this._messageLoans.delete(messageBody)

    const SQSMessage = loan.message

    this._borrowedMessages.delete(SQSMessage)
    this._messagesToBeDeleted.add(SQSMessage)

    this._scheduleRun()

    return loan.resolve()
  }

  release (messageBody) {
    if (this._running === false) {
      return Promise.reject(new Error('Consumer is not running'))
    }

    const loan = this._messageLoans.get(messageBody)

    if (loan === undefined) {
      return Promise.reject(new Error('Unknown message'))
    }

    this._messageLoans.delete(messageBody)

    const SQSMessage = loan.message

    this._borrowedMessages.delete(SQSMessage)
    this._messages.delete(SQSMessage)
    // TODO: for now we just let the message handle expire
    // we should try to return it to SQS?
    // - ChangeMessageVisibility -> 0
    this._scheduleRun()

    return loan.resolve()
  }

  /**
   * Try to schedule deleting messages
   *
   * @return {Boolean} [description]
   */
  _scheduleDeletionRun () {
    // only one delete operation allowed at a time for now
    if (this._deleteOperations.size > 1) {
      return
    }

    const numMessagesToBeDeleted = Math.min(this._messagesToBeDeleted.size, DEFAULT_SQS_MAX_BATCH_SIZE)

    if (numMessagesToBeDeleted < 1) {
      return
    }

    // This is some fugly code - there must be a better ES6
    // to pull a limited number of messages from a set - or maybe we should use
    // another collection type like an array?
    const messagesToBin = []

    for (const messageToBeDeleted of this._messagesToBeDeleted) {
      if (messagesToBin.length >= 10) {
        break
      }

      this._messagesInDeletion.add(messageToBeDeleted)
      this._messagesToBeDeleted.delete(messageToBeDeleted)
      messagesToBin.push(messageToBeDeleted)
    }

    const deleteOperation = this._delete(messagesToBin)
    .then((result) => {
      result.successful.forEach((SQSMessage) => {
        this._messagesInDeletion.delete(SQSMessage)
        this._messages.delete(SQSMessage)
      })

      // rollback state changes
      result.failed.forEach((SQSMessage) => {
        this._messagesInDeletion.delete(SQSMessage)
        this._messagesToBeDeleted.add(SQSMessage)
      })

      this._deleteOperations.delete(deleteOperation)
      this._scheduleRun()
    })
    .catch((err) => {
      // rollback state changes
      messagesToBin.forEach((SQSMessage) => {
        this._messagesInDeletion.delete(SQSMessage)
        this._messagesToBeDeleted.add(SQSMessage)
      })

      this._deleteOperations.delete(deleteOperation)
      // TODO: wrap or customise the name of the event.
      this.emit('error', err)
    })

    this._deleteOperations.add(deleteOperation)
  }

  _scheduleReceiveMessageRun () {
    const numMessagesWanted = this._waitingFetchCalls.length - this._newMessages.size

    // if we don't need to fetch any messages then just stop
    if (numMessagesWanted < 1) {
      return
    }

    // For now don't allow more than one inflight receive operation...
    // this is an extra level of state tracking pain we don't need right now
    if (this._receiveOperations.size > 0) {
      return
    }

    const receiveOperation = this._receiveMessages(numMessagesWanted)
    .then((SQSMessages) => {
      SQSMessages.forEach((SQSMessage) => {
        this._messages.add(SQSMessage)
        this._newMessages.add(SQSMessage)
      })
      this._receiveOperations.delete(receiveOperation)
      this._scheduleRun()
    })
    .catch((err) => {
      this._receiveOperations.delete(receiveOperation)
      // TODO: wrap or customise the name of the event.
      this.emit('error', err)
    })

    this._receiveOperations.add(receiveOperation)
  }

  /**
   * Attempts to handout messages to any waiting fetch calls
   * @return {[type]} [description]
   */
  _handout () {
    // no waiting callers - bail
    if (this._waitingFetchCalls.length < 1) {
      return
    }
    // no messages to handout - bail
    if (this._newMessages.size < 1) {
      return
    }

    // Get first message - this is probably not the most idiomatic way to
    // use an iterator
    const iteration = this._newMessages.values().next()

    // should not happen...!
    if (iteration.done === true) {
      return
    }
    const SQSMessage = iteration.value

    let body = {}

    try {
      body = JSON.parse(SQSMessage.Body)
    } catch (err) {
      // TODO: what to do now?
      // just drop the message and let it get reclaimed by SQS
      console.log(SQSMessage.Body, err)
      this._newMessages.delete(SQSMessage)
      this._messages.delete(SQSMessage)
      return
    }

    const loan = new MessageLoan(SQSMessage)
    this._messageLoans.set(body, loan)

    this._newMessages.delete(SQSMessage)
    this._borrowedMessages.add(SQSMessage)

    const callerDeferred = this._waitingFetchCalls.shift()

    // TODO: unpack SQS messages record lookup from deserialised body -> SQS
    callerDeferred.resolve(body)
  }

  /**
   * Schedule the fetch-work-delete operation
   * @param  {Number} delay [how many ms in the future to schedule the next run.
   * @return {[type]}       [description]
   */
  _scheduleRun (delay) {
    if (this._running === false) {
      return
    }

    if (this._scheduledRun !== null) {
      return
    }

    // TODO: extract out to instance method?
    const goDoIt = () => {
      this._dispense()
    }

    if (delay !== undefined && delay > 0) {
      this._coolDown = true
      this._scheduledRun = setTimeout(goDoIt, delay)
    } else {
      this._scheduledRun = setImmediate(goDoIt)
    }
  }

  /**
   * Stop the main-loop being run next event-loop
   * @return {[type]} [description]
   */
  _descheduleRun () {
    if (this._coolDown === true) {
      this._coolDown = false
      clearTimeout(this._scheduledRun)
    } else {
      clearImmediate(this._scheduledRun)
    }

    this._scheduledRun = null
  }

  /**
   * TODO: rename this
   * this block should be synchronous
   */
  _dispense () {
    // Calling this just to clean-up
    this._descheduleRun()

    // Do nothing if we should be stopped
    // should be impossible to get here, but lets play it safe
    if (this._running === false) {
      return
    }

    this._scheduleDeletionRun()
    this._scheduleReceiveMessageRun()
    this._handout()

    return
  }

  /**
   * Delete some messages from SQS...
   * @param  {Array} SQSMessages An array of SQS messages to be deleted
   * @return {Promise}            resolves to {failed:[SQSMessages], successful:[SQSMessages]}
   */
  _delete (SQSMessages) {
    // temporary internal idx to handle response mapping
    const idToMessageIdx = new Map()
    const entries = []

    SQSMessages.forEach((SQSMessage, idx) => {
      const id = `M-${idx}-${Date.now()}` // TODO: this is very bad id generation....
      entries.push({
        Id: id,
        ReceiptHandle: SQSMessage.ReceiptHandle
      })
      idToMessageIdx.set(id, SQSMessage)
    })

    const deleteRequest = this._client.deleteMessageBatch({
      QueueUrl: this._queueUrl,
      Entries: entries
    })

    this._deleteRequests.add(deleteRequest)

    return deleteRequest.promise()
    .then((data) => {
      this._deleteRequests.delete(deleteRequest)

      return {
        successful: data.Successful.map((res) => { return idToMessageIdx.get(res.id) }),
        failed: data.Failed.map((res) => { return idToMessageIdx.get(res.id) })
      }
    }, (err) => {
      this._deleteRequests.delete(deleteRequest)
      return Promise.reject(err)
    })
  }

  /**
   * polls and fetches a messages from the queue
   * @return {Promise} resolves either to messages or null if no message retrieved, rejects if error
   */
  _receiveMessages (numMessagesWanted) {
    const numMessagesToFetch = Math.min(numMessagesWanted, DEFAULT_SQS_MAX_BATCH_SIZE)

    const receiveRequest = this._client.receiveMessage({
      QueueUrl: this._queueUrl,
      WaitTimeSeconds: DEFAULT_WAIT,
      MaxNumberOfMessages: numMessagesToFetch,
      VisibilityTimeout: DEFAULT_VISIBILITY_TIMEOUT
    })

    this._receiveRequests.add(receiveRequest)

    return receiveRequest.promise()
    .then((data) => {
      this._receiveRequests.delete(receiveRequest)
      return data.Messages || []
    }, (err) => {
      this._receiveRequests.delete(receiveRequest)
      // Treat user abort like we found nothing
      // and let our existing logic handle it
      if (err.name === 'RequestAbortedError') {
        return []
      }
      return Promise.reject(err)
    })
  }

  /**
   * Fetches the queue url and stores internally
   * @return {Promise} resolves once completed, rejects if unable to fetch queuename
   */
  _getQueueUrl () {
    const request = this._client.getQueueUrl({
      QueueName: this._queueName
    })

    return request.promise().then((data) => {
      this._queueUrl = data.QueueUrl
    })
  }

  /**
   * Fetches the queue attributes and stores them
   * @return {Promise} resolves once completed, rejects if unable to fetch attributes
   */
  _getQueueAttributes () {
    const request = this._client.getQueueAttributes({
      AttributeNames: ['All'],
      QueueUrl: this._queueUrl
    })

    return request.promise().then((data) => {
      this._queueAttributes = data.Attributes
    })
  }

}

function reflect (promise) {
  function noop () {}
  return promise.then(noop, noop)
}
