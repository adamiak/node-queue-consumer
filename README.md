# queue-consumer

Some abstractions over SQS

_very much wip but open sourced to stop bad habits_

## Usage

- `.start()` : initializes the consumer. Returns a promise that resolves once ready or rejects if unable to initiaise
- `.stop()` : gracefully stop the consumer. New and unresolved `fetch` calls will be rejected, consumer will wait for borrowed messages to be returned or deleted. Returns a promise that resolves once stopped.
- `.fetch()` : fetches a message from the queue . Returns a promise that resolves with a message or rejects upon any error. fetch calls can end up waiting indefinately unless the consumer is stopped.
- `.delete(message)` : deletes a message from the queue. `message` must object that consumer has previously issues via `fetch`. returns a promise that resolves once the consumer ackowledges it, rejects upon any error.
- `.release(message)` : returns a message to the queue so that it may be acquired again. returns a promise that resolves once accepted by the consumer and rejects upon any error

## Example

```
const QC = require('queue-consumer');
const AWS = require('aws-sdk')

const queueName = 'my-queue-of-stuff'
const SQS = new AWS.SQS({
  region: 'eu-west-2'
})


const adapter = new QC.SQS(queueName, SQS)
// or const adapter = new QC.Memory()

const consumer = new QC.Consumer(adapter)


consumer.start()
.then(()=>{
  return consumer.fetch()
})
.then((message)=>{
  // do something with the message
  // ..

  return consumer.delete(message)
})
.then(()=>{
  // the message is now deleted
})

// some point later
consumer.stop()

```