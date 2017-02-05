# queue-consumer

Some abstractions over SQS

_very much wip but open sourced to stop bad habits_

## Usage

### Consumer

- `.start()` : initializes the consumer. Returns a promise that resolves once ready or rejects if unable to initiaise
- `.stop()` : gracefully stop the consumer. New and unresolved `fetch` calls will be rejected, consumer will wait for borrowed messages to be returned or deleted. Returns a promise that resolves once stopped.
- `.fetch()` : fetches a message from the queue . Returns a promise that resolves with a message or rejects upon any error. fetch calls can end up waiting indefinately unless the consumer is stopped.
- `.delete(message)` : deletes a message from the queue. `message` must object that consumer has previously issues via `fetch`. returns a promise that resolves once the consumer ackowledges it, rejects upon any error.
- `.release(message)` : returns a message to the queue so that it may be acquired again. returns a promise that resolves once accepted by the consumer and rejects upon any error

### Adapter

Adapters can optionally be eventEmitters and therefore may emit errors, these are not proxied through to the
`consumer`. 
At a minimum adapters must implement the following methods (using the same signatures as the `Consumer`)
- `fetch`,
- `delete`,
- `release` 

### SQS Adapter

The SQS adapter requires the name of an SQS queue to bind to and an AWS-SDK sqs client.
Once start is called it makes further calls to AWS to derive the queue's URL and attributes.

```
const AWS = require('aws-sdk')
const SQSAdapter = require('queue-consumer').SQS

const SQSClient = new AWS.SQS({
  region: 'eu-west-2'
})

const mySQSAdapter = new SQSAdapter('my-queue-things', SQSClient)
```

It currently does not support extending message timeouts so if you hold onto the message longer than you should SQS will requeue it without telling you (by default all messages are borrowed for 30 seconds).

`release`-ing a message will allow it to requeud by `SQS`, `delete`-ing will cause the message to be deleted from `SQS`.

It will emit `error`s if it experiences errors making API calls to SQS whilse fetching/deleting messages.


### Memory Adapter

The memory adapter exposes an additional `push` method allowing you to put messages on the queue.

```
const MemoryAdapter = require('queue-consumer').Memory
const myMemAdapter = new Memory()

const myMessage = {
  name: 'james',
  some: 'data'
}

myMemAdapter.push(myMessage)
```

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