module.exports = {
  Memory: require('./lib/MemoryAdapter'),
  SQS: require('./lib/SQSAdapter'),
  Consumer: require('./lib/QueueConsumer')
}