var Kafka = require('node-rdkafka');
var Transform = require('stream').Transform;
var uuid = require('uuid').v4;

var key = uuid();
console.log(key);

var topicName = 'js-sample-1';

var readStream = Kafka.KafkaConsumer.createReadStream(
  {
    'metadata.broker.list': 'localhost:9092',
    'group.id': key,
    'socket.keepalive.enable': true,
    'enable.auto.commit': false,
  },
  {},
  {
    topics: topicName,
    waitInterval: 0,
    objectMode: false,
  }
);

var transform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform(text, encoding, callback) {
    console.log(`pushing message ${text.toString()}`);
    callback(null, text);
  },
});

readStream.pipe(transform).pipe(process.stdout);

readStream.on('data', function (m) {
  console.log(JSON.stringify(m));
});

readStream.on('error', function (err) {
  console.log(err);
  process.exit(1);
});

readStream.consumer.on('event.error', function (err) {
  console.log(err);
});

var writeStream = Kafka.Producer.createWriteStream(
  {
    'metadata.broker.list': 'localhost:9092',
  },
  {},
  {
    topic: topicName,
  }
);

var queuedSuccess = writeStream.write(
  Buffer.from(JSON.stringify({ key: key, complete: false }))
);

if (queuedSuccess) {
  console.log('success : consumer');
} else {
  console.log('fail : consumer');
}