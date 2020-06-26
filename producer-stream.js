var Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
  //'debug': 'all',
  'metadata.broker.list': 'localhost:9092',
  'group.id': 'node-rdkafka-consumer-flow-example',
  'enable.auto.commit': false
});

var topicName = 'js-sample-1';

consumer.on('event.log', function(log) {
  console.log(log);
});

consumer.on('event.error', function(err) {
  console.error('Error from consumer');
  console.error(err);
});

consumer.on('ready', function(arg) {
  console.log('consumer ready.' + JSON.stringify(arg));

  consumer.subscribe([topicName]);
  consumer.consume();
});

consumer.on('data', function(m) {
  consumer.commitMessage(m);
  console.log(m.value.toString());

  try {
    const value = JSON.parse(m.value.toString());
    if (value.complete) {
      return;
    }

    var writeStream = Kafka.Producer.createWriteStream({
      'metadata.broker.list': 'localhost:9092'
    }, {}, {
      topic: topicName
    });
  
    var queuedSuccess = writeStream.write(
      Buffer.from(JSON.stringify({ key: value.key, complete: true }))
    );
  
    if (queuedSuccess) {
      console.log('success : producer');
    } else {
      console.log('fail : producer');
    }
  } catch(e) {
    console.error(e);
    return;
  }
});

consumer.on('disconnected', function(arg) {
  console.log('consumer disconnected. ' + JSON.stringify(arg));
});

consumer.connect();

setTimeout(function() {
  consumer.disconnect();
}, 1000 * 60 * 3);