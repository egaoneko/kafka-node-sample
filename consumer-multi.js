var Kafka = require('node-rdkafka');
var uuid = require('uuid').v4;

var topic1Name = 'js-sample-1';
var topic2Name = 'js-sample-2';
var key = uuid();
console.log(key);

var consumer = new Kafka.KafkaConsumer({
  //'debug': 'all',
  'metadata.broker.list': 'localhost:9092',
  'group.id': key,
  'enable.auto.commit': false,
});

consumer.on('event.log', function (log) {
  console.log(log);
});

consumer.on('event.error', function (err) {
  console.error('Error from consumer');
  console.error(err);
});

var counter = 0;
var numMessages = 5;

consumer.on('ready', function (arg) {
  console.log('consumer ready.' + JSON.stringify(arg));

  consumer.subscribe([topic2Name]);
  consumer.consume();

  try {
    var producer = new Kafka.Producer({
      //'debug' : 'all',
      'metadata.broker.list': 'localhost:9092',
    });

    producer.on('event.log', function (log) {
      console.log(log);
    });

    producer.on('event.error', function (err) {
      console.error('Error from producer');
      console.error(err);
    });

    producer.on('ready', function (arg) {
      console.log('producer ready.' + JSON.stringify(arg));

      var value = Buffer.from('start');
      var partition = -1;
      var headers = [{ header: 'header value' }];
      producer.produce(
        topic1Name,
        partition,
        value,
        key,
        Date.now(),
        '',
        headers
      );
    });

    producer.on('disconnected', function (arg) {
      console.log('producer disconnected. ' + JSON.stringify(arg));
    });

    producer.connect();
  } catch (e) {
    console.error(e);
  }
});

consumer.on('data', function (m) {
  const messageKey = m.key && m.key.toString();
  console.log('data-' + messageKey);

  if (messageKey !== key) {
    console.log('diff key' + '(' + messageKey + ',' + key + ')');
    return;
  }

  counter++;
  if (counter % numMessages === 0) {
    console.log('calling commit');
    consumer.commitMessage(m);
  }

  console.log('receive: ', m.value.toString());
});

consumer.on('disconnected', function (arg) {
  console.log('consumer disconnected. ' + JSON.stringify(arg));
});

consumer.connect();

setTimeout(function () {
  consumer.disconnect();
}, 1000 * 60 * 3);
