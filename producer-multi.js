var Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
  //'debug': 'all',
  'metadata.broker.list': 'localhost:9092',
  'group.id': 'node-rdkafka-consumer-flow-example',
  'enable.auto.commit': false,
});

var topic1Name = 'js-sample-1';
var topic2Name = 'js-sample-2';

consumer.on('event.log', function (log) {
  console.log(log);
});

consumer.on('event.error', function (err) {
  console.error('Error from consumer');
  console.error(err);
});

consumer.on('ready', function (arg) {
  console.log('consumer ready.' + JSON.stringify(arg));

  consumer.subscribe([topic1Name]);
  consumer.consume();
});

consumer.on('data', function (m) {
  consumer.commitMessage(m);
  try {
    const key = m.key && m.key.toString();

    console.log('data-' + key);

    if (!key) {
      return;
    }

    var producer = new Kafka.Producer({
      //'debug' : 'all',
      'metadata.broker.list': 'localhost:9092',
      dr_cb: true, //delivery report callback
    });

    producer.on('event.log', function (log) {
      console.log(log);
    });

    producer.on('event.error', function (err) {
      console.error('Error from producer');
      console.error(err);
    });

    var counter = 0;
    var maxMessages = 10;

    producer.on('delivery-report', function (err, report) {
      console.log('delivery-report: ' + report.topic + ',' + report.key.toString());
    });

    producer.on('ready', function (arg) {
      console.log('producer ready.' + JSON.stringify(arg));

      for (var i = 0; i < maxMessages; i++) {
        setTimeout((function(i) {
          return function() {
            console.log('value-' + i);
            var value = Buffer.from('value-' + i);
            var partition = -1;
            var headers = [{ header: 'header value' }];
            producer.produce(
              topic2Name,
              partition,
              value,
              key,
              Date.now(),
              '',
              headers
            );
          }
        })(i), 1000 * i);
      }

      var pollLoop = setTimeout(function () {
        producer.poll();
        if (counter === maxMessages) {
          clearTimeout(pollLoop);
          producer.disconnect();
        }
      }, 1000);
    });

    producer.on('disconnected', function (arg) {
      console.log('producer disconnected. ' + JSON.stringify(arg));
    });

    producer.connect();
  } catch (e) {
    console.error(e);
    return;
  }
});

consumer.on('disconnected', function (arg) {
  console.log('consumer disconnected. ' + JSON.stringify(arg));
});

consumer.connect();

setTimeout(function () {
  consumer.disconnect();
}, 1000 * 60 * 3);
