const amqp = require('amqp-connection-manager')

const EXCHANGE_NAME = 'topic.research.test.1';

// Create 3 queues
// 2 queue listen to same clients registered topic
// 1 queue listen to transactions rejected topic 
const queues = [
  { queueName: 'clients.registered.sales.assignment', topic: 'clients.registered' },
  { queueName: 'clients.registered.email.notification', topic: 'clients.registered' },
  { queueName: 'clients.crm.transactions.rejected', topic: 'transactions.rejected' }
];


// Create a connetion manager
const connection = amqp.connect(['amqp://localhost']);
connection.on('connect', () => console.log('Connected!'));
connection.on('disconnect', err => console.log('Disconnected.', err));

// Set up a channel listening for messages in the queue.
queues.forEach(({ queueName, topic }) => {
  // Handle an incomming message.
  const onMessage = data => {
    var message = JSON.parse(data.content.toString());
    console.log("subscriber: got message", message);
    channelWrapper.ack(data);
  }

  var channelWrapper = connection.createChannel({
    setup: channel => Promise.all([
      channel.assertQueue(queueName, { exclusive: true, autoDelete: true }),
      channel.assertExchange(EXCHANGE_NAME, 'topic'),
      channel.prefetch(1),
      channel.bindQueue(queueName, EXCHANGE_NAME, topic),
      channel.consume(queueName, onMessage)
    ])
  });

  channelWrapper.waitForConnect()
    .then(function () {
      console.log("Listening for messages");
    });
});


