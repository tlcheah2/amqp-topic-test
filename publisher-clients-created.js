const amqp = require('amqp-connection-manager')

const EXCHANGE_NAME = 'topic.research.test.1';
const topicToSend = 'clients.registered';
// const topicToSend = 'transactions.rejected';


// Create a connetion manager
const connection = amqp.connect(['amqp://localhost']);
connection.on('connect', () => console.log('Connected!'));
connection.on('disconnect', err => console.log('Disconnected.', err.stack));

// Create a channel wrapper
const channelWrapper = connection.createChannel({
  json: true,
  setup: channel => channel.assertExchange(EXCHANGE_NAME, 'topic')
});

// Send messages until someone hits CTRL-C or something goes wrong...
function sendMessage() {
  channelWrapper.publish(EXCHANGE_NAME, topicToSend, { client_id: 1, profile_status: 'active' }, { contentType: 'application/json', persistent: true })
    .then(function () {
      console.log("Message sent");
      // return wait(1000);
    })
    // .then(() => sendMessage())
    .catch(err => {
      console.log("Message was rejected:", err.stack);
      channelWrapper.close();
      connection.close();
    });
};

console.log("Sending messages...");
sendMessage();