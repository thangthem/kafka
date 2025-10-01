// producer/index.js
require('dotenv').config();
const { Kafka } = require('kafkajs');

const {
  KAFKA_BROKERS,
  KAFKA_CLIENT_ID = 'demo-producer',
  KAFKA_TOPIC,
} = process.env;

async function main() {
  console.log(`🚀 PRODUCER: Connect với broker: ${KAFKA_BROKERS}`);
  
  const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS.split(',').map(s => s.trim()),
    logLevel: require('kafkajs').logLevel.INFO,
  });

  const producer = kafka.producer();

  await producer.connect();
  console.log('✅ Producer connected!');

  // Gửi vài message để test auto-discovery
  const messages = [
    { key: 'test1', value: 'Message 1 - Testing auto-discovery' },
    { key: 'test2', value: 'Message 2 - Consumer sẽ tự tìm broker khác' },
    { key: 'test3', value: 'Message 3 - Kafka cluster discovery' },
    { key: 'test4', value: 'Message 4 - Chỉ cần 1 broker để bắt đầu' },
    { key: 'test5', value: 'Message 5 - Auto-discovery magic!' },
  ];

  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [message],
    });
    console.log(`📤 Sent: ${message.key} = ${message.value}`);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1s
  }

  await producer.disconnect();
  console.log('👋 Producer disconnected!');
}

main().catch(console.error);
