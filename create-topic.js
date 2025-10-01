// create-topic.js - Script để tạo topic với replication factor cao
require('dotenv').config();
const { Kafka } = require('kafkajs');

const {
  KAFKA_BROKERS,
  KAFKA_TOPIC,
} = process.env;

async function createTopic() {
  console.log(`🔧 Tạo topic "${KAFKA_TOPIC}" với replication factor 3`);
  console.log(`📍 Connect với broker: ${KAFKA_BROKERS}`);
  
  const kafka = new Kafka({
    clientId: 'topic-creator',
    brokers: KAFKA_BROKERS.split(',').map(s => s.trim()),
    logLevel: require('kafkajs').logLevel.INFO,
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log('✅ Connected to Kafka cluster');

    // Tạo topic với replication factor 3 (cho 3 brokers)
    await admin.createTopics({
      topics: [{
        topic: KAFKA_TOPIC,
        numPartitions: 3, // 3 partitions
        replicationFactor: 3, // replicate trên tất cả 3 brokers
        configEntries: [
          {
            name: 'cleanup.policy',
            value: 'delete'
          }
        ]
      }]
    });

    console.log(`✅ Topic "${KAFKA_TOPIC}" đã được tạo thành công!`);
    console.log(`📊 Partitions: 3, Replication Factor: 3`);
    
    // Hiển thị metadata của topic
    const metadata = await admin.fetchTopicMetadata({ topics: [KAFKA_TOPIC] });
    console.log('\n📋 Topic Metadata:');
    console.log(JSON.stringify(metadata, null, 2));

  } catch (error) {
    if (error.type === 'TOPIC_ALREADY_EXISTS') {
      console.log(`⚠️  Topic "${KAFKA_TOPIC}" đã tồn tại`);
    } else {
      console.error('❌ Error creating topic:', error);
    }
  } finally {
    await admin.disconnect();
    console.log('👋 Disconnected from Kafka cluster');
  }
}

createTopic().catch(console.error);
