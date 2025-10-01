// index.js
require('dotenv').config();
const { Kafka, logLevel } = require('kafkajs');

const {
  KAFKA_BROKERS,
  KAFKA_CLIENT_ID,
  KAFKA_GROUP_ID,
  KAFKA_TOPIC,
} = process.env;

async function main() {
  console.log(`🔍 CHỨNG MINH AUTO-DISCOVERY: Chỉ connect 1 broker: ${KAFKA_BROKERS}`);
  
  const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS.split(',').map(s => s.trim()),
    logLevel: logLevel.INFO,
    // Nếu bạn dùng SASL/SSL, bật thêm cấu hình dưới (ví dụ SASL PLAIN):
    // ssl: true,
    // sasl: { mechanism: 'plain', username: 'user', password: 'pass' },
  });

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

  // Log để theo dõi auto-discovery
  consumer.on(consumer.events.GROUP_JOIN, e => {
    console.log(`\n🎯 [AUTO-DISCOVERY] Consumer đã join group:`, {
      groupId: KAFKA_GROUP_ID,
      memberId: e.payload.memberId,
      leader: e.payload.isLeader,
      generationId: e.payload.generationId,
    });
  });

  consumer.on(consumer.events.CONNECT, e => {
    console.log(`\n🔗 [CONNECT] Connected to broker:`, e.payload);
  });

  consumer.on(consumer.events.DISCONNECT, e => {
    console.log(`\n❌ [DISCONNECT] Disconnected from broker:`, e.payload);
  });

  process.on('SIGINT', async () => {
    console.log('SIGINT nhận được. Đang đóng consumer...');
    await consumer.disconnect();
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    console.log('SIGTERM nhận được. Đang đóng consumer...');
    await consumer.disconnect();
    process.exit(0);
  });

  console.log(`\n🚀 Đang connect chỉ với broker: ${KAFKA_BROKERS}`);
  await consumer.connect();

  // subscribe topic
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

  console.log(`▶️  Đang lắng nghe topic "${KAFKA_TOPIC}" tại ${KAFKA_BROKERS} ...`);
  console.log(`\n💡 KAFKA AUTO-DISCOVERY: Consumer sẽ tự động tìm và kết nối với các broker khác trong cluster!`);

  // chạy loop nhận message
  await consumer.run({
    autoCommit: true, // để đơn giản; thực tế có thể tự commit thủ công
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      const key = message.key?.toString();
      const value = message.value?.toString();
      const offset = message.offset;
      const ts = message.timestamp;

      console.log(`\n📨 [MESSAGE] topic=${topic} partition=${partition} offset=${offset} ts=${ts}
  key=${key}
  value=${value}
  🎯 Broker discovery đã hoạt động - message từ partition ${partition}!`);

      // Nếu xử lý lâu, hãy gọi heartbeat() định kỳ để tránh bị rebalance:
      // await heartbeat();
    },
  });
}

main().catch(async (err) => {
  console.error('Consumer lỗi:', err);
  process.exit(1);
});
