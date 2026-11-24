require('dotenv').config();
const kafka = require('./kafka');
const pool = require('./db');

async function startConsumer() {
  const consumer = kafka.consumer({ groupId: 'inventory-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'tenant.orders', fromBeginning: true });
  console.log('Consumer connected, waiting for events on tenant.orders ...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const e = JSON.parse(message.value.toString());
      if (e.type !== 'OrderCreated') return;

      // inventory update, always scoped by tenant_id
      for (const i of e.items) {
        await pool.query(
          'UPDATE products SET stock = stock - $1 WHERE id = $2 AND tenant_id = $3',
          [i.qty, i.product_id, e.tenantId]
        );
      }
      await pool.query(
        "UPDATE orders SET status = 'PROCESSED' WHERE id = $1 AND tenant_id = $2",
        [e.orderId, e.tenantId]
      );

      console.log(
        `[OFFSET LOG] topic=${topic} partition=${partition} offset=${message.offset} ` +
        `key=${message.key?.toString()} order=${e.orderId} tenant=${e.tenantId}`
      );

      // Optional: serverless invoice worker (Vercel)
      if (process.env.INVOICE_URL) {
        try {
          const r = await fetch(`${process.env.INVOICE_URL}?orderId=${e.orderId}`);
          console.log('[INVOICE]', JSON.stringify(await r.json()));
        } catch (err) { console.error('invoice call failed', err.message); }
      }
    },
  });
}

module.exports = startConsumer;

if (require.main === module) {
  startConsumer().catch((e) => { console.error(e); process.exit(1); });
}
