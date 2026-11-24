require('dotenv').config();
const { Kafka, logLevel } = require('kafkajs');

const config = {
  brokers: [process.env.KAFKA_BROKER],
  logLevel: logLevel.ERROR,
  ssl: { rejectUnauthorized: false },
};

if (process.env.KAFKA_USER) {
  config.sasl = {
    mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256',
    username: process.env.KAFKA_USER,
    password: process.env.KAFKA_PASS,
  };
}

module.exports = new Kafka({ clientId: 'tenant-saas', ...config });