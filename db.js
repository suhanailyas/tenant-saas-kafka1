require('dotenv').config();
const { Pool } = require('pg');

// Connection pooling (max 10 connections)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  ssl: { rejectUnauthorized: false },
});

module.exports = pool;
