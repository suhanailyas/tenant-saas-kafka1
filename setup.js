// npm run setup  -> creates tables + sample data in your Neon DB
require('dotenv').config();
const fs = require('fs');
const pool = require('./db');
(async () => {
  await pool.query(fs.readFileSync(__dirname + '/schema.sql', 'utf8'));
  console.log('Schema + seed data created (no indexes yet).');
  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
