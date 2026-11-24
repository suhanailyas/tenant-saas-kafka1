require('dotenv').config();
const express = require('express');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const path = require('path');
const pool = require('./db');
const kafka = require('./kafka');

const app = express();
app.set('trust proxy', 1);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/api/', rateLimit({ windowMs: 60 * 1000, max: 300 }));

const producer = kafka.producer();
let producerReady = producer.connect().then(() => console.log('Kafka producer connected'))
  .catch((e) => console.error('Kafka producer error:', e.message));

app.get('/health', (req, res) => res.json({ ok: true }));

// ---------- Auth ----------
app.post('/api/v1/login', async (req, res) => {
  const { email, password } = req.body || {};
  const r = await pool.query('SELECT * FROM users WHERE email=$1 AND password=$2', [email, password]);
  const u = r.rows[0];
  if (!u) return res.status(401).json({ error: 'Invalid credentials' });
  const token = jwt.sign({ tenant_id: u.tenant_id, role: u.role, email: u.email },
    process.env.JWT_SECRET, { expiresIn: '2h' });
  res.json({ token, tenant_id: u.tenant_id });
});

// ---------- Tenant resolution middleware ----------
// tenant_id comes ONLY from the verified JWT claim (cannot be spoofed by a header)
app.use('/api/v1', (req, res, next) => {
  if (req.path === '/login') return next();
  try {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    const claims = jwt.verify(token, process.env.JWT_SECRET);
    req.tenantId = claims.tenant_id;
    req.role = claims.role;
    next();
  } catch {
    res.status(401).json({ error: 'Unauthorized' });
  }
});

// ---------- Products (tenant scoped) ----------
app.get('/api/v1/products', async (req, res) => {
  const r = await pool.query('SELECT id, name, stock FROM products WHERE tenant_id=$1 ORDER BY id', [req.tenantId]);
  res.json(r.rows);
});

// ---------- Order producer: 202 Accepted + Kafka event ----------
app.post('/api/v1/orders', async (req, res) => {
  try {
    const items = (req.body && req.body.items) || [];
    if (!items.length) return res.status(400).json({ error: 'items required' });

    const o = await pool.query('INSERT INTO orders(tenant_id) VALUES($1) RETURNING id', [req.tenantId]);
    const orderId = o.rows[0].id;
    for (const i of items) {
      await pool.query('INSERT INTO order_items(order_id, product_id, qty) VALUES($1,$2,$3)',
        [orderId, i.product_id, i.qty]);
    }

    await producerReady;
    // key = tenant_id -> all events of one tenant go to the same partition (ordering guarantee)
    await producer.send({
      topic: 'tenant.orders',
      messages: [{
        key: String(req.tenantId),
        value: JSON.stringify({ type: 'OrderCreated', orderId, tenantId: req.tenantId, items }),
      }],
    });

    res.status(202).json({ status: 'accepted', orderId });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ---------- BAD: N+1 query problem ----------
app.get('/api/v1/orders/slow', async (req, res) => {
  const t = Date.now();
  const orders = (await pool.query(
    'SELECT * FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 200', [req.tenantId])).rows;
  for (const o of orders) {
    o.items = (await pool.query('SELECT * FROM order_items WHERE order_id=$1', [o.id])).rows; // 1 query per order
  }
  res.json({ method: 'N+1', queries: orders.length + 1, ms: Date.now() - t, count: orders.length, orders });
});

// ---------- GOOD: single JOIN ----------
app.get('/api/v1/orders/fast', async (req, res) => {
  const t = Date.now();
  const r = await pool.query(
    `SELECT o.id, o.status, o.created_at,
            COALESCE(json_agg(json_build_object('product_id', oi.product_id, 'qty', oi.qty))
                     FILTER (WHERE oi.id IS NOT NULL), '[]') AS items
     FROM (SELECT * FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 200) o
     LEFT JOIN order_items oi ON oi.order_id = o.id
     GROUP BY o.id, o.status, o.created_at
     ORDER BY o.created_at DESC`, [req.tenantId]);
  res.json({ method: 'JOIN', queries: 1, ms: Date.now() - t, count: r.rows.length, orders: r.rows });
});

// ---------- Viva helper: run EXPLAIN ANALYZE for the active tenant ----------
app.get('/api/v1/explain', async (req, res) => {
  const r = await pool.query(
    'EXPLAIN ANALYZE SELECT * FROM orders WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 50', [req.tenantId]);
  res.type('text/plain').send(r.rows.map((x) => x['QUERY PLAN']).join('\n'));
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log('API listening on', port);
  if (process.env.RUN_CONSUMER === 'true') require('./consumer')().catch(console.error);
});
