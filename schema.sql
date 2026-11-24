DROP TABLE IF EXISTS order_items, orders, products, users, tenants CASCADE;

CREATE TABLE tenants (id SERIAL PRIMARY KEY, name TEXT);
CREATE TABLE users (id SERIAL PRIMARY KEY, tenant_id INT REFERENCES tenants(id), email TEXT UNIQUE, password TEXT, role TEXT DEFAULT 'admin');
CREATE TABLE products (id SERIAL PRIMARY KEY, tenant_id INT, name TEXT, stock INT DEFAULT 100000);
CREATE TABLE orders (id SERIAL PRIMARY KEY, tenant_id INT, status TEXT DEFAULT 'PENDING', created_at TIMESTAMP DEFAULT now());
CREATE TABLE order_items (id SERIAL PRIMARY KEY, order_id INT, product_id INT, qty INT);

INSERT INTO tenants(name) VALUES ('TenantA'), ('TenantB');
INSERT INTO users(tenant_id, email, password, role) VALUES
  (1, 'a@test.com', 'pass123', 'admin'),
  (2, 'b@test.com', 'pass123', 'admin');

INSERT INTO products(tenant_id, name) SELECT 1, 'A-Item' || g FROM generate_series(1,5) g;
INSERT INTO products(tenant_id, name) SELECT 2, 'B-Item' || g FROM generate_series(1,5) g;

-- 100k fake orders (NO index yet, so you can capture the "before" EXPLAIN ANALYZE)
INSERT INTO orders(tenant_id, status, created_at)
SELECT (random()*1 + 1)::int, 'PROCESSED', now() - (random()*365 || ' days')::interval
FROM generate_series(1, 100000);

INSERT INTO order_items(order_id, product_id, qty)
SELECT id, (random()*4 + 1)::int, 1 FROM orders;
