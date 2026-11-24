-- Run AFTER taking the "before" screenshot
CREATE INDEX idx_orders_tenant_created ON orders (tenant_id, created_at DESC);
CREATE INDEX idx_items_order ON order_items (order_id);
ANALYZE orders;
ANALYZE order_items;
