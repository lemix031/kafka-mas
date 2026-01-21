/*
BIGSERIAL je autoinkrement vrednost
TIMESTAMPTZ je timestamp with timezone
*/
CREATE TABLE IF NOT EXISTS inventory_events(
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    product_name TEXT NOT NULL,
    quantity INT NOT NULL,
    reason TEXT,
    raw_event JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS indx_inventory_events_order_id ON inventory_events(order_id);
CREATE INDEX IF NOT EXISTS indx_inventory_events_created_at ON inventory_events(created_at);

CREATE TABLE IF NOT EXISTS order_status(
    order_id TEXT PRIMARY KEY,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    product_name TEXT NOT NULL,
    quantity INT NOT NULL,
    reason TEXT
);