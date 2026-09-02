\c analytics_db;

CREATE TABLE IF NOT EXISTS elastic_order_metrics (
    product_id VARCHAR(255) NOT NULL,
    total_revenue DOUBLE PRECISION DEFAULT 0.0,
    total_orders BIGINT DEFAULT 0,
    window_start TIMESTAMP(3) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (product_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_window ON elastic_order_metrics(window_start, window_end);

