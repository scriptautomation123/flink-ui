CREATE TABLE kafka_orders (
  product_id STRING,
  amount DOUBLE,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-order-analytics',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE postgres_metrics (
  product_id STRING,
  total_revenue DOUBLE,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  PRIMARY KEY (product_id, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/analytics_db',
  'table-name' = 'elastic_order_metrics',
  'username' = 'postgres',
  'password' = 'password123',
  'driver' = 'org.postgresql.Driver'
);

INSERT INTO postgres_metrics
SELECT
  product_id,
  SUM(amount) AS total_revenue,
  HOP_START(order_time, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) AS window_start,
  HOP_END(order_time, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) AS window_end
FROM kafka_orders
GROUP BY
  product_id,
  HOP(order_time, INTERVAL '15' MINUTE, INTERVAL '1' HOUR);