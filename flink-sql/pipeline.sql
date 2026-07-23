-- 1. Tabla Origen Kafka
CREATE TABLE kafka_transactions (
    transaction_id STRING,
    user_id STRING,
    amount DOUBLE,
    event_type STRING,
    is_fraud INT,
    city STRING,
    device_type STRING,
    epoch_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(epoch_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'bank_transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_gold_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 2. Tabla Destino MySQL
CREATE TABLE fraud_metrics (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    user_id STRING,
    tx_count BIGINT,
    total_amount DOUBLE,
    fraud_count BIGINT,
    is_fraud_alert BOOLEAN
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/fraud_db?useSSL=false&serverTimezone=UTC',
    'table-name' = 'fraud_metrics',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '2s'
);

-- 3. Lógica de Negocio (Ventanas de 10 segundos)
INSERT INTO fraud_metrics
SELECT 
    window_start, window_end, user_id,
    COUNT(*) as tx_count,
    SUM(amount) as total_amount,
    SUM(is_fraud) as fraud_count,
    CASE WHEN SUM(is_fraud) > 0 OR COUNT(*) > 100 THEN TRUE ELSE FALSE END as is_fraud_alert
FROM TABLE(
    TUMBLE(TABLE kafka_transactions, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)
)
GROUP BY window_start, window_end, user_id;