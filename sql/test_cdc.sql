-- 快速测试CDC连接器语法
SET 'execution.checkpointing.interval' = '30s';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- 测试CDC表创建
CREATE TEMPORARY TABLE test_meter_reading (
    id BIGINT,
    device_id INT,
    reading_time TIMESTAMP(3),
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'flink',
    'password' = 'flink',
    'database-name' = 'power_grid',
    'schema-name' = 'public',
    'table-name' = 'meter_reading',
    'slot.name' = 'flink_slot'
);

-- 测试输出表
CREATE TEMPORARY TABLE test_output (
    device_id INT,
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2)
) WITH (
    'connector' = 'print'
);

-- 简单数据流测试
INSERT INTO test_output
SELECT device_id, voltage, current_val, power_val
FROM test_meter_reading
LIMIT 10;