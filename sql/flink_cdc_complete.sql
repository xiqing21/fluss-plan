-- 国网实时数仓完整Flink SQL作业 - 使用CDC连接器
SET 'execution.checkpointing.interval' = '30s';

-- 创建CDC源表
CREATE TABLE ods_meter_reading (
    id BIGINT,
    device_id INT,
    reading_time TIMESTAMP(3),
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    created_at TIMESTAMP(3),
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

CREATE TABLE ods_device_info (
    id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_id INT,
    manufacturer STRING,
    model STRING,
    install_date DATE,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'flink',
    'password' = 'flink',
    'database-name' = 'power_grid',
    'schema-name' = 'public',
    'table-name' = 'device_info',
    'slot.name' = 'flink_slot2'
);

CREATE TABLE ods_substation (
    id INT,
    name STRING,
    location STRING,
    voltage_level INT,
    capacity DECIMAL(10,2),
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'flink',
    'password' = 'flink',
    'database-name' = 'power_grid',
    'schema-name' = 'public',
    'table-name' = 'substation',
    'slot.name' = 'flink_slot3'
);

-- DWD层输出表
CREATE TABLE dwd_device_detail_output (
    device_id INT,
    device_code STRING,
    device_name STRING,
    substation_name STRING,
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- DWS层窗口汇总输出表
CREATE TABLE dws_device_summary_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    record_count BIGINT,
    avg_voltage DECIMAL(8,2),
    max_voltage DECIMAL(8,2),
    min_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    layer_info STRING
) WITH (
    'connector' = 'print'
);

-- ADS层应用输出表
CREATE TABLE ads_realtime_monitor_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    latest_voltage DECIMAL(8,2),
    latest_temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- DWD层：明细数据处理
INSERT INTO dwd_device_detail_output
SELECT 
    m.device_id,
    d.device_code,
    d.device_name,
    s.name as substation_name,
    m.voltage,
    m.current_val,
    m.power_val,
    m.temperature,
    -- 健康度评分
    CASE 
        WHEN m.voltage BETWEEN 200 AND 240 
             AND m.temperature < 80 
             AND m.power_factor > 0.9 THEN 95.0
        WHEN m.voltage BETWEEN 180 AND 260 
             AND m.temperature < 90 
             AND m.power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END as health_score,
    CASE 
        WHEN m.voltage > 240 OR m.voltage < 200 OR m.temperature > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    'DWD-明细数据层' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM ods_meter_reading m
JOIN ods_device_info d ON m.device_id = d.id
JOIN ods_substation s ON d.substation_id = s.id;