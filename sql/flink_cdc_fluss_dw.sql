-- 国网实时数仓完整架构：CDC → Fluss → 数仓分层
SET 'execution.checkpointing.interval' = '30s';

-- 创建Fluss Catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- CDC源表（TEMPORARY表避免Fluss Catalog限制）
CREATE TEMPORARY TABLE cdc_meter_reading (
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

CREATE TEMPORARY TABLE cdc_device_info (
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

CREATE TEMPORARY TABLE cdc_substation (
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

-- Fluss ODS层表
CREATE TABLE ods_meter_reading_fluss (
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
);

-- Fluss DWD层表
CREATE TABLE dwd_device_meter_detail (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    reading_time TIMESTAMP(3),
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    voltage_status STRING,
    temperature_status STRING,
    power_status STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (device_id, reading_time) NOT ENFORCED
);

-- Fluss DWS层表
CREATE TABLE dws_device_hour_summary (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    stat_date DATE,
    stat_hour INT,
    reading_count BIGINT,
    avg_voltage DECIMAL(8,2),
    max_voltage DECIMAL(8,2),
    min_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    max_current DECIMAL(8,2),
    avg_power DECIMAL(10,2),
    max_power DECIMAL(10,2),
    total_energy DECIMAL(12,2),
    avg_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    abnormal_count BIGINT,
    create_time TIMESTAMP(3),
    PRIMARY KEY (device_id, stat_date, stat_hour) NOT ENFORCED
);

-- 输出表（打印到控制台）
CREATE TEMPORARY TABLE dwd_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    voltage DECIMAL(8,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH ('connector' = 'print');

CREATE TEMPORARY TABLE dws_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    record_count BIGINT,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH ('connector' = 'print');

-- 第一步：CDC数据同步到Fluss ODS层
INSERT INTO ods_meter_reading_fluss
SELECT * FROM cdc_meter_reading;

-- 第二步：ODS → DWD（明细数据处理）
INSERT INTO dwd_device_meter_detail
SELECT 
    m.device_id,
    d.device_code,
    d.device_name,
    d.device_type,
    s.name as substation_name,
    m.reading_time,
    m.voltage,
    m.current_val,
    m.power_val,
    m.energy,
    m.power_factor,
    m.frequency,
    m.temperature,
    -- 健康度评分计算
    CASE 
        WHEN m.voltage BETWEEN 200 AND 240 
             AND m.temperature < 80 
             AND m.power_factor > 0.9 THEN 95.0
        WHEN m.voltage BETWEEN 180 AND 260 
             AND m.temperature < 90 
             AND m.power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END as health_score,
    -- 电压状态
    CASE 
        WHEN m.voltage > 240 THEN 'HIGH'
        WHEN m.voltage < 200 THEN 'LOW'
        ELSE 'NORMAL'
    END as voltage_status,
    -- 温度状态
    CASE 
        WHEN m.temperature > 80 THEN 'HIGH'
        WHEN m.temperature > 60 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END as temperature_status,
    -- 功率状态
    CASE 
        WHEN m.power_val > 3500 THEN 'HIGH'
        WHEN m.power_val < 2500 THEN 'LOW'
        ELSE 'NORMAL'
    END as power_status,
    CURRENT_TIMESTAMP as create_time
FROM ods_meter_reading_fluss m
JOIN cdc_device_info d ON m.device_id = d.id
JOIN cdc_substation s ON d.substation_id = s.id;

-- 第三步：DWD数据输出到控制台
INSERT INTO dwd_output
SELECT 
    device_id,
    device_code,
    substation_name,
    voltage,
    temperature,
    health_score,
    CASE 
        WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    'DWD-明细数据层(Fluss)' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM dwd_device_meter_detail;