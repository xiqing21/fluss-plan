-- 国网实时数仓Flink SQL作业脚本
-- 注意：需要在Flink SQL Client中执行

-- 设置Flink配置
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 创建Fluss Catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- 创建ODS层表（从PostgreSQL CDC同步）
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
    'slot.name' = 'flink_slot'
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
    'slot.name' = 'flink_slot'
);

CREATE TABLE ods_meter_reading (
    id BIGINT,
    device_id INT,
    reading_time TIMESTAMP(3),
    voltage DECIMAL(8,2),
    current DECIMAL(8,2),
    power DECIMAL(10,2),
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

CREATE TABLE ods_customer (
    id INT,
    customer_code STRING,
    customer_name STRING,
    customer_type STRING,
    address STRING,
    phone STRING,
    email STRING,
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
    'table-name' = 'customer',
    'slot.name' = 'flink_slot'
);

CREATE TABLE ods_customer_device (
    id INT,
    customer_id INT,
    device_id INT,
    relationship_type STRING,
    start_date DATE,
    end_date DATE,
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
    'table-name' = 'customer_device',
    'slot.name' = 'flink_slot'
);

CREATE TABLE ods_alarm_info (
    id BIGINT,
    device_id INT,
    alarm_type STRING,
    alarm_level STRING,
    alarm_message STRING,
    alarm_time TIMESTAMP(3),
    status STRING,
    resolved_time TIMESTAMP(3),
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
    'table-name' = 'alarm_info',
    'slot.name' = 'flink_slot'
);

-- 创建DWD层表（明细数据层）
CREATE TABLE dwd_device_meter_detail (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_id INT,
    substation_name STRING,
    customer_id INT,
    customer_name STRING,
    reading_time TIMESTAMP(3),
    voltage DECIMAL(8,2),
    current DECIMAL(8,2),
    power DECIMAL(10,2),
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
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- 创建DWS层表（汇总数据层）
-- 1. 设备小时级汇总表
CREATE TABLE dws_device_hour_summary (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_id INT,
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
    avg_power_factor DECIMAL(4,3),
    avg_frequency DECIMAL(6,2),
    avg_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    abnormal_count BIGINT,
    create_time TIMESTAMP(3),
    PRIMARY KEY (device_id, stat_date, stat_hour) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- 2. 变电站小时级汇总表
CREATE TABLE dws_substation_hour_summary (
    substation_id INT,
    substation_name STRING,
    stat_date DATE,
    stat_hour INT,
    total_devices INT,
    active_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    alarm_count BIGINT,
    health_score DECIMAL(5,2),
    create_time TIMESTAMP(3),
    PRIMARY KEY (substation_id, stat_date, stat_hour) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- 3. 客户用电小时级汇总表
CREATE TABLE dws_customer_hour_summary (
    customer_id INT,
    customer_code STRING,
    customer_name STRING,
    customer_type STRING,
    stat_date DATE,
    stat_hour INT,
    device_count INT,
    total_energy DECIMAL(12,2),
    peak_power DECIMAL(10,2),
    avg_power DECIMAL(10,2),
    power_cost DECIMAL(10,2),
    create_time TIMESTAMP(3),
    PRIMARY KEY (customer_id, stat_date, stat_hour) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- 创建ADS层Doris结果表
-- 创建PostgreSQL Sink表
CREATE TABLE postgres_device_realtime_monitor (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    latest_voltage DECIMAL(8,2),
    latest_current DECIMAL(8,2),
    latest_power DECIMAL(10,2),
    latest_energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    last_update_time TIMESTAMP(3),
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'device_realtime_monitor',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE postgres_device_hour_summary (
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
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'device_health_history',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE postgres_substation_overview (
    substation_id INT,
    substation_name STRING,
    total_devices INT,
    normal_devices INT,
    warning_devices INT,
    fault_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    active_alarms INT,
    last_update_time TIMESTAMP(3),
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'substation_realtime_overview',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- DWD层：设备电表明细数据处理
INSERT INTO dwd_device_meter_detail
SELECT 
    d.id as device_id,
    d.device_code,
    d.device_name,
    d.device_type,
    s.id as substation_id,
    s.name as substation_name,
    COALESCE(c.id, 0) as customer_id,
    COALESCE(c.customer_name, 'Unknown') as customer_name,
    m.reading_time,
    m.voltage,
    m.current,
    m.power,
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
        WHEN m.power > 5000 THEN 'HIGH'
        WHEN m.power < 1000 THEN 'LOW'
        ELSE 'NORMAL'
    END as power_status,
    CURRENT_TIMESTAMP as create_time
FROM ods_meter_reading m
JOIN ods_device_info d ON m.device_id = d.id
JOIN ods_substation s ON d.substation_id = s.id
LEFT JOIN ods_customer_device cd ON d.id = cd.device_id
LEFT JOIN ods_customer c ON cd.customer_id = c.id;

-- DWS层：设备小时级汇总
INSERT INTO dws_device_hour_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_id,
    substation_name,
    DATE(reading_time) as stat_date,
    HOUR(reading_time) as stat_hour,
    COUNT(*) as reading_count,
    AVG(voltage) as avg_voltage,
    MAX(voltage) as max_voltage,
    MIN(voltage) as min_voltage,
    AVG(current) as avg_current,
    MAX(current) as max_current,
    AVG(power) as avg_power,
    MAX(power) as max_power,
    SUM(energy) as total_energy,
    AVG(power_factor) as avg_power_factor,
    AVG(frequency) as avg_frequency,
    AVG(temperature) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,
    AVG(health_score) as avg_health_score,
    SUM(CASE WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN 1 ELSE 0 END) as abnormal_count,
    CURRENT_TIMESTAMP as create_time
FROM dwd_device_meter_detail
GROUP BY 
    device_id, device_code, device_name, device_type, 
    substation_id, substation_name,
    DATE(reading_time), HOUR(reading_time);

-- DWS层：变电站小时级汇总
INSERT INTO dws_substation_hour_summary
SELECT 
    substation_id,
    substation_name,
    DATE(reading_time) as stat_date,
    HOUR(reading_time) as stat_hour,
    COUNT(DISTINCT device_id) as total_devices,
    COUNT(DISTINCT CASE WHEN voltage_status = 'NORMAL' AND temperature_status != 'HIGH' THEN device_id END) as active_devices,
    SUM(power) as total_power,
    AVG(voltage) as avg_voltage,
    AVG(temperature) as avg_temperature,
    0 as alarm_count, -- 需要关联告警表
    AVG(health_score) as health_score,
    CURRENT_TIMESTAMP as create_time
FROM dwd_device_meter_detail
GROUP BY 
    substation_id, substation_name,
    DATE(reading_time), HOUR(reading_time);

-- DWS层：客户用电小时级汇总
INSERT INTO dws_customer_hour_summary
SELECT 
    customer_id,
    'CUST' || LPAD(CAST(customer_id AS STRING), 3, '0') as customer_code,
    customer_name,
    'UNKNOWN' as customer_type, -- 需要关联客户表获取类型
    DATE(reading_time) as stat_date,
    HOUR(reading_time) as stat_hour,
    COUNT(DISTINCT device_id) as device_count,
    SUM(energy) as total_energy,
    MAX(power) as peak_power,
    AVG(power) as avg_power,
    SUM(energy) * 0.6 as power_cost, -- 假设电价0.6元/kWh
    CURRENT_TIMESTAMP as create_time
FROM dwd_device_meter_detail
WHERE customer_id > 0
GROUP BY 
    customer_id, customer_name,
    DATE(reading_time), HOUR(reading_time);

-- ADS层：设备实时监控（从DWD层最新数据）
INSERT INTO postgres_device_realtime_monitor
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    voltage as latest_voltage,
    current as latest_current,
    power as latest_power,
    energy as latest_energy,
    power_factor,
    frequency,
    temperature,
    health_score,
    CASE 
        WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    reading_time as last_update_time,
    create_time
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY reading_time DESC) as rn
    FROM dwd_device_meter_detail
) t
WHERE rn = 1;

-- ADS层：设备小时汇总（从DWS层同步到PostgreSQL）
INSERT INTO postgres_device_hour_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    stat_date,
    stat_hour,
    reading_count,
    avg_voltage,
    max_voltage,
    min_voltage,
    avg_current,
    max_current,
    avg_power,
    max_power,
    total_energy,
    avg_temperature,
    max_temperature,
    avg_health_score,
    abnormal_count,
    create_time
FROM dws_device_hour_summary;

-- ADS层：变电站实时概览
INSERT INTO postgres_substation_overview
SELECT 
    s.substation_id,
    s.substation_name,
    s.total_devices,
    s.active_devices,
    s.total_devices - s.active_devices as warning_devices,
    0 as fault_devices, -- 需要关联告警数据
    s.total_power,
    s.avg_voltage,
    0 as avg_current, -- 从设备汇总计算
    s.avg_temperature,
    0 as active_alarms, -- 需要关联告警数据
    CURRENT_TIMESTAMP as last_update_time,
    s.create_time
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY substation_id ORDER BY stat_date DESC, stat_hour DESC) as rn
    FROM dws_substation_hour_summary
) s
WHERE rn = 1;