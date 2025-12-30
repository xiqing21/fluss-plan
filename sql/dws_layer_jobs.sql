-- DWS层（数据汇总层）Flink SQL作业
-- 基于DWD层数据进行时间窗口聚合和业务维度汇总

-- 设置Flink配置
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '2h';

USE CATALOG fluss_catalog;

-- 创建DWS层5分钟汇总作业
CREATE TEMPORARY VIEW dws_device_5min_agg AS
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    TUMBLE_START(reading_time, INTERVAL '5' MINUTE) as window_start,
    TUMBLE_END(reading_time, INTERVAL '5' MINUTE) as window_end,
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
    SUM(CASE WHEN voltage_status != 'NORMAL' THEN 1 ELSE 0 END) as voltage_abnormal_count,
    SUM(CASE WHEN temperature_status = 'HIGH' THEN 1 ELSE 0 END) as temp_abnormal_count,
    SUM(CASE WHEN power_status != 'NORMAL' THEN 1 ELSE 0 END) as power_abnormal_count
FROM dwd_device_meter_detail
GROUP BY 
    device_id, device_code, device_name, device_type, substation_name,
    TUMBLE(reading_time, INTERVAL '5' MINUTE);

-- 创建DWS层小时汇总作业
CREATE TEMPORARY VIEW dws_device_hour_agg AS
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_id,
    substation_name,
    TUMBLE_START(reading_time, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(reading_time, INTERVAL '1' HOUR) as window_end,
    DATE_FORMAT(TUMBLE_START(reading_time, INTERVAL '1' HOUR), 'yyyy-MM-dd') as stat_date,
    HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR)) as stat_hour,
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
    SUM(CASE WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' OR power_status != 'NORMAL' THEN 1 ELSE 0 END) as abnormal_count
FROM dwd_device_meter_detail
GROUP BY 
    device_id, device_code, device_name, device_type, 
    substation_id, substation_name,
    TUMBLE(reading_time, INTERVAL '1' HOUR);

-- 创建变电站小时汇总作业
CREATE TEMPORARY VIEW dws_substation_hour_agg AS
SELECT 
    substation_id,
    substation_name,
    TUMBLE_START(reading_time, INTERVAL '1' HOUR) as window_start,
    DATE_FORMAT(TUMBLE_START(reading_time, INTERVAL '1' HOUR), 'yyyy-MM-dd') as stat_date,
    HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR)) as stat_hour,
    COUNT(DISTINCT device_id) as total_devices,
    COUNT(DISTINCT CASE WHEN voltage_status = 'NORMAL' AND temperature_status != 'HIGH' THEN device_id END) as active_devices,
    COUNT(DISTINCT CASE WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN device_id END) as warning_devices,
    SUM(power) as total_power,
    AVG(voltage) as avg_voltage,
    AVG(current) as avg_current,
    AVG(temperature) as avg_temperature,
    AVG(health_score) as health_score
FROM dwd_device_meter_detail
GROUP BY 
    substation_id, substation_name,
    TUMBLE(reading_time, INTERVAL '1' HOUR);

-- 创建客户用电小时汇总作业
CREATE TEMPORARY VIEW dws_customer_hour_agg AS
SELECT 
    customer_id,
    customer_name,
    TUMBLE_START(reading_time, INTERVAL '1' HOUR) as window_start,
    DATE_FORMAT(TUMBLE_START(reading_time, INTERVAL '1' HOUR), 'yyyy-MM-dd') as stat_date,
    HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR)) as stat_hour,
    COUNT(DISTINCT device_id) as device_count,
    SUM(energy) as total_energy,
    MAX(power) as peak_power,
    AVG(power) as avg_power,
    -- 电费计算（分时电价）
    CASE 
        WHEN HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR)) BETWEEN 8 AND 11 
             OR HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR)) BETWEEN 18 AND 21 THEN
            SUM(energy) * 0.8  -- 峰时电价
        WHEN HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR')) BETWEEN 12 AND 17 
             OR HOUR(TUMBLE_START(reading_time, INTERVAL '1' HOUR')) BETWEEN 22 AND 23 THEN
            SUM(energy) * 0.6  -- 平时电价
        ELSE
            SUM(energy) * 0.4  -- 谷时电价
    END as power_cost
FROM dwd_device_meter_detail
WHERE customer_id > 0
GROUP BY 
    customer_id, customer_name,
    TUMBLE(reading_time, INTERVAL '1' HOUR);

-- 创建PostgreSQL DWS层结果表
CREATE TABLE postgres_device_5min_summary (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    stat_time TIMESTAMP(3),
    reading_count BIGINT,
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_power DECIMAL(10,2),
    avg_temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'device_5min_summary',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE postgres_substation_hour_summary (
    substation_id INT,
    substation_name STRING,
    stat_date DATE,
    stat_hour INT,
    total_devices INT,
    active_devices INT,
    warning_devices INT,
    fault_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    alarm_count BIGINT,
    health_score DECIMAL(5,2),
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'substation_hour_summary',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE postgres_customer_hour_summary (
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
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'customer_hour_summary',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- 插入DWS层5分钟汇总数据到PostgreSQL
INSERT INTO postgres_device_5min_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    window_start as stat_time,
    reading_count,
    avg_voltage,
    avg_current,
    avg_power,
    avg_temperature,
    avg_health_score as health_score,
    CASE 
        WHEN voltage_abnormal_count > 0 OR temp_abnormal_count > 0 OR power_abnormal_count > 0 THEN 'WARNING'
        WHEN avg_health_score < 70 THEN 'FAULT'
        ELSE 'NORMAL'
    END as status,
    CURRENT_TIMESTAMP as create_time
FROM dws_device_5min_agg;

-- 插入DWS层设备小时汇总数据到PostgreSQL
INSERT INTO postgres_device_hour_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    CAST(stat_date AS DATE) as stat_date,
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
    CURRENT_TIMESTAMP as create_time
FROM dws_device_hour_agg;

-- 插入DWS层变电站小时汇总数据到PostgreSQL
INSERT INTO postgres_substation_hour_summary
SELECT 
    substation_id,
    substation_name,
    CAST(stat_date AS DATE) as stat_date,
    stat_hour,
    total_devices,
    active_devices,
    warning_devices,
    0 as fault_devices, -- 需要关联告警数据计算
    total_power,
    avg_voltage,
    avg_current,
    avg_temperature,
    0 as alarm_count, -- 需要关联告警数据计算
    health_score,
    CURRENT_TIMESTAMP as create_time
FROM dws_substation_hour_agg;

-- 插入DWS层客户用电小时汇总数据到PostgreSQL
INSERT INTO postgres_customer_hour_summary
SELECT 
    customer_id,
    'CUST' || LPAD(CAST(customer_id AS STRING), 3, '0') as customer_code,
    customer_name,
    'UNKNOWN' as customer_type, -- 需要关联客户表获取类型
    CAST(stat_date AS DATE) as stat_date,
    stat_hour,
    device_count,
    total_energy,
    peak_power,
    avg_power,
    power_cost,
    CURRENT_TIMESTAMP as create_time
FROM dws_customer_hour_agg;