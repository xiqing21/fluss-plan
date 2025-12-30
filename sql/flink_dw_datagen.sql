-- 国网实时数仓完整分层架构 - 使用datagen + Fluss + 窗口汇总 + PostgreSQL Sink
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- ODS层：模拟电表读数数据源（增加水印支持窗口计算）
CREATE TEMPORARY TABLE ods_meter_reading (
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
    WATERMARK FOR reading_time AS reading_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.id.min' = '1',
    'fields.id.max' = '10000',
    'fields.device_id.min' = '1',
    'fields.device_id.max' = '5',
    'fields.voltage.min' = '180',
    'fields.voltage.max' = '260',
    'fields.current_val.min' = '8',
    'fields.current_val.max' = '25',
    'fields.power_val.min' = '1500',
    'fields.power_val.max' = '5000',
    'fields.energy.min' = '50',
    'fields.energy.max' = '1500',
    'fields.power_factor.min' = '0.75',
    'fields.power_factor.max' = '1.0',
    'fields.frequency.min' = '49.0',
    'fields.frequency.max' = '51.0',
    'fields.temperature.min' = '25',
    'fields.temperature.max' = '95'
);

-- ODS层：Fluss存储原始数据
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'fluss'
);

-- DWD层：设备电表明细数据层（Fluss表）
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
    WATERMARK FOR reading_time AS reading_time - INTERVAL '5' SECOND,
    PRIMARY KEY (device_id, reading_time) NOT ENFORCED
) WITH (
    'connector' = 'fluss'
);

-- DWS层：设备30秒窗口汇总表（Fluss表）
CREATE TABLE dws_device_30s_summary (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
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
    PRIMARY KEY (device_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'fluss'
);

-- DWS层：变电站30秒窗口汇总表（Fluss表）
CREATE TABLE dws_substation_30s_summary (
    substation_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_devices INT,
    active_devices INT,
    warning_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    create_time TIMESTAMP(3),
    PRIMARY KEY (substation_name, window_start) NOT ENFORCED
) WITH (
    'connector' = 'fluss'
);

-- ADS层：PostgreSQL Sink表 - 设备实时监控
CREATE TEMPORARY TABLE ads_device_realtime_monitor (
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
    'sink.buffer-flush.max-rows' = '50',
    'sink.buffer-flush.interval' = '2s'
);

-- ADS层：PostgreSQL Sink表 - 设备窗口汇总
CREATE TEMPORARY TABLE ads_device_window_summary (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
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
    'table-name' = 'device_window_summary',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '20',
    'sink.buffer-flush.interval' = '3s'
);

-- ADS层：PostgreSQL Sink表 - 变电站汇总
CREATE TEMPORARY TABLE ads_substation_summary (
    substation_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_devices INT,
    active_devices INT,
    warning_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'substation_summary',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '10',
    'sink.buffer-flush.interval' = '3s'
);

-- 控制台输出表（用于监控）
CREATE TEMPORARY TABLE console_output (
    layer_name STRING,
    device_id INT,
    device_code STRING,
    substation_name STRING,
    metric_name STRING,
    metric_value STRING,
    status STRING,
    process_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- ===== 数据流处理作业 =====

-- 第一步：ODS层数据同步到Fluss
INSERT INTO ods_meter_reading_fluss
SELECT * FROM ods_meter_reading;

-- 第二步：ODS -> DWD（明细数据处理）
INSERT INTO dwd_device_meter_detail
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CONCAT('智能电表', CAST(device_id AS STRING)) as device_name,
    'SMART_METER' as device_type,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        WHEN device_id <= 4 THEN '北京变电站'
        ELSE '深圳变电站'
    END as substation_name,
    reading_time,
    voltage,
    current_val,
    power_val,
    energy,
    power_factor,
    frequency,
    temperature,
    -- 健康度评分计算（更复杂的逻辑）
    CASE 
        WHEN voltage BETWEEN 210 AND 230 
             AND temperature < 70 
             AND power_factor > 0.95 
             AND frequency BETWEEN 49.8 AND 50.2 THEN 98.0
        WHEN voltage BETWEEN 200 AND 240 
             AND temperature < 80 
             AND power_factor > 0.9 
             AND frequency BETWEEN 49.5 AND 50.5 THEN 90.0
        WHEN voltage BETWEEN 180 AND 260 
             AND temperature < 90 
             AND power_factor > 0.8 THEN 75.0
        ELSE 50.0
    END as health_score,
    -- 电压状态
    CASE 
        WHEN voltage > 250 THEN 'CRITICAL_HIGH'
        WHEN voltage > 240 THEN 'HIGH'
        WHEN voltage < 180 THEN 'CRITICAL_LOW'
        WHEN voltage < 200 THEN 'LOW'
        ELSE 'NORMAL'
    END as voltage_status,
    -- 温度状态
    CASE 
        WHEN temperature > 90 THEN 'CRITICAL_HIGH'
        WHEN temperature > 80 THEN 'HIGH'
        WHEN temperature > 60 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END as temperature_status,
    -- 功率状态
    CASE 
        WHEN power_val > 4500 THEN 'HIGH'
        WHEN power_val < 2000 THEN 'LOW'
        ELSE 'NORMAL'
    END as power_status,
    CURRENT_TIMESTAMP as create_time
FROM ods_meter_reading_fluss;

-- 第三步：DWD -> DWS（30秒窗口汇总）
INSERT INTO dws_device_30s_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    TUMBLE_START(reading_time, INTERVAL '30' SECOND) as window_start,
    TUMBLE_END(reading_time, INTERVAL '30' SECOND) as window_end,
    COUNT(*) as reading_count,
    AVG(voltage) as avg_voltage,
    MAX(voltage) as max_voltage,
    MIN(voltage) as min_voltage,
    AVG(current_val) as avg_current,
    MAX(current_val) as max_current,
    AVG(power_val) as avg_power,
    MAX(power_val) as max_power,
    SUM(energy) as total_energy,
    AVG(temperature) as avg_temperature,
    MAX(temperature) as max_temperature,
    AVG(health_score) as avg_health_score,
    SUM(CASE WHEN voltage_status != 'NORMAL' OR temperature_status IN ('HIGH', 'CRITICAL_HIGH') THEN 1 ELSE 0 END) as abnormal_count,
    CURRENT_TIMESTAMP as create_time
FROM dwd_device_meter_detail
GROUP BY 
    device_id, device_code, device_name, device_type, substation_name,
    TUMBLE(reading_time, INTERVAL '30' SECOND);

-- 第四步：DWD -> DWS（变电站30秒窗口汇总）
INSERT INTO dws_substation_30s_summary
SELECT 
    substation_name,
    TUMBLE_START(reading_time, INTERVAL '30' SECOND) as window_start,
    TUMBLE_END(reading_time, INTERVAL '30' SECOND) as window_end,
    COUNT(DISTINCT device_id) as total_devices,
    COUNT(DISTINCT CASE WHEN voltage_status = 'NORMAL' AND temperature_status IN ('NORMAL', 'MEDIUM') THEN device_id END) as active_devices,
    COUNT(DISTINCT CASE WHEN voltage_status != 'NORMAL' OR temperature_status IN ('HIGH', 'CRITICAL_HIGH') THEN device_id END) as warning_devices,
    SUM(power_val) as total_power,
    AVG(voltage) as avg_voltage,
    AVG(temperature) as avg_temperature,
    AVG(health_score) as avg_health_score,
    CURRENT_TIMESTAMP as create_time
FROM dwd_device_meter_detail
GROUP BY 
    substation_name,
    TUMBLE(reading_time, INTERVAL '30' SECOND);

-- 第五步：DWD -> ADS（设备实时监控到PostgreSQL）
INSERT INTO ads_device_realtime_monitor
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    voltage as latest_voltage,
    current_val as latest_current,
    power_val as latest_power,
    energy as latest_energy,
    power_factor,
    frequency,
    temperature,
    health_score,
    CASE 
        WHEN voltage_status IN ('CRITICAL_HIGH', 'CRITICAL_LOW') OR temperature_status = 'CRITICAL_HIGH' THEN 'CRITICAL'
        WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN 'WARNING'
        WHEN health_score < 80 THEN 'ATTENTION'
        ELSE 'NORMAL'
    END as status,
    reading_time as last_update_time,
    create_time
FROM dwd_device_meter_detail;

-- 第六步：DWS -> ADS（设备窗口汇总到PostgreSQL）
INSERT INTO ads_device_window_summary
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    window_start,
    window_end,
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
FROM dws_device_30s_summary;

-- 第七步：DWS -> ADS（变电站汇总到PostgreSQL）
INSERT INTO ads_substation_summary
SELECT 
    substation_name,
    window_start,
    window_end,
    total_devices,
    active_devices,
    warning_devices,
    total_power,
    avg_voltage,
    avg_temperature,
    avg_health_score,
    create_time
FROM dws_substation_30s_summary;

-- 第八步：控制台监控输出（DWD层）
INSERT INTO console_output
SELECT 
    'DWD-明细层' as layer_name,
    device_id,
    device_code,
    substation_name,
    '健康度' as metric_name,
    CAST(health_score AS STRING) as metric_value,
    CASE 
        WHEN voltage_status != 'NORMAL' OR temperature_status IN ('HIGH', 'CRITICAL_HIGH') THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    CURRENT_TIMESTAMP as process_time
FROM dwd_device_meter_detail;

-- 第九步：控制台监控输出（DWS层）
INSERT INTO console_output
SELECT 
    'DWS-汇总层' as layer_name,
    device_id,
    device_code,
    substation_name,
    '30秒平均电压' as metric_name,
    CAST(avg_voltage AS STRING) as metric_value,
    CASE 
        WHEN abnormal_count > 0 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    CURRENT_TIMESTAMP as process_time
FROM dws_device_30s_summary;