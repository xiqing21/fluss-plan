-- 国网实时数仓Flink SQL作业脚本 - 使用datagen模拟数据源
SET 'execution.checkpointing.interval' = '30s';

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- ODS层：模拟电表读数数据源
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
    temperature DECIMAL(5,2)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.id.min' = '1',
    'fields.id.max' = '10000',
    'fields.device_id.min' = '1',
    'fields.device_id.max' = '5',
    'fields.voltage.min' = '200',
    'fields.voltage.max' = '240',
    'fields.current_val.min' = '10',
    'fields.current_val.max' = '20',
    'fields.power_val.min' = '2000',
    'fields.power_val.max' = '4000',
    'fields.energy.min' = '100',
    'fields.energy.max' = '1000',
    'fields.power_factor.min' = '0.8',
    'fields.power_factor.max' = '1.0',
    'fields.frequency.min' = '49.5',
    'fields.frequency.max' = '50.5',
    'fields.temperature.min' = '30',
    'fields.temperature.max' = '80'
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
    PRIMARY KEY (device_id, reading_time) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- DWS层：设备小时级汇总表（Fluss表）
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
) WITH (
    'connector' = 'fluss',
    'table.type' = 'log'
);

-- ADS层：设备实时监控输出表（打印到控制台）
CREATE TEMPORARY TABLE ads_device_monitor_output (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    latest_voltage DECIMAL(8,2),
    latest_current DECIMAL(8,2),
    latest_power DECIMAL(10,2),
    health_score DECIMAL(5,2),
    status STRING,
    process_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- DWD层数据处理：ODS -> DWD
INSERT INTO dwd_device_meter_detail
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CONCAT('设备', CAST(device_id AS STRING)) as device_name,
    'METER' as device_type,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    reading_time,
    voltage,
    current_val,
    power_val,
    energy,
    power_factor,
    frequency,
    temperature,
    -- 健康度评分计算
    CASE 
        WHEN voltage BETWEEN 200 AND 240 
             AND temperature < 80 
             AND power_factor > 0.9 THEN 95.0
        WHEN voltage BETWEEN 180 AND 260 
             AND temperature < 90 
             AND power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END as health_score,
    -- 电压状态
    CASE 
        WHEN voltage > 240 THEN 'HIGH'
        WHEN voltage < 200 THEN 'LOW'
        ELSE 'NORMAL'
    END as voltage_status,
    -- 温度状态
    CASE 
        WHEN temperature > 80 THEN 'HIGH'
        WHEN temperature > 60 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END as temperature_status,
    -- 功率状态
    CASE 
        WHEN power_val > 3500 THEN 'HIGH'
        WHEN power_val < 2500 THEN 'LOW'
        ELSE 'NORMAL'
    END as power_status,
    CURRENT_TIMESTAMP as create_time
FROM ods_meter_reading;

-- ADS层数据处理：DWD -> ADS（实时监控）
INSERT INTO ads_device_monitor_output
SELECT 
    device_id,
    device_code,
    device_name,
    device_type,
    substation_name,
    voltage as latest_voltage,
    current_val as latest_current,
    power_val as latest_power,
    health_score,
    CASE 
        WHEN voltage_status != 'NORMAL' OR temperature_status = 'HIGH' THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    CURRENT_TIMESTAMP as process_time
FROM dwd_device_meter_detail;