-- 国网实时数仓Flink SQL作业脚本 - 简化版本测试数仓分层逻辑
SET 'execution.checkpointing.interval' = '30s';

-- ODS层：模拟电表读数数据源
CREATE TABLE ods_meter_reading (
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
    'rows-per-second' = '3',
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

-- DWD层：设备电表明细数据处理结果
CREATE TABLE dwd_device_detail_output (
    device_id INT,
    device_code STRING,
    device_name STRING,
    device_type STRING,
    substation_name STRING,
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    voltage_status STRING,
    temperature_status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- DWS层：设备汇总数据
CREATE TABLE dws_device_summary_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    record_count BIGINT,
    layer_info STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- ADS层：应用数据服务输出
CREATE TABLE ads_device_monitor_output (
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

-- DWD层数据处理：ODS -> DWD（明细数据层）
INSERT INTO dwd_device_detail_output
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CONCAT('设备', CAST(device_id AS STRING)) as device_name,
    'METER' as device_type,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    voltage,
    current_val,
    power_val,
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
    'DWD-明细数据层' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM ods_meter_reading;

-- DWS层数据处理：DWD -> DWS（汇总数据层）
INSERT INTO dws_device_summary_output
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    AVG(voltage) as avg_voltage,
    AVG(temperature) as avg_temperature,
    AVG(CASE 
        WHEN voltage BETWEEN 200 AND 240 
             AND temperature < 80 
             AND power_factor > 0.9 THEN 95.0
        WHEN voltage BETWEEN 180 AND 260 
             AND temperature < 90 
             AND power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END) as avg_health_score,
    COUNT(*) as record_count,
    'DWS-汇总数据层' as layer_info,
    TUMBLE_START(reading_time, INTERVAL '30' SECOND) as window_start,
    TUMBLE_END(reading_time, INTERVAL '30' SECOND) as window_end
FROM ods_meter_reading
GROUP BY 
    device_id,
    TUMBLE(reading_time, INTERVAL '30' SECOND);

-- ADS层数据处理：DWS -> ADS（应用数据层）
INSERT INTO ads_device_monitor_output
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    voltage as latest_voltage,
    temperature as latest_temperature,
    CASE 
        WHEN voltage BETWEEN 200 AND 240 
             AND temperature < 80 
             AND power_factor > 0.9 THEN 95.0
        WHEN voltage BETWEEN 180 AND 260 
             AND temperature < 90 
             AND power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END as health_score,
    CASE 
        WHEN voltage > 240 OR voltage < 200 OR temperature > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    'ADS-应用数据层' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM ods_meter_reading;