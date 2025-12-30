#!/bin/bash
# 启动完整数仓分层作业 - 分别提交DWD、DWS、ADS作业

set -e

echo "=== 启动完整数仓分层作业 ==="

# 创建表结构
echo "创建表结构..."
cat > /tmp/create_tables.sql << 'EOF'
SET 'execution.checkpointing.interval' = '30s';

-- ODS层：模拟实时数据源
CREATE TABLE ods_meter_reading (
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
    'rows-per-second' = '5',
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

CREATE TABLE dwd_output (
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

CREATE TABLE dws_window_output (
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
    max_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    abnormal_count BIGINT,
    layer_info STRING
) WITH ('connector' = 'print');

CREATE TABLE ads_monitor_output (
    device_id INT,
    device_code STRING,
    substation_name STRING,
    latest_voltage DECIMAL(8,2),
    latest_temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH ('connector' = 'print');
EOF

# 创建DWD作业
cat > /tmp/dwd_job.sql << 'EOF'
INSERT INTO dwd_output
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    voltage,
    temperature,
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
    'DWD-明细数据层' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM ods_meter_reading;
EOF

# 创建DWS作业
cat > /tmp/dws_job.sql << 'EOF'
INSERT INTO dws_window_output
SELECT 
    device_id,
    CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
    CASE 
        WHEN device_id <= 2 THEN '上海变电站'
        ELSE '北京变电站'
    END as substation_name,
    TUMBLE_START(reading_time, INTERVAL '20' SECOND) as window_start,
    TUMBLE_END(reading_time, INTERVAL '20' SECOND) as window_end,
    COUNT(*) as record_count,
    AVG(voltage) as avg_voltage,
    MAX(voltage) as max_voltage,
    MIN(voltage) as min_voltage,
    AVG(temperature) as avg_temperature,
    MAX(temperature) as max_temperature,
    AVG(CASE 
        WHEN voltage BETWEEN 200 AND 240 
             AND temperature < 80 
             AND power_factor > 0.9 THEN 95.0
        WHEN voltage BETWEEN 180 AND 260 
             AND temperature < 90 
             AND power_factor > 0.8 THEN 80.0
        ELSE 60.0
    END) as avg_health_score,
    SUM(CASE 
        WHEN voltage > 240 OR voltage < 200 OR temperature > 80 THEN 1 
        ELSE 0 
    END) as abnormal_count,
    'DWS-汇总数据层' as layer_info
FROM ods_meter_reading
GROUP BY 
    device_id,
    TUMBLE(reading_time, INTERVAL '20' SECOND);
EOF

echo "提交表创建..."
/opt/flink/bin/sql-client.sh -f /tmp/create_tables.sql > /app/logs/create_tables.log 2>&1

echo "提交DWD作业..."
nohup /opt/flink/bin/sql-client.sh -f /tmp/dwd_job.sql > /app/logs/dwd_job.log 2>&1 &

echo "等待5秒..."
sleep 5

echo "提交DWS作业..."
nohup /opt/flink/bin/sql-client.sh -f /tmp/dws_job.sql > /app/logs/dws_job.log 2>&1 &

echo "等待20秒查看结果..."
sleep 20

echo "=== 检查作业状态 ==="
curl -s http://localhost:8081/jobs

echo -e "\n=== 查看数仓分层输出 ==="
echo "DWD层输出:"
find /opt/flink/log -name '*taskexecutor*.out' -exec tail -50 {} \; | grep 'DWD-明细数据层' | head -5

echo -e "\nDWS层输出:"
find /opt/flink/log -name '*taskexecutor*.out' -exec tail -50 {} \; | grep 'DWS-汇总数据层' | head -5

echo -e "\n=== 数仓分层作业启动完成 ==="