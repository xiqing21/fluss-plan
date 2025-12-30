#!/bin/bash
# 简化的数据管道启动脚本 - 使用JDBC连接器

set -e

echo "=== 启动简化数据管道 ==="

# 检查Flink是否运行
if ! curl -s http://localhost:8081/overview > /dev/null; then
    echo "启动Flink集群..."
    /opt/flink/bin/start-cluster.sh
    sleep 10
fi

echo "=== 创建简化的Flink SQL作业 ==="

# 创建简化的Flink SQL脚本 - 使用JDBC连接器
cat > /tmp/flink_simple_pipeline.sql << 'EOF'
-- 设置Flink配置
SET 'execution.checkpointing.interval' = '30s';

-- 创建PostgreSQL源表（使用JDBC连接器读取）
CREATE TABLE meter_reading_source (
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
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid',
    'table-name' = 'meter_reading',
    'username' = 'postgres',
    'password' = 'postgres',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '4',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '10000'
);

-- 创建设备信息源表
CREATE TABLE device_info_source (
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
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid',
    'table-name' = 'device_info',
    'username' = 'postgres',
    'password' = 'postgres'
);

-- 创建变电站信息源表
CREATE TABLE substation_source (
    id INT,
    name STRING,
    location STRING,
    voltage_level INT,
    capacity DECIMAL(10,2),
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid',
    'table-name' = 'substation',
    'username' = 'postgres',
    'password' = 'postgres'
);

-- 创建PostgreSQL Sink表（设备实时监控）
CREATE TABLE device_realtime_monitor_sink (
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
    create_time TIMESTAMP(3),
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
    'table-name' = 'device_realtime_monitor',
    'username' = 'sink_user',
    'password' = 'sink123',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- 启动数据流作业：设备实时监控
INSERT INTO device_realtime_monitor_sink
SELECT 
    d.id as device_id,
    d.device_code,
    d.device_name,
    d.device_type,
    s.name as substation_name,
    m.voltage as latest_voltage,
    m.current_val as latest_current,
    m.power_val as latest_power,
    m.energy as latest_energy,
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
    -- 设备状态
    CASE 
        WHEN m.voltage > 240 OR m.voltage < 200 OR m.temperature > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    m.reading_time as last_update_time,
    CURRENT_TIMESTAMP as create_time
FROM meter_reading_source m
JOIN device_info_source d ON m.device_id = d.id
JOIN substation_source s ON d.substation_id = s.id
WHERE m.reading_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
EOF

echo "执行Flink SQL作业..."
# 在后台执行Flink SQL
nohup /opt/flink/bin/sql-client.sh -f /tmp/flink_simple_pipeline.sql > /app/logs/flink_simple.log 2>&1 &

echo "等待作业启动..."
sleep 20

echo "=== 检查Flink作业状态 ==="
curl -s http://localhost:8081/jobs | python3 -m json.tool 2>/dev/null || echo "无法获取作业状态"

echo "=== 简化数据管道启动完成 ==="
echo "日志文件: /app/logs/flink_simple.log"
echo "监控地址: http://localhost:8081"