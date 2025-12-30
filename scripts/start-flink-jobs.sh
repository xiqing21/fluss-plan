#!/bin/bash
# 启动Flink CDC作业脚本

set -e

echo "=== 启动国网实时数仓Flink作业 ==="

# 检查Flink是否运行
if ! curl -s http://localhost:8081/overview > /dev/null; then
    echo "启动Flink集群..."
    /opt/flink/bin/start-cluster.sh
    sleep 10
fi

# 检查Fluss是否运行
if ! curl -s http://localhost:8084 > /dev/null; then
    echo "启动Fluss服务..."
    /opt/fluss/bin/fluss-daemon.sh start coordinator-server
    /opt/fluss/bin/fluss-daemon.sh start tablet-server
    sleep 10
fi

echo "=== 创建PostgreSQL CDC复制槽 ==="
# 创建CDC复制槽
PGPASSWORD=flink psql -h localhost -U flink -d power_grid -c "
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');
" 2>/dev/null || echo "复制槽已存在或创建失败"

echo "=== 启动Flink SQL Client执行CDC作业 ==="

# 创建简化的Flink SQL脚本
cat > /tmp/flink_cdc_simple.sql << 'EOF'
-- 设置Flink配置
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '1h';

-- 创建PostgreSQL CDC源表（电表读数）
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

-- 创建变电站信息源表
CREATE TABLE substation_source (
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

-- 启动实时数据流作业：设备实时监控
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
JOIN substation_source s ON d.substation_id = s.id;
EOF

echo "执行Flink SQL作业..."
# 在后台执行Flink SQL
nohup /opt/flink/bin/sql-client.sh -f /tmp/flink_cdc_simple.sql > /app/logs/flink_cdc.log 2>&1 &

echo "等待作业启动..."
sleep 15

echo "=== 检查Flink作业状态 ==="
curl -s http://localhost:8081/jobs | python3 -m json.tool 2>/dev/null || echo "无法获取作业状态"

echo "=== Flink CDC作业启动完成 ==="
echo "日志文件: /app/logs/flink_cdc.log"
echo "监控地址: http://localhost:8081"