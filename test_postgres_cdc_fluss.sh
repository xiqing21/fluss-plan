#!/bin/bash

# 测试PostgreSQL CDC + Fluss数仓分层
set -e

CONTAINER_NAME="power-grid-realtime"

echo "=== PostgreSQL CDC + Fluss数仓分层测试 ==="

# 检查容器状态
if ! docker ps | grep $CONTAINER_NAME > /dev/null; then
    echo "错误: 容器未运行，请先执行 ./deploy.sh"
    exit 1
fi

echo "✓ 容器运行正常"

# 1. 初始化PostgreSQL数据
echo ""
echo "=== 1. 初始化PostgreSQL测试数据 ==="

docker exec $CONTAINER_NAME bash -c "
    echo '启动PostgreSQL服务...'
    service postgresql start
    sleep 5
    
    echo '创建测试数据库和表...'
    sudo -u postgres psql << 'EOSQL'
-- 创建数据库
DROP DATABASE IF EXISTS power_grid;
CREATE DATABASE power_grid;

-- 连接到数据库
\c power_grid;

-- 创建变电站表
CREATE TABLE substation (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    voltage_level INTEGER,
    capacity DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建设备信息表
CREATE TABLE device_info (
    id SERIAL PRIMARY KEY,
    device_code VARCHAR(50) UNIQUE NOT NULL,
    device_name VARCHAR(100),
    device_type VARCHAR(50),
    substation_id INTEGER REFERENCES substation(id),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    install_date DATE,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建电表读数表
CREATE TABLE meter_reading (
    id BIGSERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES device_info(id),
    reading_time TIMESTAMP NOT NULL,
    voltage DECIMAL(8,2),
    current_val DECIMAL(8,2),
    power_val DECIMAL(10,2),
    energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO substation (name, location, voltage_level, capacity) VALUES
('上海变电站', '上海市浦东新区', 220, 500.00),
('北京变电站', '北京市朝阳区', 500, 1000.00),
('深圳变电站', '深圳市南山区', 110, 300.00);

INSERT INTO device_info (device_code, device_name, device_type, substation_id, manufacturer, model, install_date) VALUES
('DEV001', '主变压器1号', '变压器', 1, '西门子', 'SGB-10', '2023-01-15'),
('DEV002', '主变压器2号', '变压器', 1, 'ABB', 'ONAN-500', '2023-02-20'),
('DEV003', '开关柜1号', '开关设备', 2, '施耐德', 'SM6-24', '2023-03-10'),
('DEV004', '开关柜2号', '开关设备', 2, '西门子', '8DJH-36', '2023-04-05'),
('DEV005', '电容器组', '补偿设备', 3, '思源电气', 'TBB-10', '2023-05-12');

-- 插入一些初始电表读数
INSERT INTO meter_reading (device_id, reading_time, voltage, current_val, power_val, energy, power_factor, frequency, temperature) VALUES
(1, NOW() - INTERVAL '1 hour', 220.5, 15.2, 3350.0, 150.5, 0.95, 50.0, 45.2),
(2, NOW() - INTERVAL '1 hour', 218.8, 16.1, 3520.0, 162.3, 0.92, 49.8, 48.1),
(3, NOW() - INTERVAL '1 hour', 221.2, 14.8, 3270.0, 145.8, 0.96, 50.2, 42.5),
(4, NOW() - INTERVAL '1 hour', 219.6, 15.9, 3490.0, 158.7, 0.94, 49.9, 46.8),
(5, NOW() - INTERVAL '1 hour', 220.1, 15.5, 3410.0, 152.1, 0.93, 50.1, 44.3);

-- 创建CDC用户和权限
CREATE USER flink WITH PASSWORD 'flink' REPLICATION;
GRANT ALL PRIVILEGES ON DATABASE power_grid TO flink;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flink;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flink;

-- 创建复制槽
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('flink_slot2', 'pgoutput');
SELECT pg_create_logical_replication_slot('flink_slot3', 'pgoutput');

\q
EOSQL

    echo '✓ PostgreSQL测试数据初始化完成'
"

# 2. 测试Fluss连接
echo ""
echo "=== 2. 测试Fluss服务 ==="

docker exec $CONTAINER_NAME bash -c "
    echo '等待Fluss服务启动...'
    sleep 30
    
    echo '检查Fluss API...'
    if curl -s http://localhost:9123/api/v1/databases > /dev/null 2>&1; then
        echo '✓ Fluss API 可访问'
        echo 'Fluss数据库列表:'
        curl -s http://localhost:9123/api/v1/databases 2>/dev/null | head -5 || echo '无法获取数据库列表'
    else
        echo '✗ Fluss API 不可访问'
        echo '检查Fluss进程...'
        ps aux | grep fluss || echo '未找到Fluss进程'
    fi
"

# 3. 创建并执行CDC + Fluss测试
echo ""
echo "=== 3. 执行CDC + Fluss数仓分层测试 ==="

# 创建简化的CDC测试脚本
cat > /tmp/cdc_fluss_test.sql << 'EOF'
-- PostgreSQL CDC + Fluss数仓分层测试
SET 'execution.checkpointing.interval' = '30s';

-- CDC源表
CREATE TABLE cdc_meter_reading (
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

-- 输出表
CREATE TABLE cdc_output (
    device_id INT,
    voltage DECIMAL(8,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status STRING,
    layer_info STRING,
    process_time TIMESTAMP(3)
) WITH ('connector' = 'print');

-- 简单的数据处理和输出
INSERT INTO cdc_output
SELECT 
    device_id,
    voltage,
    temperature,
    CASE 
        WHEN voltage BETWEEN 200 AND 240 AND temperature < 80 THEN 95.0
        WHEN voltage BETWEEN 180 AND 260 AND temperature < 90 THEN 80.0
        ELSE 60.0
    END as health_score,
    CASE 
        WHEN voltage > 240 OR voltage < 200 OR temperature > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status,
    'CDC-实时数据层' as layer_info,
    CURRENT_TIMESTAMP as process_time
FROM cdc_meter_reading;
EOF

# 将测试脚本复制到容器并执行
docker cp /tmp/cdc_fluss_test.sql $CONTAINER_NAME:/tmp/

echo "启动CDC测试..."
docker exec $CONTAINER_NAME bash -c "
    cd /app
    
    echo '检查PostgreSQL CDC连接器...'
    if find /opt/flink/lib -name '*postgres*cdc*.jar' | head -1 > /dev/null; then
        echo '✓ 找到PostgreSQL CDC连接器'
        find /opt/flink/lib -name '*postgres*cdc*.jar'
    else
        echo '✗ 未找到PostgreSQL CDC连接器'
        echo '可用的连接器:'
        ls /opt/flink/lib/*cdc*.jar 2>/dev/null || echo '无CDC连接器'
    fi
    
    echo '启动CDC测试作业...'
    timeout 90s /opt/flink/bin/sql-client.sh -f /tmp/cdc_fluss_test.sql > /tmp/cdc_test.log 2>&1 &
    CDC_PID=\$!
    
    echo '等待CDC作业启动...'
    sleep 20
    
    echo '插入新的测试数据...'
    sudo -u postgres psql power_grid << 'EOSQL'
INSERT INTO meter_reading (device_id, reading_time, voltage, current_val, power_val, energy, power_factor, frequency, temperature) VALUES
(1, NOW(), 225.0, 16.0, 3600.0, 160.0, 0.94, 50.0, 50.0),
(2, NOW(), 215.0, 15.5, 3330.0, 155.0, 0.91, 49.9, 55.0),
(3, NOW(), 230.0, 17.0, 3910.0, 170.0, 0.97, 50.1, 60.0);
EOSQL
    
    echo '等待CDC处理...'
    sleep 30
    
    echo '检查CDC测试结果...'
    if grep -q 'CDC-实时数据层' /tmp/cdc_test.log 2>/dev/null; then
        echo '✓ CDC测试成功，发现输出数据'
        echo 'CDC输出示例:'
        grep 'CDC-实时数据层' /tmp/cdc_test.log | head -3
    else
        echo '✗ CDC测试可能失败'
        echo 'CDC测试日志:'
        tail -20 /tmp/cdc_test.log 2>/dev/null || echo '无日志文件'
    fi
    
    # 检查Flink作业状态
    echo '检查Flink作业状态...'
    JOBS=\$(curl -s http://localhost:8081/jobs 2>/dev/null | grep -o '\"state\":\"RUNNING\"' | wc -l || echo '0')
    echo \"运行中的作业数: \$JOBS\"
    
    # 停止测试作业
    kill \$CDC_PID 2>/dev/null || true
"

# 4. 数据生成器测试
echo ""
echo "=== 4. 启动数据生成器 ==="

docker exec $CONTAINER_NAME bash -c "
    cd /app
    
    echo '启动数据生成器...'
    python3 scripts/data_generator.py > /tmp/datagen.log 2>&1 &
    DATAGEN_PID=\$!
    
    echo '等待数据生成...'
    sleep 15
    
    echo '检查生成的数据...'
    sudo -u postgres psql power_grid -c 'SELECT COUNT(*) as total_records FROM meter_reading;'
    sudo -u postgres psql power_grid -c 'SELECT device_id, COUNT(*) as records FROM meter_reading GROUP BY device_id ORDER BY device_id;'
    
    echo '停止数据生成器...'
    kill \$DATAGEN_PID 2>/dev/null || true
    
    echo '✓ 数据生成器测试完成'
"

# 5. 生成测试报告
echo ""
echo "=== 测试报告 ==="
echo "时间: $(date)"
echo ""

# 检查服务状态
echo "服务状态检查:"
docker exec $CONTAINER_NAME bash -c "
    echo '- PostgreSQL:' \$(pg_isready -h localhost -p 5432 > /dev/null 2>&1 && echo '✓ 运行中' || echo '✗ 异常')
    echo '- Flink:' \$(curl -s http://localhost:8081/overview > /dev/null 2>&1 && echo '✓ 运行中' || echo '✗ 异常')
    echo '- Fluss:' \$(curl -s http://localhost:9123 > /dev/null 2>&1 && echo '✓ 运行中' || echo '✗ 异常')
"

# 数据统计
echo ""
echo "数据统计:"
docker exec $CONTAINER_NAME bash -c "
    sudo -u postgres psql power_grid << 'EOSQL'
SELECT 
    'substation' as table_name, 
    COUNT(*) as record_count 
FROM substation
UNION ALL
SELECT 
    'device_info' as table_name, 
    COUNT(*) as record_count 
FROM device_info
UNION ALL
SELECT 
    'meter_reading' as table_name, 
    COUNT(*) as record_count 
FROM meter_reading;
EOSQL
"

echo ""
echo "=== 测试完成 ==="
echo "如需进一步测试:"
echo "1. 进入容器: docker exec -it $CONTAINER_NAME bash"
echo "2. 查看日志: docker logs $CONTAINER_NAME"
echo "3. 访问Flink UI: http://localhost:8081"
echo "4. 访问Fluss UI: http://localhost:8084"

# 清理临时文件
rm -f /tmp/cdc_fluss_test.sql

echo ""
echo "PostgreSQL CDC + Fluss测试脚本执行完成！"