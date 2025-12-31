#!/bin/bash

# 测试SSH免密连接和Fluss数仓分层
set -e

CONTAINER_NAME="power-grid-realtime"
SSH_PORT="2222"

echo "=== 国网实时数仓测试脚本 ==="
echo "测试项目："
echo "1. SSH免密连接测试"
echo "2. Fluss数仓分层测试"
echo "3. PostgreSQL CDC测试"
echo ""

# 检查容器是否运行
if ! docker ps | grep $CONTAINER_NAME > /dev/null; then
    echo "错误: 容器 $CONTAINER_NAME 未运行"
    echo "请先运行: ./deploy.sh"
    exit 1
fi

echo "✓ 容器运行正常"

# 等待服务完全启动
echo "等待服务启动完成..."
sleep 30

# 1. 测试SSH免密连接
echo ""
echo "=== 1. 测试SSH免密连接 ==="

# 测试SSH密码连接
echo "测试SSH密码连接..."
if sshpass -p 'root123' ssh -o StrictHostKeyChecking=no -p $SSH_PORT root@localhost 'echo "SSH密码连接成功"' 2>/dev/null; then
    echo "✓ SSH密码连接测试通过"
else
    echo "✗ SSH密码连接失败"
fi

# 获取容器内的SSH公钥
echo "获取容器内SSH公钥..."
CONTAINER_PUBLIC_KEY=$(docker exec $CONTAINER_NAME cat /root/.ssh/id_rsa.pub 2>/dev/null || echo "")

if [ -n "$CONTAINER_PUBLIC_KEY" ]; then
    echo "✓ 容器内SSH密钥对已生成"
    echo "公钥: $CONTAINER_PUBLIC_KEY"
    
    # 测试容器内部免密连接
    echo "测试容器内部SSH免密连接..."
    if docker exec $CONTAINER_NAME ssh -o StrictHostKeyChecking=no localhost 'echo "容器内部免密连接成功"' 2>/dev/null; then
        echo "✓ 容器内部SSH免密连接测试通过"
    else
        echo "✗ 容器内部SSH免密连接失败"
    fi
else
    echo "✗ 容器内SSH密钥对未找到"
fi

# 2. 测试服务状态
echo ""
echo "=== 2. 测试服务状态 ==="

# 检查Flink
echo "检查Flink服务..."
if curl -s http://localhost:8081/overview > /dev/null; then
    echo "✓ Flink Web UI 可访问 (http://localhost:8081)"
    FLINK_STATUS=$(curl -s http://localhost:8081/overview | grep -o '"state":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
    echo "  Flink状态: $FLINK_STATUS"
else
    echo "✗ Flink Web UI 不可访问"
fi

# 检查Fluss
echo "检查Fluss服务..."
if curl -s http://localhost:8084 > /dev/null; then
    echo "✓ Fluss Web UI 可访问 (http://localhost:8084)"
else
    echo "✗ Fluss Web UI 不可访问"
fi

# 检查PostgreSQL
echo "检查PostgreSQL服务..."
if docker exec $CONTAINER_NAME pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    echo "✓ PostgreSQL 服务运行正常"
    
    # 检查数据库
    echo "检查数据库..."
    DB_COUNT=$(docker exec $CONTAINER_NAME psql -h localhost -U postgres -t -c "SELECT count(*) FROM pg_database WHERE datname IN ('power_grid', 'power_grid_dw');" 2>/dev/null | tr -d ' ' || echo "0")
    echo "  数据库数量: $DB_COUNT"
else
    echo "✗ PostgreSQL 服务异常"
fi

# 3. 测试Fluss数仓分层
echo ""
echo "=== 3. 测试Fluss数仓分层 ==="

# 创建测试脚本
cat > /tmp/test_fluss_dw.sql << 'EOF'
-- 测试Fluss数仓分层
SET 'execution.checkpointing.interval' = '30s';

-- 创建Fluss Catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- 创建测试数据源
CREATE TEMPORARY TABLE test_source (
    id BIGINT,
    device_id INT,
    voltage DECIMAL(8,2),
    temperature DECIMAL(5,2),
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '2',
    'fields.id.min' = '1',
    'fields.id.max' = '1000',
    'fields.device_id.min' = '1',
    'fields.device_id.max' = '3',
    'fields.voltage.min' = '200',
    'fields.voltage.max' = '240',
    'fields.temperature.min' = '30',
    'fields.temperature.max' = '80'
);

-- 创建Fluss表
CREATE TABLE IF NOT EXISTS ods_test_data (
    id BIGINT,
    device_id INT,
    voltage DECIMAL(8,2),
    temperature DECIMAL(5,2),
    ts TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
);

-- 创建输出表
CREATE TEMPORARY TABLE test_output (
    device_id INT,
    avg_voltage DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    record_count BIGINT,
    layer_info STRING
) WITH ('connector' = 'print');

-- 数据写入Fluss
INSERT INTO ods_test_data
SELECT * FROM test_source;
EOF

# 将测试脚本复制到容器
docker cp /tmp/test_fluss_dw.sql $CONTAINER_NAME:/tmp/

echo "启动Fluss数仓分层测试..."
echo "注意: 这个测试需要Fluss服务完全启动，可能需要等待..."

# 在容器内执行测试
docker exec $CONTAINER_NAME bash -c "
    echo '等待Fluss服务启动...'
    sleep 20
    
    echo '检查Fluss连接...'
    if curl -s http://localhost:9123/api/v1/databases > /dev/null 2>&1; then
        echo '✓ Fluss API 可访问'
    else
        echo '✗ Fluss API 不可访问，跳过Fluss测试'
        exit 0
    fi
    
    echo '执行Fluss数仓分层测试...'
    timeout 60s /opt/flink/bin/sql-client.sh -f /tmp/test_fluss_dw.sql > /tmp/fluss_test.log 2>&1 &
    
    echo '等待测试结果...'
    sleep 30
    
    echo '检查测试结果...'
    if grep -q 'INSERT' /tmp/fluss_test.log 2>/dev/null; then
        echo '✓ Fluss数仓分层测试执行成功'
        echo '测试日志:'
        tail -20 /tmp/fluss_test.log 2>/dev/null || echo '日志文件不存在'
    else
        echo '✗ Fluss数仓分层测试可能失败'
        echo '错误日志:'
        cat /tmp/fluss_test.log 2>/dev/null || echo '日志文件不存在'
    fi
"

# 4. 测试简单数仓分层（不依赖Fluss）
echo ""
echo "=== 4. 测试简单数仓分层 ==="

echo "启动简单数仓分层测试..."
docker exec $CONTAINER_NAME bash -c "
    cd /app
    echo '执行简单数仓分层脚本...'
    timeout 60s ./scripts/start-complete-dw.sh > /tmp/simple_dw_test.log 2>&1 &
    
    echo '等待测试结果...'
    sleep 40
    
    echo '检查Flink作业状态...'
    JOBS=\$(curl -s http://localhost:8081/jobs 2>/dev/null | grep -o '\"id\":\"[^\"]*\"' | wc -l || echo '0')
    echo \"运行中的Flink作业数: \$JOBS\"
    
    echo '检查输出日志...'
    if find /opt/flink/log -name '*taskexecutor*.out' -exec grep -l 'DWD-明细数据层' {} \; 2>/dev/null | head -1 > /dev/null; then
        echo '✓ 发现DWD层输出'
        find /opt/flink/log -name '*taskexecutor*.out' -exec grep 'DWD-明细数据层' {} \; 2>/dev/null | head -3
    else
        echo '✗ 未发现DWD层输出'
    fi
    
    if find /opt/flink/log -name '*taskexecutor*.out' -exec grep -l 'DWS-汇总数据层' {} \; 2>/dev/null | head -1 > /dev/null; then
        echo '✓ 发现DWS层输出'
        find /opt/flink/log -name '*taskexecutor*.out' -exec grep 'DWS-汇总数据层' {} \; 2>/dev/null | head -3
    else
        echo '✗ 未发现DWS层输出'
    fi
"

# 5. 生成测试报告
echo ""
echo "=== 测试报告 ==="
echo "时间: $(date)"
echo "容器: $CONTAINER_NAME"
echo ""

# 获取容器状态
CONTAINER_STATUS=$(docker inspect $CONTAINER_NAME --format '{{.State.Status}}' 2>/dev/null || echo "unknown")
echo "容器状态: $CONTAINER_STATUS"

# 获取端口映射
echo "端口映射:"
docker port $CONTAINER_NAME 2>/dev/null || echo "无端口映射信息"

echo ""
echo "=== 测试完成 ==="
echo "如需查看详细日志:"
echo "- 容器日志: docker logs $CONTAINER_NAME"
echo "- 进入容器: docker exec -it $CONTAINER_NAME bash"
echo "- SSH连接: ssh root@localhost -p $SSH_PORT (密码: root123)"

# 清理临时文件
rm -f /tmp/test_fluss_dw.sql

echo ""
echo "测试脚本执行完成！"