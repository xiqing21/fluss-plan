#!/bin/bash

# 国网实时数仓健康检查脚本
set -e

echo "=== 国网实时数仓健康检查 ==="

# 检查SSH服务
echo "检查SSH服务..."
if systemctl is-active --quiet ssh; then
    echo "✓ SSH服务运行正常"
else
    echo "✗ SSH服务异常"
    exit 1
fi

# 检查PostgreSQL
echo "检查PostgreSQL..."
if pg_isready -h localhost -p 5432 -U postgres; then
    echo "✓ PostgreSQL运行正常"
else
    echo "✗ PostgreSQL异常"
    exit 1
fi

# 检查Flink
echo "检查Flink..."
if curl -s http://localhost:8081 > /dev/null; then
    echo "✓ Flink运行正常"
else
    echo "✗ Flink异常"
    exit 1
fi

# 检查Fluss
echo "检查Fluss..."
if curl -s http://localhost:8084 > /dev/null; then
    echo "✓ Fluss运行正常"
else
    echo "✗ Fluss异常"
    exit 1
fi

# 检查Doris FE
echo "检查Doris FE..."
if curl -s http://localhost:8030 > /dev/null; then
    echo "✓ Doris FE运行正常"
else
    echo "✗ Doris FE异常"
    exit 1
fi

# 检查Grafana
echo "检查Grafana..."
if curl -s http://localhost:3000 > /dev/null; then
    echo "✓ Grafana运行正常"
else
    echo "✗ Grafana异常"
    exit 1
fi

# 检查数据库连接
echo "检查数据库连接..."
export PGPASSWORD=postgres
if psql -h localhost -U postgres -d power_grid -c "SELECT COUNT(*) FROM device_info;" > /dev/null; then
    echo "✓ 数据库连接正常"
else
    echo "✗ 数据库连接异常"
    exit 1
fi

# 检查免密登录
echo "检查SSH免密登录..."
if ssh -o BatchMode=yes -o ConnectTimeout=5 localhost echo "SSH免密登录成功" > /dev/null 2>&1; then
    echo "✓ SSH免密登录正常"
else
    echo "✗ SSH免密登录异常"
    exit 1
fi

echo "=== 所有服务健康检查通过 ==="