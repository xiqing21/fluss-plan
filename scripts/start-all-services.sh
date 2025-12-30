#!/bin/bash

# 启动所有服务脚本
set -e

echo "=== 启动国网实时数仓所有服务 ==="

# 检查是否以root用户运行
if [ "$EUID" -ne 0 ]; then
    echo "请以root用户运行此脚本"
    exit 1
fi

# 启动SSH服务
echo "启动SSH服务..."
service ssh start
echo "✓ SSH服务已启动"

# 启动PostgreSQL
echo "启动PostgreSQL..."
service postgresql start
echo "✓ PostgreSQL已启动"

# 等待PostgreSQL完全启动
echo "等待PostgreSQL完全启动..."
while ! pg_isready -h localhost -p 5432 -U postgres; do
    sleep 2
done

# 初始化PostgreSQL数据（如果需要）
echo "检查PostgreSQL数据初始化..."
export PGPASSWORD=postgres
if ! psql -h localhost -U postgres -d power_grid -c "SELECT 1 FROM device_info LIMIT 1;" > /dev/null 2>&1; then
    echo "初始化PostgreSQL数据..."
    psql -h localhost -U postgres -d power_grid -f /app/sql/init_postgresql.sql
    echo "✓ PostgreSQL数据初始化完成"
fi

# 启动Flink集群
echo "启动Flink集群..."
export FLINK_HOME=/opt/flink
$FLINK_HOME/bin/start-cluster.sh
echo "✓ Flink集群已启动"

# 启动Fluss
echo "启动Fluss..."
export FLUSS_HOME=/opt/fluss
$FLUSS_HOME/bin/fluss-daemon.sh start server
echo "✓ Fluss已启动"

# 启动Doris FE
echo "启动Doris FE..."
cd /opt/doris/fe
./bin/start_fe.sh --daemon
echo "✓ Doris FE已启动"

# 等待Doris FE启动
echo "等待Doris FE完全启动..."
sleep 30
while ! curl -s http://localhost:8030 > /dev/null; do
    sleep 5
done

# 启动Doris BE
echo "启动Doris BE..."
cd /opt/doris/be
./bin/start_be.sh --daemon
echo "✓ Doris BE已启动"

# 初始化Doris数据（如果需要）
echo "检查Doris数据初始化..."
if ! mysql -h127.0.0.1 -P9030 -uroot -e "USE power_grid_dw; SELECT 1;" > /dev/null 2>&1; then
    echo "初始化Doris数据..."
    mysql -h127.0.0.1 -P9030 -uroot < /app/sql/init_doris.sql
    echo "✓ Doris数据初始化完成"
fi

# 启动Grafana
echo "启动Grafana..."
service grafana-server start
echo "✓ Grafana已启动"

# 等待所有服务启动完成
echo "等待所有服务完全启动..."
sleep 10

# 运行健康检查
echo "运行健康检查..."
/app/scripts/healthcheck.sh

# 启动数据生成器
echo "启动数据生成器..."
nohup python3 /app/scripts/data_generator.py > /app/logs/data_gen.log 2>&1 &
echo "✓ 数据生成器已启动"

# 启动Flink作业
echo "启动Flink CDC作业..."
/app/scripts/start-flink-jobs.sh

echo "=== 所有服务启动完成 ==="
echo ""
echo "服务访问地址:"
echo "- Flink Web UI: http://localhost:8081"
echo "- Fluss Web UI: http://localhost:8084"
echo "- Doris FE: http://localhost:8030"
echo "- Grafana: http://localhost:3000 (admin/admin)"
echo "- SSH: ssh root@localhost -p 22 (密码: root123)"
echo ""
echo "数据库连接:"
echo "- PostgreSQL: localhost:5432/power_grid (postgres/postgres)"
echo "- Doris: localhost:9030/power_grid_dw (root/)"
echo ""
echo "日志文件位置: /app/logs/"