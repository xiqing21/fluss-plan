#!/bin/bash

# 国网实时数仓服务初始化脚本
set -e

echo "开始初始化服务..."

# 等待PostgreSQL启动
echo "等待PostgreSQL启动..."
while ! pg_isready -h localhost -p 5432 -U postgres; do
    sleep 2
done

# 初始化PostgreSQL数据库
echo "初始化PostgreSQL数据库..."
export PGPASSWORD=postgres
psql -h localhost -U postgres -d power_grid -f /app/sql/init_postgresql.sql

# 等待Doris FE启动
echo "等待Doris FE启动..."
sleep 30
while ! curl -s http://localhost:8030 > /dev/null; do
    sleep 5
done

# 初始化Doris数据库
echo "初始化Doris数据库..."
mysql -h127.0.0.1 -P9030 -uroot < /app/sql/init_doris.sql

# 启动数据生成器
echo "启动数据生成器..."
python3 /app/scripts/data_generator.py &

# 启动Flink作业
echo "启动Flink CDC作业..."
/app/scripts/start-flink-jobs.sh

echo "服务初始化完成！"