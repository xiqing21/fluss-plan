#!/bin/bash
# 启动Flink作业脚本 - 使用CDC连接器和完整数仓分层

set -e

echo "=== 启动国网实时数仓Flink作业 ==="

echo "=== 创建PostgreSQL CDC复制槽 ==="
# 创建CDC复制槽（如果不存在）
PGPASSWORD=flink psql -h localhost -U flink -d power_grid -c "
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');
" 2>/dev/null || echo "复制槽已存在"

echo "=== 启动完整数仓分层作业 ==="

# 执行完整的数仓分层SQL脚本
echo "执行Flink SQL作业..."
nohup /opt/flink/bin/sql-client.sh -f /app/sql/flink_jobs.sql > /app/logs/flink_dw.log 2>&1 &

echo "等待作业启动..."
sleep 10

echo "=== 检查Flink作业状态 ==="
curl -s http://localhost:8081/jobs | python3 -m json.tool 2>/dev/null || echo "无法获取作业状态"

echo "=== 数仓分层作业启动完成 ==="
echo "日志文件: /app/logs/flink_dw.log"
echo "监控地址: http://localhost:8081"

echo "=== 查看作业输出 ==="
sleep 5
tail -30 /app/logs/flink_dw.log || echo "暂无输出"