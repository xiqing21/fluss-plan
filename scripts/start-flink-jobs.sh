#!/bin/bash
# 启动完整数仓分层作业 - 使用datagen + Fluss + 窗口汇总 + PostgreSQL Sink

set -e

echo "=== 启动国网实时数仓完整分层作业 ==="

echo "=== 检查PostgreSQL Sink数据库连接 ==="
PGPASSWORD=sink123 psql -h localhost -U sink_user -d power_grid_dw -c "SELECT 'PostgreSQL Sink连接正常';" || {
    echo "PostgreSQL Sink连接失败，请检查数据库配置"
    exit 1
}

echo "=== 启动完整数仓分层作业（datagen + Fluss + 窗口汇总） ==="

# 执行完整的数仓分层SQL脚本
echo "执行Flink数仓分层作业..."
nohup /opt/flink/bin/sql-client.sh -f /app/sql/flink_dw_datagen.sql > /app/logs/flink_dw_datagen.log 2>&1 &

echo "等待作业启动..."
sleep 15

echo "=== 检查Flink作业状态 ==="
curl -s http://localhost:8081/jobs | python3 -m json.tool 2>/dev/null || echo "无法获取作业状态"

echo "=== 数仓分层作业启动完成 ==="
echo "日志文件: /app/logs/flink_dw_datagen.log"
echo "监控地址: http://localhost:8081"

echo "=== 查看作业输出（最近30行） ==="
sleep 5
tail -30 /app/logs/flink_dw_datagen.log || echo "暂无输出"

echo ""
echo "=== 数据流架构 ==="
echo "ODS(datagen) -> ODS(Fluss) -> DWD(Fluss) -> DWS(Fluss,30s窗口) -> ADS(PostgreSQL)"
echo "包含："
echo "- 设备明细数据处理（DWD层）"
echo "- 30秒窗口汇总（DWS层）"
echo "- 变电站汇总（DWS层）"
echo "- PostgreSQL Sink输出（ADS层）"
echo "- 控制台监控输出"