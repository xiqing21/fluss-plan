#!/bin/bash

# 国网实时数仓部署脚本
set -e

IMAGE_NAME="power-grid-realtime"
TAG="2.0"
CONTAINER_NAME="power-grid-realtime"

echo "=== 部署国网实时数仓 ==="

# 停止并删除现有容器
if docker ps -a | grep $CONTAINER_NAME > /dev/null; then
    echo "停止现有容器..."
    docker stop $CONTAINER_NAME || true
    docker rm $CONTAINER_NAME || true
fi

# 加载镜像（如果需要）
if [ -f "power-grid-realtime_2.0.tar.gz" ]; then
    echo "加载镜像..."
    docker load < power-grid-realtime_2.0.tar.gz
fi

# 运行容器
echo "启动容器..."
docker run -d \
    --name $CONTAINER_NAME \
    -p 2222:22 \
    -p 5432:5432 \
    -p 8081:8081 \
    -p 8084:8084 \
    -p 3000:3000 \
    $IMAGE_NAME:$TAG

echo "等待服务启动..."
sleep 10

# 检查容器状态
if docker ps | grep $CONTAINER_NAME > /dev/null; then
    echo "✓ 容器启动成功"
    echo ""
    echo "服务访问地址:"
    echo "- SSH: ssh root@localhost -p 2222 (密码: root123)"
    echo "- Flink Web UI: http://localhost:8081"
    echo "- Fluss Web UI: http://localhost:8084"
    echo "- Grafana: http://localhost:3000 (admin/admin)"
    echo ""
    echo "数据库连接信息:"
    echo "- Source PostgreSQL: localhost:5432/power_grid (postgres/postgres)"
    echo "- Sink PostgreSQL: localhost:5432/power_grid_dw (sink_user/sink123)"
    echo ""
    echo "容器日志: docker logs $CONTAINER_NAME"
else
    echo "✗ 容器启动失败"
    docker logs $CONTAINER_NAME
    exit 1
fi
