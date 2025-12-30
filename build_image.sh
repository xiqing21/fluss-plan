#!/bin/bash

# 国网实时数仓镜像构建脚本
set -e

IMAGE_NAME="power-grid-realtime"
TAG="2.0"

echo "=== 构建国网实时数仓镜像 ==="
echo "镜像名称: $IMAGE_NAME:$TAG"
echo "使用代理: http://host.docker.internal:7890"

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误: Docker未运行，请先启动Docker"
    exit 1
fi

# 构建镜像
echo "开始构建镜像..."
docker build \
    --platform linux/amd64 \
    --build-arg HTTP_PROXY=http://host.docker.internal:7890 \
    --build-arg HTTPS_PROXY=http://host.docker.internal:7890 \
    --network=host \
    -t $IMAGE_NAME:$TAG .

echo "✓ 镜像构建完成"

# 检查镜像大小
echo "检查镜像信息..."
docker images $IMAGE_NAME:$TAG

# 保存镜像为tar包
echo "保存镜像为tar包..."
docker save $IMAGE_NAME:$TAG | gzip > ${IMAGE_NAME}_${TAG}.tar.gz

# 计算镜像信息
IMAGE_SIZE=$(docker images $IMAGE_NAME:$TAG --format "table {{.Size}}" | tail -n 1)
TAR_SIZE=$(du -h ${IMAGE_NAME}_${TAG}.tar.gz | cut -f1)

echo "=== 构建完成 ==="
echo "镜像大小: $IMAGE_SIZE"
echo "压缩包大小: $TAR_SIZE"
echo "压缩包位置: $(pwd)/${IMAGE_NAME}_${TAG}.tar.gz"

# 生成部署脚本
cat > deploy.sh << EOF
#!/bin/bash

# 国网实时数仓部署脚本
set -e

IMAGE_NAME="$IMAGE_NAME"
TAG="$TAG"
CONTAINER_NAME="power-grid-realtime"

echo "=== 部署国网实时数仓 ==="

# 停止并删除现有容器
if docker ps -a | grep \$CONTAINER_NAME > /dev/null; then
    echo "停止现有容器..."
    docker stop \$CONTAINER_NAME || true
    docker rm \$CONTAINER_NAME || true
fi

# 加载镜像（如果需要）
if [ -f "${IMAGE_NAME}_${TAG}.tar.gz" ]; then
    echo "加载镜像..."
    docker load < ${IMAGE_NAME}_${TAG}.tar.gz
fi

# 运行容器
echo "启动容器..."
docker run -d \\
    --name \$CONTAINER_NAME \\
    -p 2222:22 \\
    -p 5432:5432 \\
    -p 8081:8081 \\
    -p 8084:8084 \\
    -p 9030:9030 \\
    -p 8030:8030 \\
    -p 3000:3000 \\
    \$IMAGE_NAME:\$TAG

echo "等待服务启动..."
sleep 60

# 检查容器状态
if docker ps | grep \$CONTAINER_NAME > /dev/null; then
    echo "✓ 容器启动成功"
    echo ""
    echo "服务访问地址:"
    echo "- SSH: ssh root@localhost -p 2222 (密码: root123)"
    echo "- Flink Web UI: http://localhost:8081"
    echo "- Fluss Web UI: http://localhost:8084"
    echo "- Doris FE: http://localhost:9030"
    echo "- Grafana: http://localhost:3000 (admin/admin)"
    echo ""
    echo "容器日志: docker logs \$CONTAINER_NAME"
else
    echo "✗ 容器启动失败"
    docker logs \$CONTAINER_NAME
    exit 1
fi
EOF

chmod +x deploy.sh
echo "部署脚本: deploy.sh"

echo ""
echo "=== 使用方法 ==="
echo "部署: ./deploy.sh"