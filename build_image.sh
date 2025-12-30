#!/bin/bash

# 国网实时数仓镜像构建脚本
set -e

# 构建参数配置
IMAGE_NAME="power-grid-realtime"
TAG="2.0"
PROXY="http://host.docker.internal:7890"

echo "=== 构建国网实时数仓镜像 ==="
echo "镜像名称: $IMAGE_NAME:$TAG"
echo "代理设置: $PROXY"

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误: Docker未运行，请先启动Docker"
    exit 1
fi

# 检查代理是否可用
if ! curl -x $PROXY --connect-timeout 5 https://www.google.com > /dev/null 2>&1; then
    echo "警告: 代理 $PROXY 不可用，将使用直连"
    PROXY=""
fi

# 构建镜像
echo "开始构建镜像..."
if [ -n "$PROXY" ]; then
    docker build \
        --build-arg HTTP_PROXY=$PROXY \
        --build-arg HTTPS_PROXY=$PROXY \
        --no-cache \
        -t $IMAGE_NAME:$TAG .
else
    docker build --no-cache -t $IMAGE_NAME:$TAG .
fi

echo "✓ 镜像构建完成"

# 检查镜像大小
echo "检查镜像信息..."
docker images $IMAGE_NAME:$TAG

# 运行基础健康检查
echo "运行基础健康检查..."
CONTAINER_ID=$(docker run -d -p 2222:22 -p 5432:5432 -p 8081:8081 -p 3000:3000 $IMAGE_NAME:$TAG)

echo "等待容器启动..."
sleep 30

# 检查容器状态
if docker ps | grep $CONTAINER_ID > /dev/null; then
    echo "✓ 容器启动成功"
    
    # 等待服务初始化
    echo "等待服务初始化..."
    sleep 60
    
    # 运行健康检查
    echo "运行健康检查..."
    if docker exec $CONTAINER_ID /app/scripts/healthcheck.sh; then
        echo "✓ 健康检查通过"
    else
        echo "✗ 健康检查失败"
        docker logs $CONTAINER_ID
    fi
    
    # 测试SSH连接
    echo "测试SSH连接..."
    if sshpass -p root123 ssh -o StrictHostKeyChecking=no -p 2222 root@localhost echo "SSH连接成功"; then
        echo "✓ SSH连接测试通过"
    else
        echo "✗ SSH连接测试失败"
    fi
    
else
    echo "✗ 容器启动失败"
    docker logs $CONTAINER_ID
    exit 1
fi

# 清理测试容器
echo "清理测试容器..."
docker stop $CONTAINER_ID
docker rm $CONTAINER_ID

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

# 创建版本说明文件
cat > VERSION_${TAG}.md << EOF
# 国网实时数仓测试环境 v${TAG}

## 构建信息
- 构建时间: $(date)
- 镜像大小: $IMAGE_SIZE
- 压缩包大小: $TAR_SIZE

## 包含组件
- PostgreSQL 13 (CDC源数据库)
- Apache Flink 2.2.0 + CDC连接器
- Apache Fluss 0.8 (流存储)
- Apache Doris 1.2.7 (OLAP数据库)
- Grafana (数据可视化)
- SSH服务 (免密登录支持)
- Python数据生成器

## 端口映射
- 22: SSH服务
- 5432: PostgreSQL
- 8081: Flink Web UI
- 8084: Fluss Web UI
- 9030: Doris FE
- 8030: Doris BE
- 3000: Grafana

## 默认账户
- SSH: root/root123
- PostgreSQL: postgres/postgres
- Doris: root/
- Grafana: admin/admin

## 使用方法
1. 加载镜像: docker load < ${IMAGE_NAME}_${TAG}.tar.gz
2. 运行容器: docker run -d -p 2222:22 -p 5432:5432 -p 8081:8081 -p 3000:3000 $IMAGE_NAME:$TAG
3. SSH连接: ssh root@localhost -p 2222
4. 访问服务: 浏览器打开对应端口

## 代理支持
- 支持HTTP代理: host.docker.internal:7890
- 自动配置APT和pip代理
EOF

echo "版本说明文件: VERSION_${TAG}.md"

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
    -e HTTP_PROXY=http://host.docker.internal:7890 \\
    -e HTTPS_PROXY=http://host.docker.internal:7890 \\
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
echo "=== 构建和测试完成 ==="
echo "可以使用以下命令部署:"
echo "./deploy.sh"