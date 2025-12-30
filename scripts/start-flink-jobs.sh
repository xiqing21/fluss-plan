#!/bin/bash

# 启动Flink CDC作业脚本
set -e

echo "启动Flink作业..."

# 等待Flink集群启动
echo "等待Flink集群启动..."
while ! curl -s http://localhost:8081 > /dev/null; do
    sleep 5
done

# 等待Fluss启动
echo "等待Fluss启动..."
while ! curl -s http://localhost:8084 > /dev/null; do
    sleep 5
done

# 设置Flink环境变量
export FLINK_HOME=/opt/flink
export PATH=$FLINK_HOME/bin:$PATH

# 下载必要的连接器JAR包
FLINK_LIB_DIR=$FLINK_HOME/lib
CONNECTOR_DIR=/tmp/connectors

mkdir -p $CONNECTOR_DIR

# 下载PostgreSQL CDC连接器
if [ ! -f "$FLINK_LIB_DIR/flink-sql-connector-postgres-cdc-3.2.0.jar" ]; then
    echo "下载PostgreSQL CDC连接器..."
    wget -O $CONNECTOR_DIR/flink-sql-connector-postgres-cdc-3.2.0.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.2.0/flink-sql-connector-postgres-cdc-3.2.0.jar
    cp $CONNECTOR_DIR/flink-sql-connector-postgres-cdc-3.2.0.jar $FLINK_LIB_DIR/
fi

# 下载Doris连接器
if [ ! -f "$FLINK_LIB_DIR/flink-doris-connector-1.18-1.6.1.jar" ]; then
    echo "下载Doris连接器..."
    wget -O $CONNECTOR_DIR/flink-doris-connector-1.18-1.6.1.jar \
        https://repo1.maven.org/maven2/org/apache/doris/flink-doris-connector-1.18/1.6.1/flink-doris-connector-1.18-1.6.1.jar
    cp $CONNECTOR_DIR/flink-doris-connector-1.18-1.6.1.jar $FLINK_LIB_DIR/
fi

# 重启Flink集群以加载新的连接器
echo "重启Flink集群..."
$FLINK_HOME/bin/stop-cluster.sh
sleep 5
$FLINK_HOME/bin/start-cluster.sh

# 等待Flink重新启动
echo "等待Flink重新启动..."
while ! curl -s http://localhost:8081 > /dev/null; do
    sleep 5
done

# 提交Flink SQL作业
echo "提交Flink SQL作业..."

# 1. 提交ODS和DWD层作业
echo "提交ODS和DWD层作业..."
$FLINK_HOME/bin/sql-client.sh -f /app/sql/flink_jobs.sql

# 2. 提交DWS层作业
echo "提交DWS层作业..."
$FLINK_HOME/bin/sql-client.sh -f /app/sql/dws_layer_jobs.sql

echo "Flink作业启动完成！"