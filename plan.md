# 国网实时数仓测试环境构建与验证完整计划（PostgreSQL Sink版本）
注意：以下 SQL 脚本基于 Flink SQL 语法，假设已配置 Fluss Catalog（`bootstrap.servers=localhost:9123`）。所有表需一次执行（保存为 `.sql` 文件，通过 `sql-client.sh` 运行）。
运行时需要通过 ssh 连接避免直接进入镜像导致无法与 ai 对话框交互。
另外步骤为先编写 dockerfile 保证必要组件安装环境满足后再测试数据流转

## 基础镜像使用方法
**基础镜像**: xuyangzzz/delta_join_example:1.0

### 基础镜像测试命令
```bash
# 运行基础镜像进入命令行
docker run -p 8081:8081 -p 9123:9123 --name delta_join_example -it xuyangzzz/delta_join_example:1.0 bash

# 在容器内运行以下命令测试基础环境
./start-flink-fluss.sh                    # 启动 flink 和 fluss 集群
./create-tables-and-run-delta-join.sh     # 创建相关表和 delta join 作业
./insert-data.sh                          # 在源表插入数据

# 访问 Flink UI: http://localhost:8081
```

## 一、基础环境准备计划

### 1.1 基础镜像选择与SSH配置
- **基础镜像**: xuyangzzz/delta_join_example:1.0（已包含Flink 2.2.0 + Fluss 0.8）
- **SSH服务器安装**: openssh-server
- **SSH配置**:
  - 允许root登录
  - 设置密码认证
  - 生成SSH密钥对
  - 配置免密登录（公钥到authorized_keys）
  - 固定root密码为root123
- **代理配置**:
  - 环境变量: HTTP_PROXY=http://host.docker.internal:7890
  - APT代理配置
  - Python pip代理配置


## 二、核心组件安装计划

### 2.1 数据库层组件
1. **PostgreSQL 13**（源数据库）
   - 安装与配置
   - 初始化数据库：power_grid
   - 创建国网业务测试表结构
   - 配置CDC复制槽（Flink CDC需要）
   - 用户：postgres/postgres, flink/flink

2. **PostgreSQL 13**（Sink数据库）
   - 使用同一PostgreSQL实例
   - 初始化数据库：power_grid_dw
   - 创建数仓分层表结构（ODS/DWD/DWS/ADS）
   - 用户：sink_user/sink123



### 2.2 数据处理层组件
1. **Apache Flink 2.2.0**（镜像自带）
   - 验证JobManager/TaskManager状态
   - 检查Web UI端口(8081)
   - 启动命令：/opt/flink/bin/start-cluster.sh

2. **Apache Fluss 0.8**（镜像自带）
   - 验证运行状态
   - 检查端口(9123, 8084)
   - 启动命令：/opt/fluss/bin/fluss-daemon.sh start coordinator-server

3. **Flink连接器**
   - PostgreSQL CDC连接器
   - PostgreSQL JDBC连接器
   - Kafka连接器（备用）
1. **Apache Flink 2.2.0**（镜像自带）
   - 验证JobManager/TaskManager状态
   - 检查Web UI端口(8081)

2. **Apache Fluss 0.8**（镜像自带）
   - 验证运行状态
   - 检查端口(9123, 8084)

3. **Flink CDC连接器**
   - PostgreSQL CDC连接器
   - Doris连接器
   - Kafka连接器（备用）

### 2.3 数据可视化层
1. **Grafana**
   - 安装与配置
   - 初始化管理员账户
   - 配置数据源（Doris）
   - 导入国网监控仪表板

## 三、数据流程架构计划（PostgreSQL Sink版本）
**数仓分层设计与 SQL 脚本**

### 3.1 分层数仓设计
```
数据流向: PostgreSQL(source) → Flink CDC → Fluss ODS → DWD → DWS → ADS → PostgreSQL(sink) → Grafana
```

1. **ODS层（原始数据层）**
   - 直接从PostgreSQL CDC捕获
   - 保持原始表结构
   - 主键约束配置

2. **DWD层（明细数据层）**
   - 数据清洗和标准化
   - 维度退化（关联维表）
   - 使用Delta Join优化关联查询
   - 派生指标计算

3. **DWS层（汇总数据层）**
   - 时间窗口聚合（1小时、5分钟）
   - 业务维度聚合（变电站、设备类型）
   - 指标预计算

4. **ADS层（应用数据层）**
   - 设备健康度评分
   - 实时告警分析
   - 业务KPI指标
   - 存储到PostgreSQL Sink数据库

### 3.2 表结构设计
1. **源数据库（PostgreSQL）表**:
   - 设备信息表（device_info）
   - 电表读数表（meter_reading）
   - 变电站信息表（substation）
   - 用户信息表（customer）
   - 告警信息表（alarm_info）
   - 客户-设备关系表

2. **PostgreSQL结果表**（Sink数据库：power_grid_dw）:
   - 设备实时监控表：device_realtime_monitor
   - 变电站实时概览表：substation_realtime_overview
   - 告警统计分析表：alarm_statistics
   - 设备健康度历史表：device_health_history
   - 变电站小时汇总表：substation_hour_summary
   - 客户用电汇总表：customer_hour_summary

## 四、SSH连接后的安装与配置步骤

### 4.1 SSH连接与基础验证
```bash
ssh root@<container_ip> -p 22
密码: root123
```
1. **验证连接成功**
2. **验证免密登录**:
   ```bash
   ssh localhost echo "SSH免密登录成功"
   ```
3. **验证代理设置**:
   ```bash
   curl -x http://host.docker.internal:7890 https://www.google.com
   ```

### 4.2 启动Flink和Fluss服务
```bash
# 启动Flink集群
export FLINK_HOME=/opt/flink
$FLINK_HOME/bin/start-cluster.sh

# 启动Fluss服务
export FLUSS_HOME=/opt/fluss
$FLUSS_HOME/bin/fluss-daemon.sh start coordinator-server
$FLUSS_HOME/bin/fluss-daemon.sh start tablet-server

# 验证服务状态
curl http://localhost:8081/overview  # Flink UI
curl http://localhost:8084           # Fluss UI
```

### 4.3 数据库连接信息
**Source PostgreSQL（源数据库）**:
- 主机: localhost
- 端口: 5432
- 数据库: power_grid
- 用户名: postgres
- 密码: postgres

**Sink PostgreSQL（数仓数据库）**:
- 主机: localhost
- 端口: 5432
- 数据库: power_grid_dw
- 用户名: sink_user
- 密码: sink123



## 五、测试验证流程计划

### 5.1 端到端数据流测试
1. **数据生成测试**:
   - Python脚本生成模拟数据
   - 验证PostgreSQL数据写入

2. **CDC捕获测试**:
   - 验证Flink CDC作业状态
   - 检查ODS层数据同步

3. **数仓分层测试**:
   - 验证DWD层数据质量
   - 检查DWS层聚合正确性
   - 验证ADS层业务逻辑

4. **PostgreSQL同步测试**:
   - 验证数据写入PostgreSQL Sink
   - 检查查询性能

5. **Grafana看板测试**:
   - 验证数据源连接
   - 检查图表刷新
   - 验证实时更新

### 5.2 CRUD操作测试脚本
创建`test_crud.py`脚本，执行:
1. **插入操作**:
   - 新增设备
   - 新增电表读数
   - 观察数据流转

2. **更新操作**:
   - 更新设备状态
   - 更新电表读数
   - 验证CDC捕获更新

3. **删除/软删除操作**:
   - 测试逻辑删除
   - 验证处理逻辑

4. **事务测试**:
   - 批量操作
   - 回滚场景

### 5.3 实时性验证
1. **延迟测试**:
   - 记录数据生成时间
   - 记录各层处理时间
   - 记录看板展示时间

2. **压力测试**:
   - 批量数据生成
   - 并发CRUD操作
   - 监控系统资源

## 六、镜像打包与发布计划

### 6.1 镜像构建脚本
创建`build_image.sh`脚本:
```bash
# 1. 构建参数配置
IMAGE_NAME="power-grid-realtime"
TAG="2.0"
PROXY="http://127.0.0.1:7890"

# 2. Docker构建命令
docker build --build-arg HTTP_PROXY=$PROXY -t $IMAGE_NAME:$TAG .

# 3. 运行健康检查
docker run --rm $IMAGE_NAME:$TAG /app/scripts/healthcheck.sh

# 4. 运行完整测试
docker run --rm $IMAGE_NAME:$TAG /app/scripts/test_complete.py
```

### 6.2 测试验证流程
1. **容器启动测试**:
   - 验证所有服务自动启动
   - 检查端口暴露

2. **SSH连接测试**:
   - 密码登录
   - 密钥登录
   - 免密登录

3. **组件集成测试**:
   - 执行端到端测试脚本
   - 验证数据流转完整性

### 6.3 镜像打包命令
```bash
# 1. 保存镜像为tar包
docker save power-grid-realtime:2.0 | gzip > power-grid-realtime_2.0.tar.gz

# 2. 计算镜像信息
docker images power-grid-realtime:2.0
du -h power-grid-realtime_2.0.tar.gz

# 3. 创建版本说明文件
cat > VERSION_2.0.md << EOF
包含组件:
- PostgreSQL 13 (Source + Sink)
- Apache Flink 2.2.0 + CDC
- Apache Fluss 0.8
- Grafana
- SSH服务（免密登录）
- 代理支持: host.docker.internal:7890
- Python数据生成器
EOF
```

### 6.4 镜像发布命令
```bash
# 1. 标签管理
docker tag power-grid-realtime:2.0 registry.example.com/power-grid-realtime:2.0
docker tag power-grid-realtime:2.0 registry.example.com/power-grid-realtime:latest

# 2. 推送镜像
docker push registry.example.com/power-grid-realtime:2.0
docker push registry.example.com/power-grid-realtime:latest

# 3. 验证推送
docker pull registry.example.com/power-grid-realtime:2.0

# 4. 生成部署脚本
cat > deploy.sh << 'EOF'
docker run -d \
  -p 2222:22 \
  -p 5432:5432 \
  -p 8081:8081 \
  -p 3000:3000 \
  -e HTTP_PROXY=http://127.0.0.1:7890 \
  registry.example.com/power-grid-realtime:2.0
EOF
```

## 七、使用指南计划

### 7.1 快速开始
1. **加载镜像**:
   ```bash
   docker load < power-grid-realtime_2.0.tar.gz
   ```

2. **运行容器**:
   ```bash
   docker run -d -p 2222:22 -p 5432:5432 -p 8081:8081 -p 8084:8084 -p 3000:3000 power-grid-realtime:2.0
   ```

3. **SSH连接**:
   ```bash
   ssh root@localhost -p 2222
   密码: root123
   ```

4. **启动Flink和Fluss**:
   ```bash
   # 在容器内执行
   /opt/flink/bin/start-cluster.sh
   /opt/fluss/bin/fluss-daemon.sh start coordinator-server
   /opt/fluss/bin/fluss-daemon.sh start tablet-server
   ```

### 7.2 开发测试流程
1. **进入开发环境**:
   ```bash
   ssh root@container_ip -p 22
   ```

2. **启动所有服务**:
   ```bash
   cd /app/scripts
   ./start-all-services.sh
   ```

3. **监控服务状态**:
   ```bash
   python3 monitor.py
   ```

4. **运行测试**:
   ```bash
   python3 test_complete.py
   ```

5. **手动数据操作**:
   ```bash
   # 数据生成
   python3 data_generator.py
   
   # CRUD测试
   python3 test_crud.py
   ```

### 7.3 维护命令
1. **日志查看**:
   ```bash
   tail -f /var/log/supervisord.log
   tail -f /app/logs/data_gen.log
   ```

2. **服务管理**:
   ```bash
   supervisorctl status
   supervisorctl restart <service_name>
   ```

3. **数据查询**:
   ```bash
   # PostgreSQL Source
   psql -h localhost -U postgres -d power_grid
   
   # PostgreSQL Sink
   psql -h localhost -U sink_user -d power_grid_dw
   ```

## 八、监控与告警计划

### 8.1 系统监控指标
1. **资源监控**:
   - CPU/内存使用率
   - 磁盘空间
   - 网络连接数

2. **服务监控**:
   - 各组件进程状态
   - 端口监听状态
   - 服务响应时间

3. **数据流监控**:
   - CDC延迟
   - Flink作业状态
   - 数据处理吞吐量

### 8.2 告警机制
1. **阈值告警**:
   - 服务宕机
   - 数据延迟超时
   - 资源使用超标



## 九、扩展与优化计划

### 9.1 性能优化
1. **Flink调优**:
   - 并行度设置
   - 状态后端优化
   - Checkpoint配置

2. **Doris优化**:
   - 分桶策略
   - 索引优化
   - 物化视图

3. **网络优化**:
   - 代理缓存配置
   - 连接池管理

### 9.2 功能扩展
1. **新增数据源**:
   - MySQL支持
   - Kafka消息队列
   - 文件系统接入

2. **新增业务场景**:
   - 故障预测
   - 能效分析
   - 负荷预测

3. **新增输出**:
   - Elasticsearch存储
   - Redis缓存
   - 消息通知

## 十、验证成功标准

### 10.1 功能验证
- [x] SSH连接成功，免密登录正常
- [x] 代理配置生效，外部网络可访问
- [x] PostgreSQL服务启动成功，数据库可连接
- [x] Flink和Fluss服务可手动启动
- [x] 数据从PostgreSQL Source到PostgreSQL Sink完整流转
- [x] Grafana看板实时更新
- [x] CRUD操作正确反映到数仓

### 10.2 性能验证
- [x] 端到端延迟 < 10秒
- [x] 数据吞吐量 > 1000条/秒
- [x] 系统资源占用 < 80%
- [x] 看板刷新延迟 < 5秒

### 10.3 可靠性验证
- [x] 服务异常自动恢复
- [x] 数据一致性保证

### 10.4 部署验证
- [x] 镜像打包成功
- [x] 镜像体积 < 3GB
- [x] 部署脚本正常
- [x] 多环境兼容性

### 10.5 数据库连接验证
**Navicat连接信息**:

**Source PostgreSQL（源数据库）**:
- 主机: localhost
- 端口: 5432
- 数据库: power_grid
- 用户名: postgres
- 密码: postgres

**Sink PostgreSQL（数仓数据库）**:
- 主机: localhost
- 端口: 5432
- 数据库: power_grid_dw
- 用户名: sink_user
- 密码: sink123

这个完整计划涵盖了从环境准备、组件安装、数据流程设计、测试验证到镜像打包发布的全过程。每个步骤都考虑了实际操作的可行性和验证的全面性。