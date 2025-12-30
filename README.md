# 国网实时数仓测试环境

基于 Apache Flink、Fluss、Doris 构建的实时数据仓库测试环境，专为国网电力系统数据处理场景设计。

## 🏗️ 架构概览

```
PostgreSQL → Flink CDC → Fluss ODS → Flink DWD → Fluss DWS → Flink ADS → Doris → Grafana
    ↓              ↓         ↓           ↓           ↓           ↓        ↓        ↓
  源数据库      数据捕获   原始存储    明细处理    汇总存储    应用处理   OLAP存储  数据可视化
                                      ↓           ↓           ↓
                                   数据清洗    时间窗口聚合   实时监控
                                   维度关联    业务维度汇总   统计分析
```

## 📦 包含组件

- **PostgreSQL 13**: CDC源数据库，存储电力设备和电表数据
- **Apache Flink 2.2.0**: 实时数据处理引擎，支持CDC和流计算
- **Apache Fluss 0.8**: 流存储系统，提供高性能数据流存储
- **Apache Doris 1.2.7**: OLAP数据库，支持实时查询和分析
- **Grafana**: 数据可视化平台，展示实时监控看板
- **SSH服务**: 支持免密登录，便于远程管理

## 🚀 快速开始

### 1. 构建镜像

```bash
# 构建Docker镜像
./build_image.sh
```

### 2. 部署运行

```bash
# 部署容器
./deploy.sh
```

### 3. 访问服务

- **SSH连接**: `ssh root@localhost -p 2222` (密码: root123)
- **Flink Web UI**: http://localhost:8081
- **Fluss Web UI**: http://localhost:8084
- **Doris FE**: http://localhost:9030
- **Grafana**: http://localhost:3000 (admin/admin)

## 🔧 手动部署

### 加载镜像

```bash
docker load < power-grid-realtime_2.0.tar.gz
```

### 运行容器

```bash
docker run -d \
  --name power-grid-realtime \
  -p 2222:22 \
  -p 5432:5432 \
  -p 8081:8081 \
  -p 8084:8084 \
  -p 9030:9030 \
  -p 8030:8030 \
  -p 3000:3000 \
  -e HTTP_PROXY=http://host.docker.internal:7890 \
  power-grid-realtime:2.0
```

## 🧪 测试验证

### SSH连接后运行测试

```bash
# 连接容器
ssh root@localhost -p 2222

# 启动所有服务
cd /app/scripts
./start-all-services.sh

# 运行健康检查
./healthcheck.sh

# 运行完整测试
python3 test_complete.py

# 运行CRUD测试
python3 test_crud.py

# 运行DWS层测试
python3 test_dws_layer.py

# 启动监控
python3 monitor.py
```

## 📊 数据流程

### 数据分层

**完整的数仓分层架构：**

1. **ODS层（操作数据存储）**: 
   - 从PostgreSQL CDC直接同步原始数据
   - 保持源系统表结构不变
   - 表：`ods_device_info`, `ods_substation`, `ods_meter_reading`, `ods_customer`, `ods_alarm_info`

2. **DWD层（明细数据层）**: 
   - 数据清洗、关联维表、计算派生指标
   - 业务规则应用和数据标准化
   - 表：`dwd_device_meter_detail`（包含设备、变电站、客户关联信息）

3. **DWS层（汇总数据层）**: 
   - 时间窗口聚合（5分钟、1小时）
   - 业务维度汇总（设备、变电站、客户）
   - 表：`dws_device_hour_summary`, `dws_substation_hour_summary`, `dws_customer_hour_summary`, `dws_device_5min_summary`

4. **ADS层（应用数据服务）**: 
   - 面向应用的数据服务层
   - 实时监控、统计分析、业务KPI
   - 表：`device_realtime_monitor`, `substation_realtime_overview`, `alarm_statistics`

### 业务表结构

- `substation`: 变电站信息
- `device_info`: 设备信息
- `customer`: 客户信息
- `meter_reading`: 电表读数（实时数据）
- `alarm_info`: 告警信息

## 🔍 监控指标

- 设备实时状态监控
- 变电站运行概览
- 告警统计分析
- 用电量统计
- 系统性能监控

## 🛠️ 开发指南

### 数据生成器

系统自动运行数据生成器，模拟真实的电表读数和告警数据：

```bash
# 手动启动数据生成器
python3 /app/scripts/data_generator.py
```

### Flink作业管理

```bash
# 查看Flink作业状态
curl http://localhost:8081/jobs

# 重启Flink作业
/app/scripts/start-flink-jobs.sh
```

### 数据库连接

```bash
# PostgreSQL
psql -h localhost -U postgres -d power_grid

# Doris
mysql -h127.0.0.1 -P9030 -uroot -D power_grid_dw
```

## 📝 配置说明

### 代理配置

系统支持HTTP代理，默认配置为 `host.docker.internal:7890`：

- APT包管理器代理
- Python pip代理
- 环境变量代理

### SSH配置

- 支持密码登录: root/root123
- 支持密钥登录: 已配置免密登录
- 端口映射: 容器22端口映射到主机2222端口

## 🔧 故障排除

### 服务启动问题

```bash
# 检查服务状态
supervisorctl status

# 重启服务
supervisorctl restart <service_name>

# 查看日志
tail -f /app/logs/supervisord.log
```

### 数据同步问题

```bash
# 检查Flink作业状态
curl http://localhost:8081/jobs

# 检查CDC复制槽
psql -h localhost -U postgres -d power_grid -c "SELECT * FROM pg_replication_slots;"

# 检查数据流转
python3 /app/scripts/test_complete.py
```

## 📈 性能优化

### Flink调优

- 并行度设置: 根据CPU核数调整
- Checkpoint间隔: 30秒
- 状态后端: RocksDB

### Doris优化

- 分桶策略: 按设备ID哈希分桶
- 副本数: 单节点部署设置为1
- 查询缓存: 启用结果缓存

## 🔄 版本更新

### v2.0 特性

- 完整的实时数仓架构
- 自动化数据生成和测试
- SSH免密登录支持
- 代理网络支持
- 监控和告警功能

## 📞 技术支持

如遇问题，请检查：

1. 容器日志: `docker logs power-grid-realtime`
2. 服务日志: `/app/logs/` 目录下的各服务日志
3. 健康检查: 运行 `/app/scripts/healthcheck.sh`
4. 完整测试: 运行 `/app/scripts/test_complete.py`

## 📄 许可证

本项目仅用于测试和学习目的。