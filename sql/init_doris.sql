-- 国网实时数仓Doris初始化脚本

-- 创建数据库
CREATE DATABASE IF NOT EXISTS power_grid_dw;
USE power_grid_dw;

-- 创建设备实时监控表（ADS层）
CREATE TABLE IF NOT EXISTS device_realtime_monitor (
    device_id INT,
    device_code VARCHAR(50),
    device_name VARCHAR(100),
    device_type VARCHAR(50),
    substation_name VARCHAR(100),
    latest_voltage DECIMAL(8,2),
    latest_current DECIMAL(8,2),
    latest_power DECIMAL(10,2),
    latest_energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status VARCHAR(20),
    last_update_time DATETIME,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(device_id)
DISTRIBUTED BY HASH(device_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建变电站实时概览表（ADS层）
CREATE TABLE IF NOT EXISTS substation_realtime_overview (
    substation_id INT,
    substation_name VARCHAR(100),
    total_devices INT,
    normal_devices INT,
    warning_devices INT,
    fault_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    active_alarms INT,
    last_update_time DATETIME,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(substation_id)
DISTRIBUTED BY HASH(substation_id) BUCKETS 3
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建告警统计分析表（ADS层）
CREATE TABLE IF NOT EXISTS alarm_statistics (
    alarm_date DATE,
    alarm_hour INT,
    substation_id INT,
    substation_name VARCHAR(100),
    device_type VARCHAR(50),
    alarm_type VARCHAR(50),
    alarm_level VARCHAR(20),
    alarm_count INT,
    resolved_count INT,
    avg_resolution_time DECIMAL(10,2),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(alarm_date, alarm_hour, substation_id, device_type, alarm_type)
DISTRIBUTED BY HASH(substation_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建设备健康度历史表（DWS层）
CREATE TABLE IF NOT EXISTS device_health_history (
    device_id INT,
    device_code VARCHAR(50),
    device_name VARCHAR(100),
    device_type VARCHAR(50),
    substation_name VARCHAR(100),
    stat_date DATE,
    stat_hour INT,
    reading_count BIGINT,
    avg_voltage DECIMAL(8,2),
    max_voltage DECIMAL(8,2),
    min_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    max_current DECIMAL(8,2),
    avg_power DECIMAL(10,2),
    max_power DECIMAL(10,2),
    total_energy DECIMAL(12,2),
    avg_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    avg_health_score DECIMAL(5,2),
    abnormal_count BIGINT,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(device_id, stat_date, stat_hour)
DISTRIBUTED BY HASH(device_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建变电站小时汇总表（DWS层）
CREATE TABLE IF NOT EXISTS substation_hour_summary (
    substation_id INT,
    substation_name VARCHAR(100),
    stat_date DATE,
    stat_hour INT,
    total_devices INT,
    active_devices INT,
    warning_devices INT,
    fault_devices INT,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    alarm_count BIGINT,
    health_score DECIMAL(5,2),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(substation_id, stat_date, stat_hour)
DISTRIBUTED BY HASH(substation_id) BUCKETS 3
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建客户用电小时汇总表（DWS层）
CREATE TABLE IF NOT EXISTS customer_hour_summary (
    customer_id INT,
    customer_code VARCHAR(50),
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),
    stat_date DATE,
    stat_hour INT,
    device_count INT,
    total_energy DECIMAL(12,2),
    peak_power DECIMAL(10,2),
    avg_power DECIMAL(10,2),
    power_cost DECIMAL(10,2),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(customer_id, stat_date, stat_hour)
DISTRIBUTED BY HASH(customer_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建设备5分钟汇总表（DWS层 - 更细粒度）
CREATE TABLE IF NOT EXISTS device_5min_summary (
    device_id INT,
    device_code VARCHAR(50),
    device_name VARCHAR(100),
    substation_name VARCHAR(100),
    stat_time DATETIME,
    reading_count INT,
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_power DECIMAL(10,2),
    avg_temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status VARCHAR(20),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(device_id, stat_time)
DISTRIBUTED BY HASH(device_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- 创建用户用电统计表（ADS层）
CREATE TABLE IF NOT EXISTS customer_power_statistics (
    customer_id INT,
    customer_code VARCHAR(50),
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),
    stat_date DATE,
    total_energy DECIMAL(12,2),
    peak_power DECIMAL(10,2),
    avg_power DECIMAL(10,2),
    power_cost DECIMAL(10,2),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(customer_id, stat_date)
DISTRIBUTED BY HASH(customer_id) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);