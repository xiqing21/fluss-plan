-- 国网实时数仓PostgreSQL Sink数据库初始化脚本

-- 创建设备实时监控表（ADS层）
CREATE TABLE IF NOT EXISTS device_realtime_monitor (
    device_id INTEGER,
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
    last_update_time TIMESTAMP,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (device_id)
);

-- 创建变电站实时概览表（ADS层）
CREATE TABLE IF NOT EXISTS substation_realtime_overview (
    substation_id INTEGER,
    substation_name VARCHAR(100),
    total_devices INTEGER,
    normal_devices INTEGER,
    warning_devices INTEGER,
    fault_devices INTEGER,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    active_alarms INTEGER,
    last_update_time TIMESTAMP,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (substation_id)
);

-- 创建告警统计分析表（ADS层）
CREATE TABLE IF NOT EXISTS alarm_statistics (
    id SERIAL PRIMARY KEY,
    alarm_date DATE,
    alarm_hour INTEGER,
    substation_id INTEGER,
    substation_name VARCHAR(100),
    device_type VARCHAR(50),
    alarm_type VARCHAR(50),
    alarm_level VARCHAR(20),
    alarm_count INTEGER,
    resolved_count INTEGER,
    avg_resolution_time DECIMAL(10,2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建设备健康度历史表（DWS层）
CREATE TABLE IF NOT EXISTS device_health_history (
    id SERIAL PRIMARY KEY,
    device_id INTEGER,
    device_code VARCHAR(50),
    device_name VARCHAR(100),
    device_type VARCHAR(50),
    substation_name VARCHAR(100),
    stat_date DATE,
    stat_hour INTEGER,
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
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, stat_date, stat_hour)
);

-- 创建变电站小时汇总表（DWS层）
CREATE TABLE IF NOT EXISTS substation_hour_summary (
    id SERIAL PRIMARY KEY,
    substation_id INTEGER,
    substation_name VARCHAR(100),
    stat_date DATE,
    stat_hour INTEGER,
    total_devices INTEGER,
    active_devices INTEGER,
    warning_devices INTEGER,
    fault_devices INTEGER,
    total_power DECIMAL(12,2),
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_temperature DECIMAL(5,2),
    alarm_count BIGINT,
    health_score DECIMAL(5,2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(substation_id, stat_date, stat_hour)
);

-- 创建客户用电小时汇总表（DWS层）
CREATE TABLE IF NOT EXISTS customer_hour_summary (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    customer_code VARCHAR(50),
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),
    stat_date DATE,
    stat_hour INTEGER,
    device_count INTEGER,
    total_energy DECIMAL(12,2),
    peak_power DECIMAL(10,2),
    avg_power DECIMAL(10,2),
    power_cost DECIMAL(10,2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, stat_date, stat_hour)
);

-- 创建设备5分钟汇总表（DWS层）
CREATE TABLE IF NOT EXISTS device_5min_summary (
    id SERIAL PRIMARY KEY,
    device_id INTEGER,
    device_code VARCHAR(50),
    device_name VARCHAR(100),
    substation_name VARCHAR(100),
    stat_time TIMESTAMP,
    reading_count INTEGER,
    avg_voltage DECIMAL(8,2),
    avg_current DECIMAL(8,2),
    avg_power DECIMAL(10,2),
    avg_temperature DECIMAL(5,2),
    health_score DECIMAL(5,2),
    status VARCHAR(20),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, stat_time)
);

-- 创建用户用电统计表（ADS层）
CREATE TABLE IF NOT EXISTS customer_power_statistics (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    customer_code VARCHAR(50),
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),
    stat_date DATE,
    total_energy DECIMAL(12,2),
    peak_power DECIMAL(10,2),
    avg_power DECIMAL(10,2),
    power_cost DECIMAL(10,2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, stat_date)
);

-- 创建索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_device_realtime_monitor_update_time ON device_realtime_monitor(last_update_time);
CREATE INDEX IF NOT EXISTS idx_device_health_history_date_hour ON device_health_history(stat_date, stat_hour);
CREATE INDEX IF NOT EXISTS idx_substation_hour_summary_date_hour ON substation_hour_summary(stat_date, stat_hour);
CREATE INDEX IF NOT EXISTS idx_alarm_statistics_date_hour ON alarm_statistics(alarm_date, alarm_hour);

COMMIT;