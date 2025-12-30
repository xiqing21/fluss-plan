-- 国网实时数仓PostgreSQL初始化脚本

-- 创建变电站信息表
CREATE TABLE IF NOT EXISTS substation (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    voltage_level INTEGER,
    capacity DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建设备信息表
CREATE TABLE IF NOT EXISTS device_info (
    id SERIAL PRIMARY KEY,
    device_code VARCHAR(50) UNIQUE NOT NULL,
    device_name VARCHAR(100) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    substation_id INTEGER REFERENCES substation(id),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    install_date DATE,
    status VARCHAR(20) DEFAULT 'NORMAL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建用户信息表
CREATE TABLE IF NOT EXISTS customer (
    id SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(20) DEFAULT 'RESIDENTIAL',
    address VARCHAR(200),
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建客户-设备关系表
CREATE TABLE IF NOT EXISTS customer_device (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customer(id),
    device_id INTEGER REFERENCES device_info(id),
    relationship_type VARCHAR(20) DEFAULT 'OWNER',
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建电表读数表
CREATE TABLE IF NOT EXISTS meter_reading (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES device_info(id),
    reading_time TIMESTAMP NOT NULL,
    voltage DECIMAL(8,2),
    current DECIMAL(8,2),
    power DECIMAL(10,2),
    energy DECIMAL(12,2),
    power_factor DECIMAL(4,3),
    frequency DECIMAL(6,2),
    temperature DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建告警信息表
CREATE TABLE IF NOT EXISTS alarm_info (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES device_info(id),
    alarm_type VARCHAR(50) NOT NULL,
    alarm_level VARCHAR(20) DEFAULT 'WARNING',
    alarm_message TEXT,
    alarm_time TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    resolved_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO substation (name, location, voltage_level, capacity) VALUES
('北京变电站', '北京市朝阳区', 220, 500.00),
('上海变电站', '上海市浦东新区', 500, 1000.00),
('广州变电站', '广州市天河区', 110, 300.00);

INSERT INTO device_info (device_code, device_name, device_type, substation_id, manufacturer, model) VALUES
('DEV001', '主变压器1', 'TRANSFORMER', 1, '西门子', 'SGB-220'),
('DEV002', '断路器1', 'BREAKER', 1, 'ABB', 'VD4-220'),
('DEV003', '电表1', 'METER', 1, '国电南瑞', 'PCS-9611'),
('DEV004', '主变压器2', 'TRANSFORMER', 2, '西门子', 'SGB-500'),
('DEV005', '电表2', 'METER', 2, '国电南瑞', 'PCS-9612');

INSERT INTO customer (customer_code, customer_name, customer_type, address) VALUES
('CUST001', '北京工业园区', 'INDUSTRIAL', '北京市朝阳区工业园'),
('CUST002', '上海商业中心', 'COMMERCIAL', '上海市浦东新区商业街'),
('CUST003', '广州居民小区', 'RESIDENTIAL', '广州市天河区住宅区');

INSERT INTO customer_device (customer_id, device_id) VALUES
(1, 3), (2, 5), (3, 3);

-- 创建CDC复制槽
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');

-- 设置表的REPLICA IDENTITY为FULL（CDC需要）
ALTER TABLE substation REPLICA IDENTITY FULL;
ALTER TABLE device_info REPLICA IDENTITY FULL;
ALTER TABLE customer REPLICA IDENTITY FULL;
ALTER TABLE customer_device REPLICA IDENTITY FULL;
ALTER TABLE meter_reading REPLICA IDENTITY FULL;
ALTER TABLE alarm_info REPLICA IDENTITY FULL;

-- 创建索引优化查询性能
CREATE INDEX idx_meter_reading_device_time ON meter_reading(device_id, reading_time);
CREATE INDEX idx_alarm_info_device_time ON alarm_info(device_id, alarm_time);
CREATE INDEX idx_device_info_substation ON device_info(substation_id);

COMMIT;