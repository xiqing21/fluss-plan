#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试数仓分层脚本
直接通过REST API提交Flink作业，避免使用SQL Client
"""

import requests
import json
import time
import psycopg2

def submit_flink_job():
    """通过REST API提交Flink作业"""
    print("=== 通过REST API提交数仓分层作业 ===")
    
    # 简化的SQL作业，只测试核心流程
    sql_statements = [
        # 1. 设置配置
        "SET 'execution.checkpointing.interval' = '30s'",
        
        # 2. 创建Fluss Catalog
        """CREATE CATALOG fluss_catalog WITH (
            'type' = 'fluss',
            'bootstrap.servers' = 'localhost:9123'
        )""",
        
        # 3. 使用Fluss Catalog
        "USE CATALOG fluss_catalog",
        
        # 4. 创建datagen源表
        """CREATE TEMPORARY TABLE ods_meter_reading (
            id BIGINT,
            device_id INT,
            reading_time TIMESTAMP(3),
            voltage DECIMAL(8,2),
            current_val DECIMAL(8,2),
            power_val DECIMAL(10,2),
            temperature DECIMAL(5,2),
            WATERMARK FOR reading_time AS reading_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.id.min' = '1',
            'fields.id.max' = '10000',
            'fields.device_id.min' = '1',
            'fields.device_id.max' = '3',
            'fields.voltage.min' = '200',
            'fields.voltage.max' = '240',
            'fields.current_val.min' = '10',
            'fields.current_val.max' = '20',
            'fields.power_val.min' = '2000',
            'fields.power_val.max' = '4000',
            'fields.temperature.min' = '30',
            'fields.temperature.max' = '80'
        )""",
        
        # 5. 创建PostgreSQL Sink表
        """CREATE TEMPORARY TABLE ads_device_monitor (
            device_id INT,
            device_code STRING,
            voltage DECIMAL(8,2),
            power_val DECIMAL(10,2),
            temperature DECIMAL(5,2),
            health_score DECIMAL(5,2),
            status STRING,
            process_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/power_grid_dw',
            'table-name' = 'device_realtime_monitor',
            'username' = 'sink_user',
            'password' = 'sink123',
            'sink.buffer-flush.max-rows' = '10',
            'sink.buffer-flush.interval' = '2s'
        )""",
        
        # 6. 创建控制台输出表
        """CREATE TEMPORARY TABLE console_output (
            device_id INT,
            device_code STRING,
            voltage DECIMAL(8,2),
            power_val DECIMAL(10,2),
            temperature DECIMAL(5,2),
            health_score DECIMAL(5,2),
            status STRING,
            process_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )""",
        
        # 7. 数据处理和输出
        """INSERT INTO ads_device_monitor
        SELECT 
            device_id,
            CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
            voltage,
            power_val,
            temperature,
            CASE 
                WHEN voltage BETWEEN 210 AND 230 AND temperature < 70 THEN 95.0
                WHEN voltage BETWEEN 200 AND 240 AND temperature < 80 THEN 85.0
                ELSE 70.0
            END as health_score,
            CASE 
                WHEN voltage > 240 OR temperature > 80 THEN 'WARNING'
                ELSE 'NORMAL'
            END as status,
            CURRENT_TIMESTAMP as process_time
        FROM ods_meter_reading""",
        
        # 8. 控制台输出
        """INSERT INTO console_output
        SELECT 
            device_id,
            CONCAT('DEV', LPAD(CAST(device_id AS STRING), 3, '0')) as device_code,
            voltage,
            power_val,
            temperature,
            CASE 
                WHEN voltage BETWEEN 210 AND 230 AND temperature < 70 THEN 95.0
                WHEN voltage BETWEEN 200 AND 240 AND temperature < 80 THEN 85.0
                ELSE 70.0
            END as health_score,
            CASE 
                WHEN voltage > 240 OR temperature > 80 THEN 'WARNING'
                ELSE 'NORMAL'
            END as status,
            CURRENT_TIMESTAMP as process_time
        FROM ods_meter_reading"""
    ]
    
    # 将SQL语句写入文件
    with open('/tmp/quick_dw_test.sql', 'w') as f:
        for stmt in sql_statements:
            f.write(stmt + ';\n\n')
    
    print("SQL作业文件已生成: /tmp/quick_dw_test.sql")
    return True

def check_postgresql_data():
    """检查PostgreSQL中的数据"""
    print("\n=== 检查PostgreSQL Sink数据 ===")
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='power_grid_dw',
            user='sink_user',
            password='sink123'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM device_realtime_monitor")
        count = cursor.fetchone()[0]
        print(f"device_realtime_monitor表记录数: {count}")
        
        if count > 0:
            cursor.execute("""
                SELECT device_id, device_code, voltage, power_val, temperature, 
                       health_score, status, process_time
                FROM device_realtime_monitor 
                ORDER BY process_time DESC 
                LIMIT 5
            """)
            
            rows = cursor.fetchall()
            print("最新5条记录:")
            for row in rows:
                print(f"  设备{row[0]} ({row[1]}) - 电压:{row[2]}V 功率:{row[3]}W 温度:{row[4]}°C 健康度:{row[5]} 状态:{row[6]} 时间:{row[7]}")
        
        cursor.close()
        conn.close()
        return count > 0
        
    except Exception as e:
        print(f"检查PostgreSQL数据失败: {e}")
        return False

def main():
    """主函数"""
    print("=== 快速数仓分层测试 ===")
    
    # 1. 生成简化的SQL作业
    if not submit_flink_job():
        print("生成SQL作业失败")
        return
    
    # 2. 提交作业（使用后台方式）
    print("\n=== 提交Flink作业 ===")
    import subprocess
    
    cmd = "nohup /opt/flink/bin/sql-client.sh -f /tmp/quick_dw_test.sql > /tmp/quick_dw_test.log 2>&1 &"
    subprocess.run(cmd, shell=True)
    
    print("作业已提交，等待启动...")
    time.sleep(20)
    
    # 3. 检查作业状态
    print("\n=== 检查Flink作业状态 ===")
    try:
        response = requests.get('http://localhost:8081/jobs', timeout=5)
        jobs = response.json()
        print(f"当前作业数: {len(jobs.get('jobs', []))}")
        for job in jobs.get('jobs', []):
            print(f"  作业ID: {job['id']}, 状态: {job['status']}")
    except Exception as e:
        print(f"获取作业状态失败: {e}")
    
    # 4. 等待数据处理
    print("\n=== 等待数据处理 ===")
    for i in range(6):
        print(f"等待中... ({i+1}/6)")
        time.sleep(10)
        
        # 检查数据
        if check_postgresql_data():
            print("✅ 数据已成功写入PostgreSQL!")
            break
    else:
        print("⚠️ 等待超时，请检查日志")
    
    # 5. 显示日志
    print("\n=== 查看作业日志 ===")
    try:
        with open('/tmp/quick_dw_test.log', 'r') as f:
            lines = f.readlines()
            print("最后20行日志:")
            for line in lines[-20:]:
                print(f"  {line.strip()}")
    except:
        print("无法读取日志文件")

if __name__ == "__main__":
    main()