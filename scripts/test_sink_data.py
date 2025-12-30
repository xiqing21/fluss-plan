#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试PostgreSQL Sink数据脚本
检查数仓分层数据是否正确写入PostgreSQL
"""

import psycopg2
import time
from datetime import datetime

def connect_db():
    """连接PostgreSQL数据库"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='power_grid_dw',
            user='sink_user',
            password='sink123'
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def test_device_realtime_monitor(conn):
    """测试设备实时监控表"""
    print("\n=== 设备实时监控表 (device_realtime_monitor) ===")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT device_id, device_code, device_name, substation_name, 
                   latest_voltage, latest_power, temperature, health_score, 
                   status, last_update_time
            FROM device_realtime_monitor 
            ORDER BY last_update_time DESC 
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        if rows:
            print(f"找到 {len(rows)} 条实时监控数据:")
            for row in rows:
                print(f"设备{row[0]} ({row[1]}) - {row[3]} - 电压:{row[4]}V 功率:{row[5]}W 温度:{row[6]}°C 健康度:{row[7]} 状态:{row[8]} 时间:{row[9]}")
        else:
            print("❌ 没有找到实时监控数据")
        
        cursor.close()
        return len(rows) > 0
    except Exception as e:
        print(f"❌ 查询实时监控表失败: {e}")
        return False

def test_device_window_summary(conn):
    """测试设备窗口汇总表"""
    print("\n=== 设备窗口汇总表 (device_window_summary) ===")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT device_id, device_code, substation_name, window_start, window_end,
                   reading_count, avg_voltage, max_voltage, avg_power, abnormal_count
            FROM device_window_summary 
            ORDER BY window_start DESC 
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        if rows:
            print(f"找到 {len(rows)} 条窗口汇总数据:")
            for row in rows:
                print(f"设备{row[0]} ({row[1]}) - {row[2]} - 窗口:{row[3]} ~ {row[4]} - 记录数:{row[5]} 平均电压:{row[6]}V 最大电压:{row[7]}V 平均功率:{row[8]}W 异常数:{row[9]}")
        else:
            print("❌ 没有找到窗口汇总数据")
        
        cursor.close()
        return len(rows) > 0
    except Exception as e:
        print(f"❌ 查询窗口汇总表失败: {e}")
        return False

def test_substation_summary(conn):
    """测试变电站汇总表"""
    print("\n=== 变电站汇总表 (substation_summary) ===")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT substation_name, window_start, window_end, total_devices, 
                   active_devices, warning_devices, total_power, avg_voltage, avg_temperature
            FROM substation_summary 
            ORDER BY window_start DESC 
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        if rows:
            print(f"找到 {len(rows)} 条变电站汇总数据:")
            for row in rows:
                print(f"{row[0]} - 窗口:{row[1]} ~ {row[2]} - 设备总数:{row[3]} 正常:{row[4]} 告警:{row[5]} 总功率:{row[6]}W 平均电压:{row[7]}V 平均温度:{row[8]}°C")
        else:
            print("❌ 没有找到变电站汇总数据")
        
        cursor.close()
        return len(rows) > 0
    except Exception as e:
        print(f"❌ 查询变电站汇总表失败: {e}")
        return False

def get_table_counts(conn):
    """获取各表的记录数"""
    print("\n=== 数据表记录统计 ===")
    tables = [
        'device_realtime_monitor',
        'device_window_summary', 
        'substation_summary'
    ]
    
    for table in tables:
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} 条记录")
            cursor.close()
        except Exception as e:
            print(f"{table}: 查询失败 - {e}")

def main():
    print("=== 国网实时数仓PostgreSQL Sink数据测试 ===")
    print(f"测试时间: {datetime.now()}")
    
    conn = connect_db()
    if not conn:
        return
    
    try:
        # 获取记录统计
        get_table_counts(conn)
        
        # 测试各个表的数据
        device_ok = test_device_realtime_monitor(conn)
        window_ok = test_device_window_summary(conn)
        substation_ok = test_substation_summary(conn)
        
        print("\n=== 测试结果汇总 ===")
        print(f"设备实时监控表: {'✅ 正常' if device_ok else '❌ 异常'}")
        print(f"设备窗口汇总表: {'✅ 正常' if window_ok else '❌ 异常'}")
        print(f"变电站汇总表: {'✅ 正常' if substation_ok else '❌ 异常'}")
        
        if device_ok and window_ok and substation_ok:
            print("\n🎉 所有数仓分层数据都正常写入PostgreSQL!")
        else:
            print("\n⚠️  部分数据可能还在处理中，请稍后再试")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()