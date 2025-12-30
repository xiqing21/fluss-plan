#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓监控看板
实时显示设备状态、窗口汇总和变电站概览
"""

import psycopg2
import time
import os
from datetime import datetime, timedelta

def clear_screen():
    """清屏"""
    os.system('clear' if os.name == 'posix' else 'cls')

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

def get_device_status_summary(conn):
    """获取设备状态汇总"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM device_realtime_monitor 
            GROUP BY status
            ORDER BY count DESC
        """)
        return cursor.fetchall()
    except:
        return []

def get_latest_device_data(conn):
    """获取最新设备数据"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT device_id, device_code, substation_name, latest_voltage, 
                   latest_power, temperature, health_score, status, last_update_time
            FROM device_realtime_monitor 
            ORDER BY last_update_time DESC 
            LIMIT 8
        """)
        return cursor.fetchall()
    except:
        return []

def get_substation_overview(conn):
    """获取变电站概览"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT substation_name, total_devices, active_devices, warning_devices,
                   total_power, avg_voltage, avg_temperature, window_start
            FROM substation_summary 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
            ORDER BY window_start DESC
            LIMIT 6
        """)
        return cursor.fetchall()
    except:
        return []

def get_window_summary_stats(conn):
    """获取窗口汇总统计"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_windows,
                AVG(reading_count) as avg_readings_per_window,
                AVG(avg_voltage) as overall_avg_voltage,
                AVG(avg_power) as overall_avg_power,
                SUM(abnormal_count) as total_abnormal
            FROM device_window_summary 
            WHERE window_start >= NOW() - INTERVAL '10 minutes'
        """)
        return cursor.fetchone()
    except:
        return (0, 0, 0, 0, 0)

def display_dashboard(conn):
    """显示监控看板"""
    clear_screen()
    
    print("=" * 80)
    print("🏭 国网实时数仓监控看板")
    print(f"📅 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 设备状态汇总
    print("\n📊 设备状态汇总:")
    status_data = get_device_status_summary(conn)
    if status_data:
        total_devices = sum(row[1] for row in status_data)
        for status, count in status_data:
            percentage = (count / total_devices * 100) if total_devices > 0 else 0
            status_icon = {
                'NORMAL': '✅',
                'WARNING': '⚠️',
                'CRITICAL': '🚨',
                'ATTENTION': '🔍'
            }.get(status, '❓')
            print(f"  {status_icon} {status}: {count} 台 ({percentage:.1f}%)")
    else:
        print("  ❌ 暂无设备状态数据")
    
    # 最新设备数据
    print("\n🔧 最新设备数据:")
    device_data = get_latest_device_data(conn)
    if device_data:
        print("  设备ID | 设备编码 | 变电站     | 电压(V) | 功率(W) | 温度(°C) | 健康度 | 状态     | 更新时间")
        print("  " + "-" * 75)
        for row in device_data:
            device_id, code, substation, voltage, power, temp, health, status, update_time = row
            status_icon = {
                'NORMAL': '✅',
                'WARNING': '⚠️', 
                'CRITICAL': '🚨',
                'ATTENTION': '🔍'
            }.get(status, '❓')
            time_str = update_time.strftime('%H:%M:%S') if update_time else 'N/A'
            print(f"  {device_id:6} | {code:8} | {substation:10} | {voltage:7.1f} | {power:7.0f} | {temp:8.1f} | {health:6.1f} | {status_icon}{status:8} | {time_str}")
    else:
        print("  ❌ 暂无设备数据")
    
    # 变电站概览
    print("\n🏢 变电站概览:")
    substation_data = get_substation_overview(conn)
    if substation_data:
        print("  变电站名   | 总设备 | 正常 | 告警 | 总功率(W) | 平均电压(V) | 平均温度(°C) | 窗口时间")
        print("  " + "-" * 75)
        for row in substation_data:
            name, total, active, warning, power, voltage, temp, window_time = row
            time_str = window_time.strftime('%H:%M:%S') if window_time else 'N/A'
            print(f"  {name:10} | {total:6} | {active:4} | {warning:4} | {power:9.0f} | {voltage:11.1f} | {temp:12.1f} | {time_str}")
    else:
        print("  ❌ 暂无变电站数据")
    
    # 窗口汇总统计
    print("\n📈 窗口汇总统计 (最近10分钟):")
    stats = get_window_summary_stats(conn)
    if stats and stats[0] > 0:
        total_windows, avg_readings, avg_voltage, avg_power, total_abnormal = stats
        print(f"  📊 总窗口数: {total_windows}")
        print(f"  📊 平均每窗口读数: {avg_readings:.1f}")
        print(f"  ⚡ 整体平均电压: {avg_voltage:.1f}V")
        print(f"  🔌 整体平均功率: {avg_power:.0f}W")
        print(f"  ⚠️  异常事件总数: {total_abnormal}")
    else:
        print("  ❌ 暂无窗口汇总数据")
    
    print("\n" + "=" * 80)
    print("🔄 数据流: ODS(datagen) → ODS(Fluss) → DWD(Fluss) → DWS(Fluss,30s窗口) → ADS(PostgreSQL)")
    print("⏱️  自动刷新中... (Ctrl+C 退出)")

def main():
    """主函数"""
    print("启动国网实时数仓监控看板...")
    
    conn = connect_db()
    if not conn:
        print("无法连接数据库，退出")
        return
    
    try:
        while True:
            display_dashboard(conn)
            time.sleep(5)  # 每5秒刷新一次
    except KeyboardInterrupt:
        print("\n\n👋 监控看板已退出")
    finally:
        conn.close()

if __name__ == "__main__":
    main()