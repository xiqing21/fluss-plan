#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓完整分层同步脚本
实现 ODS → DWD → DWS → ADS 完整数据流
"""

import psycopg2
import time
import logging
from datetime import datetime, date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sync_data_warehouse():
    """完整数仓分层同步脚本"""
    
    # 源数据库连接
    source_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='power_grid',
        user='postgres',
        password='postgres'
    )
    
    # Sink数据库连接
    sink_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='power_grid_dw',
        user='sink_user',
        password='sink123'
    )
    
    try:
        source_cursor = source_conn.cursor()
        sink_cursor = sink_conn.cursor()
        
        logger.info("=== 开始ODS层数据同步 ===")
        
        # ODS层：直接从源数据库获取原始数据
        logger.info("同步设备信息...")
        source_cursor.execute("SELECT * FROM device_info ORDER BY id")
        devices = source_cursor.fetchall()
        
        source_cursor.execute("SELECT * FROM substation ORDER BY id")
        substations = source_cursor.fetchall()
        
        source_cursor.execute("SELECT * FROM meter_reading ORDER BY id DESC LIMIT 1000")
        readings = source_cursor.fetchall()
        
        logger.info(f"ODS层数据: 设备{len(devices)}个, 变电站{len(substations)}个, 读数{len(readings)}条")
        
        logger.info("=== 开始DWD层数据处理 ===")
        
        # DWD层：设备电表明细数据处理（关联维表，计算派生指标）
        dwd_data = []
        for reading in readings:
            # reading: (id, device_id, reading_time, voltage, current, power, energy, power_factor, frequency, temperature, created_at)
            device_id = reading[1]
            
            # 查找对应设备和变电站信息
            device = next((d for d in devices if d[0] == device_id), None)
            if not device:
                continue
                
            substation = next((s for s in substations if s[0] == device[4]), None)
            if not substation:
                continue
            
            # 计算健康度评分
            voltage, current, power, temperature, power_factor = reading[3], reading[4], reading[5], reading[9], reading[7]
            
            if voltage and temperature and power_factor:
                if 200 <= voltage <= 240 and temperature < 80 and power_factor > 0.9:
                    health_score = 95.0
                elif 180 <= voltage <= 260 and temperature < 90 and power_factor > 0.8:
                    health_score = 80.0
                else:
                    health_score = 60.0
            else:
                health_score = 50.0
            
            # 状态判断
            status = 'WARNING' if (voltage and (voltage > 240 or voltage < 200)) or (temperature and temperature > 80) else 'NORMAL'
            
            dwd_record = {
                'device_id': device_id,
                'device_code': device[1],
                'device_name': device[2],
                'device_type': device[3],
                'substation_name': substation[1],
                'reading_time': reading[2],
                'voltage': voltage,
                'current': current,
                'power': power,
                'energy': reading[6],
                'power_factor': power_factor,
                'frequency': reading[8],
                'temperature': temperature,
                'health_score': health_score,
                'status': status
            }
            dwd_data.append(dwd_record)
        
        logger.info(f"DWD层处理完成: {len(dwd_data)}条明细记录")
        
        logger.info("=== 开始DWS层数据汇总 ===")
        
        # DWS层：按设备汇总（最新状态）
        device_summary = {}
        for record in dwd_data:
            device_id = record['device_id']
            if device_id not in device_summary or record['reading_time'] > device_summary[device_id]['reading_time']:
                device_summary[device_id] = record
        
        logger.info(f"DWS层汇总完成: {len(device_summary)}个设备")
        
        logger.info("=== 开始ADS层数据写入 ===")
        
        # ADS层：写入设备实时监控表
        sink_cursor.execute("DELETE FROM device_realtime_monitor")
        
        for device_id, summary in device_summary.items():
            sink_cursor.execute("""
                INSERT INTO device_realtime_monitor 
                (device_id, device_code, device_name, device_type, substation_name,
                 latest_voltage, latest_current, latest_power, latest_energy,
                 power_factor, frequency, temperature, health_score, status,
                 last_update_time, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                summary['device_id'], summary['device_code'], summary['device_name'], 
                summary['device_type'], summary['substation_name'],
                summary['voltage'], summary['current'], summary['power'], summary['energy'],
                summary['power_factor'], summary['frequency'], summary['temperature'], 
                summary['health_score'], summary['status'],
                summary['reading_time'], datetime.now()
            ))
        
        # ADS层：写入变电站实时概览表
        sink_cursor.execute("DELETE FROM substation_realtime_overview")
        
        substation_stats = {}
        for summary in device_summary.values():
            sub_name = summary['substation_name']
            if sub_name not in substation_stats:
                substation_stats[sub_name] = {
                    'total_devices': 0,
                    'normal_devices': 0,
                    'warning_devices': 0,
                    'total_power': 0,
                    'voltages': [],
                    'temperatures': []
                }
            
            stats = substation_stats[sub_name]
            stats['total_devices'] += 1
            if summary['status'] == 'NORMAL':
                stats['normal_devices'] += 1
            else:
                stats['warning_devices'] += 1
            
            if summary['power']:
                stats['total_power'] += summary['power']
            if summary['voltage']:
                stats['voltages'].append(summary['voltage'])
            if summary['temperature']:
                stats['temperatures'].append(summary['temperature'])
        
        for sub_id, (sub_name, stats) in enumerate(substation_stats.items(), 1):
            avg_voltage = sum(stats['voltages']) / len(stats['voltages']) if stats['voltages'] else 0
            avg_temperature = sum(stats['temperatures']) / len(stats['temperatures']) if stats['temperatures'] else 0
            
            sink_cursor.execute("""
                INSERT INTO substation_realtime_overview 
                (substation_id, substation_name, total_devices, normal_devices, 
                 warning_devices, fault_devices, total_power, avg_voltage, 
                 avg_current, avg_temperature, active_alarms, last_update_time, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sub_id, sub_name, stats['total_devices'], stats['normal_devices'],
                stats['warning_devices'], 0, stats['total_power'], avg_voltage,
                0, avg_temperature, 0, datetime.now(), datetime.now()
            ))
        
        sink_conn.commit()
        
        # 验证结果
        sink_cursor.execute("SELECT COUNT(*) FROM device_realtime_monitor")
        device_count = sink_cursor.fetchone()[0]
        
        sink_cursor.execute("SELECT COUNT(*) FROM substation_realtime_overview")
        substation_count = sink_cursor.fetchone()[0]
        
        logger.info(f"✓ ADS层同步完成: 设备监控{device_count}条, 变电站概览{substation_count}条")
        
        # 显示样例数据
        sink_cursor.execute("""
            SELECT device_code, latest_voltage, temperature, health_score, status 
            FROM device_realtime_monitor 
            ORDER BY device_id 
            LIMIT 5
        """)
        samples = sink_cursor.fetchall()
        logger.info("设备监控数据样例:")
        for sample in samples:
            logger.info(f"  {sample[0]}: 电压{sample[1]}V, 温度{sample[2]}°C, 健康度{sample[3]}, 状态{sample[4]}")
        
    except Exception as e:
        logger.error(f"数仓分层同步失败: {e}")
        sink_conn.rollback()
    finally:
        source_conn.close()
        sink_conn.close()

if __name__ == "__main__":
    logger.info("开始完整数仓分层同步...")
    sync_data_warehouse()
    logger.info("数仓分层同步完成")