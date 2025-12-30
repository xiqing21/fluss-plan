#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓数据生成器
生成模拟的电表读数和告警数据
"""

import psycopg2
import time
import random
import logging
from datetime import datetime, timedelta
from faker import Faker

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/data_gen.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
fake = Faker('zh_CN')

class PowerGridDataGenerator:
    def __init__(self):
        self.conn = None
        self.device_ids = []
        self.connect_db()
        self.load_device_ids()
    
    def connect_db(self):
        """连接PostgreSQL数据库"""
        try:
            self.conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='power_grid',
                user='postgres',
                password='postgres'
            )
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def load_device_ids(self):
        """加载设备ID列表"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM device_info WHERE status = 'NORMAL'")
            self.device_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"加载了 {len(self.device_ids)} 个设备")
        except Exception as e:
            logger.error(f"加载设备ID失败: {e}")
    
    def generate_meter_reading(self, device_id):
        """生成电表读数数据"""
        base_voltage = random.uniform(215, 235)  # 基准电压
        base_current = random.uniform(10, 50)    # 基准电流
        
        # 添加一些随机波动
        voltage = base_voltage + random.uniform(-5, 5)
        current = base_current + random.uniform(-2, 2)
        power = voltage * current * random.uniform(0.85, 0.95)  # 功率
        energy = power * random.uniform(0.9, 1.1)  # 电能
        power_factor = random.uniform(0.85, 0.98)  # 功率因数
        frequency = 50.0 + random.uniform(-0.2, 0.2)  # 频率
        temperature = random.uniform(25, 75)  # 温度
        
        return {
            'device_id': device_id,
            'reading_time': datetime.now(),
            'voltage': round(voltage, 2),
            'current': round(current, 2),
            'power': round(power, 2),
            'energy': round(energy, 2),
            'power_factor': round(power_factor, 3),
            'frequency': round(frequency, 2),
            'temperature': round(temperature, 2)
        }
    
    def generate_alarm(self, device_id):
        """生成告警数据"""
        alarm_types = [
            'OVERVOLTAGE', 'UNDERVOLTAGE', 'OVERCURRENT', 
            'OVERTEMPERATURE', 'POWER_FAILURE', 'COMMUNICATION_FAULT'
        ]
        alarm_levels = ['INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        alarm_type = random.choice(alarm_types)
        alarm_level = random.choice(alarm_levels)
        
        messages = {
            'OVERVOLTAGE': f'设备电压超过安全范围: {random.uniform(240, 260):.2f}V',
            'UNDERVOLTAGE': f'设备电压低于安全范围: {random.uniform(180, 200):.2f}V',
            'OVERCURRENT': f'设备电流超过额定值: {random.uniform(60, 80):.2f}A',
            'OVERTEMPERATURE': f'设备温度过高: {random.uniform(80, 100):.2f}°C',
            'POWER_FAILURE': '设备电源故障',
            'COMMUNICATION_FAULT': '设备通信异常'
        }
        
        return {
            'device_id': device_id,
            'alarm_type': alarm_type,
            'alarm_level': alarm_level,
            'alarm_message': messages.get(alarm_type, '未知告警'),
            'alarm_time': datetime.now(),
            'status': 'ACTIVE'
        }
    
    def insert_meter_reading(self, data):
        """插入电表读数"""
        try:
            cursor = self.conn.cursor()
            sql = """
            INSERT INTO meter_reading 
            (device_id, reading_time, voltage, current, power, energy, 
             power_factor, frequency, temperature)
            VALUES (%(device_id)s, %(reading_time)s, %(voltage)s, %(current)s, 
                    %(power)s, %(energy)s, %(power_factor)s, %(frequency)s, %(temperature)s)
            """
            cursor.execute(sql, data)
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"插入电表读数失败: {e}")
            self.conn.rollback()
    
    def insert_alarm(self, data):
        """插入告警数据"""
        try:
            cursor = self.conn.cursor()
            sql = """
            INSERT INTO alarm_info 
            (device_id, alarm_type, alarm_level, alarm_message, alarm_time, status)
            VALUES (%(device_id)s, %(alarm_type)s, %(alarm_level)s, 
                    %(alarm_message)s, %(alarm_time)s, %(status)s)
            """
            cursor.execute(sql, data)
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"插入告警数据失败: {e}")
            self.conn.rollback()
    
    def run(self):
        """运行数据生成器"""
        logger.info("开始生成数据...")
        
        while True:
            try:
                # 为每个设备生成电表读数
                for device_id in self.device_ids:
                    meter_data = self.generate_meter_reading(device_id)
                    self.insert_meter_reading(meter_data)
                    
                    # 10%概率生成告警
                    if random.random() < 0.1:
                        alarm_data = self.generate_alarm(device_id)
                        self.insert_alarm(alarm_data)
                
                logger.info(f"生成了 {len(self.device_ids)} 条电表读数")
                
                # 等待30秒
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("数据生成器停止")
                break
            except Exception as e:
                logger.error(f"数据生成异常: {e}")
                time.sleep(10)
        
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    generator = PowerGridDataGenerator()
    generator.run()