#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓完整测试脚本
测试端到端数据流转
"""

import psycopg2
import pymysql
import time
import requests
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PowerGridTester:
    def __init__(self):
        self.pg_conn = None
        self.doris_conn = None
        self.setup_connections()
    
    def setup_connections(self):
        """建立数据库连接"""
        try:
            # PostgreSQL连接
            self.pg_conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='power_grid',
                user='postgres',
                password='postgres'
            )
            logger.info("PostgreSQL连接成功")
            
            # Doris连接
            self.doris_conn = pymysql.connect(
                host='localhost',
                port=9030,
                user='root',
                password='',
                database='power_grid_dw'
            )
            logger.info("Doris连接成功")
            
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def test_service_availability(self):
        """测试服务可用性"""
        logger.info("=== 测试服务可用性 ===")
        
        services = [
            ('Flink Web UI', 'http://localhost:8081'),
            ('Fluss Web UI', 'http://localhost:8084'),
            ('Doris FE', 'http://localhost:8030'),
            ('Grafana', 'http://localhost:3000')
        ]
        
        for name, url in services:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"✓ {name} 可访问")
                else:
                    logger.warning(f"✗ {name} 返回状态码: {response.status_code}")
            except Exception as e:
                logger.error(f"✗ {name} 不可访问: {e}")
    
    def test_data_insertion(self):
        """测试数据插入"""
        logger.info("=== 测试数据插入 ===")
        
        try:
            cursor = self.pg_conn.cursor()
            
            # 插入测试电表读数
            test_data = {
                'device_id': 1,
                'reading_time': datetime.now(),
                'voltage': 220.5,
                'current': 15.2,
                'power': 3351.6,
                'energy': 1000.0,
                'power_factor': 0.95,
                'frequency': 50.0,
                'temperature': 45.5
            }
            
            sql = """
            INSERT INTO meter_reading 
            (device_id, reading_time, voltage, current, power, energy, 
             power_factor, frequency, temperature)
            VALUES (%(device_id)s, %(reading_time)s, %(voltage)s, %(current)s, 
                    %(power)s, %(energy)s, %(power_factor)s, %(frequency)s, %(temperature)s)
            """
            
            cursor.execute(sql, test_data)
            self.pg_conn.commit()
            cursor.close()
            
            logger.info("✓ 测试数据插入成功")
            
        except Exception as e:
            logger.error(f"✗ 数据插入失败: {e}")
            self.pg_conn.rollback()
    
    def test_data_query(self):
        """测试数据查询"""
        logger.info("=== 测试数据查询 ===")
        
        try:
            # 测试PostgreSQL查询
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute("SELECT COUNT(*) FROM meter_reading")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            logger.info(f"✓ PostgreSQL电表读数记录数: {pg_count}")
            
            # 测试Doris查询
            doris_cursor = self.doris_conn.cursor()
            doris_cursor.execute("SELECT COUNT(*) FROM device_realtime_monitor")
            doris_count = doris_cursor.fetchone()[0]
            doris_cursor.close()
            logger.info(f"✓ Doris实时监控记录数: {doris_count}")
            
        except Exception as e:
            logger.error(f"✗ 数据查询失败: {e}")
    
    def test_crud_operations(self):
        """测试CRUD操作"""
        logger.info("=== 测试CRUD操作 ===")
        
        try:
            cursor = self.pg_conn.cursor()
            
            # CREATE - 插入新设备
            cursor.execute("""
                INSERT INTO device_info (device_code, device_name, device_type, substation_id)
                VALUES ('TEST001', '测试设备', 'METER', 1)
                RETURNING id
            """)
            new_device_id = cursor.fetchone()[0]
            logger.info(f"✓ CREATE: 新增设备ID {new_device_id}")
            
            # READ - 查询设备
            cursor.execute("SELECT device_name FROM device_info WHERE id = %s", (new_device_id,))
            device_name = cursor.fetchone()[0]
            logger.info(f"✓ READ: 设备名称 {device_name}")
            
            # UPDATE - 更新设备状态
            cursor.execute("""
                UPDATE device_info SET status = 'MAINTENANCE' WHERE id = %s
            """, (new_device_id,))
            logger.info("✓ UPDATE: 设备状态更新成功")
            
            # DELETE - 删除设备（软删除）
            cursor.execute("""
                UPDATE device_info SET status = 'DELETED' WHERE id = %s
            """, (new_device_id,))
            logger.info("✓ DELETE: 设备软删除成功")
            
            self.pg_conn.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"✗ CRUD操作失败: {e}")
            self.pg_conn.rollback()
    
    def test_data_latency(self):
        """测试数据延迟"""
        logger.info("=== 测试数据延迟 ===")
        
        try:
            start_time = datetime.now()
            
            # 插入测试数据
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                INSERT INTO meter_reading 
                (device_id, reading_time, voltage, current, power, energy, 
                 power_factor, frequency, temperature)
                VALUES (1, %s, 220.0, 10.0, 2200.0, 500.0, 0.9, 50.0, 40.0)
            """, (start_time,))
            self.pg_conn.commit()
            cursor.close()
            
            # 等待数据同步到Doris
            max_wait = 60  # 最大等待60秒
            wait_time = 0
            
            while wait_time < max_wait:
                try:
                    doris_cursor = self.doris_conn.cursor()
                    doris_cursor.execute("""
                        SELECT COUNT(*) FROM device_realtime_monitor 
                        WHERE last_update_time >= %s
                    """, (start_time,))
                    count = doris_cursor.fetchone()[0]
                    doris_cursor.close()
                    
                    if count > 0:
                        end_time = datetime.now()
                        latency = (end_time - start_time).total_seconds()
                        logger.info(f"✓ 数据同步延迟: {latency:.2f}秒")
                        break
                except:
                    pass
                
                time.sleep(5)
                wait_time += 5
            
            if wait_time >= max_wait:
                logger.warning("✗ 数据同步超时")
                
        except Exception as e:
            logger.error(f"✗ 延迟测试失败: {e}")
    
    def run_all_tests(self):
        """运行所有测试"""
        logger.info("开始运行完整测试...")
        
        try:
            self.test_service_availability()
            time.sleep(2)
            
            self.test_data_insertion()
            time.sleep(2)
            
            self.test_data_query()
            time.sleep(2)
            
            self.test_crud_operations()
            time.sleep(2)
            
            self.test_data_latency()
            time.sleep(5)
            
            # 运行DWS层测试
            logger.info("=== 运行DWS层测试 ===")
            import subprocess
            result = subprocess.run(['python3', '/app/scripts/test_dws_layer.py'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("✓ DWS层测试通过")
            else:
                logger.error(f"✗ DWS层测试失败: {result.stderr}")
            
            logger.info("=== 所有测试完成 ===")
            
        except Exception as e:
            logger.error(f"测试执行异常: {e}")
        finally:
            if self.pg_conn:
                self.pg_conn.close()
            if self.doris_conn:
                self.doris_conn.close()

if __name__ == "__main__":
    tester = PowerGridTester()
    tester.run_all_tests()