#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓CRUD操作测试脚本
测试数据的增删改查操作和CDC捕获
"""

import psycopg2
import time
import logging
from datetime import datetime, timedelta
from faker import Faker

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
fake = Faker('zh_CN')

class CRUDTester:
    def __init__(self):
        self.conn = None
        self.connect_db()
    
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
    
    def test_create_operations(self):
        """测试CREATE操作"""
        logger.info("=== 测试CREATE操作 ===")
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 创建新变电站
            cursor.execute("""
                INSERT INTO substation (name, location, voltage_level, capacity)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, ('测试变电站', '测试地址', 110, 200.0))
            
            substation_id = cursor.fetchone()[0]
            logger.info(f"✓ 创建变电站成功，ID: {substation_id}")
            
            # 2. 创建新设备
            cursor.execute("""
                INSERT INTO device_info (device_code, device_name, device_type, substation_id, manufacturer, model)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, ('TEST_DEV_001', '测试电表', 'METER', substation_id, '测试厂商', 'TEST-MODEL'))
            
            device_id = cursor.fetchone()[0]
            logger.info(f"✓ 创建设备成功，ID: {device_id}")
            
            # 3. 创建新客户
            cursor.execute("""
                INSERT INTO customer (customer_code, customer_name, customer_type, address, phone)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, ('TEST_CUST_001', '测试客户', 'COMMERCIAL', '测试地址', '13800138000'))
            
            customer_id = cursor.fetchone()[0]
            logger.info(f"✓ 创建客户成功，ID: {customer_id}")
            
            # 4. 创建客户-设备关系
            cursor.execute("""
                INSERT INTO customer_device (customer_id, device_id, relationship_type)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (customer_id, device_id, 'OWNER'))
            
            relation_id = cursor.fetchone()[0]
            logger.info(f"✓ 创建客户-设备关系成功，ID: {relation_id}")
            
            # 5. 批量创建电表读数
            readings_data = []
            for i in range(10):
                readings_data.append((
                    device_id,
                    datetime.now() - timedelta(minutes=i*5),
                    220.0 + i * 0.5,
                    15.0 + i * 0.2,
                    3300.0 + i * 10,
                    1000.0 + i * 5,
                    0.95,
                    50.0,
                    45.0 + i
                ))
            
            cursor.executemany("""
                INSERT INTO meter_reading 
                (device_id, reading_time, voltage, current, power, energy, power_factor, frequency, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, readings_data)
            
            logger.info(f"✓ 批量创建电表读数成功，共{len(readings_data)}条")
            
            # 6. 创建告警信息
            cursor.execute("""
                INSERT INTO alarm_info (device_id, alarm_type, alarm_level, alarm_message, alarm_time)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (device_id, 'OVERTEMPERATURE', 'WARNING', '设备温度过高: 85.5°C', datetime.now()))
            
            alarm_id = cursor.fetchone()[0]
            logger.info(f"✓ 创建告警信息成功，ID: {alarm_id}")
            
            self.conn.commit()
            
            # 返回创建的ID用于后续测试
            return {
                'substation_id': substation_id,
                'device_id': device_id,
                'customer_id': customer_id,
                'relation_id': relation_id,
                'alarm_id': alarm_id
            }
            
        except Exception as e:
            logger.error(f"CREATE操作失败: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def test_read_operations(self, test_ids):
        """测试READ操作"""
        logger.info("=== 测试READ操作 ===")
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 查询变电站信息
            cursor.execute("SELECT name, location FROM substation WHERE id = %s", (test_ids['substation_id'],))
            substation = cursor.fetchone()
            logger.info(f"✓ 查询变电站: {substation[0]} - {substation[1]}")
            
            # 2. 查询设备信息
            cursor.execute("""
                SELECT d.device_name, d.device_type, s.name 
                FROM device_info d 
                JOIN substation s ON d.substation_id = s.id 
                WHERE d.id = %s
            """, (test_ids['device_id'],))
            device = cursor.fetchone()
            logger.info(f"✓ 查询设备: {device[0]} ({device[1]}) - 所属变电站: {device[2]}")
            
            # 3. 查询客户信息
            cursor.execute("SELECT customer_name, customer_type FROM customer WHERE id = %s", (test_ids['customer_id'],))
            customer = cursor.fetchone()
            logger.info(f"✓ 查询客户: {customer[0]} ({customer[1]})")
            
            # 4. 查询最新电表读数
            cursor.execute("""
                SELECT voltage, current, power, temperature, reading_time
                FROM meter_reading 
                WHERE device_id = %s 
                ORDER BY reading_time DESC 
                LIMIT 1
            """, (test_ids['device_id'],))
            reading = cursor.fetchone()
            logger.info(f"✓ 最新电表读数: 电压{reading[0]}V, 电流{reading[1]}A, 功率{reading[2]}W, 温度{reading[3]}°C")
            
            # 5. 统计查询
            cursor.execute("""
                SELECT COUNT(*) as total_readings,
                       AVG(voltage) as avg_voltage,
                       MAX(temperature) as max_temp
                FROM meter_reading 
                WHERE device_id = %s
            """, (test_ids['device_id'],))
            stats = cursor.fetchone()
            logger.info(f"✓ 统计信息: 总读数{stats[0]}条, 平均电压{stats[1]:.2f}V, 最高温度{stats[2]:.1f}°C")
            
            # 6. 关联查询
            cursor.execute("""
                SELECT c.customer_name, d.device_name, cd.relationship_type
                FROM customer c
                JOIN customer_device cd ON c.id = cd.customer_id
                JOIN device_info d ON cd.device_id = d.id
                WHERE c.id = %s
            """, (test_ids['customer_id'],))
            relations = cursor.fetchall()
            for rel in relations:
                logger.info(f"✓ 客户关系: {rel[0]} {rel[2]} {rel[1]}")
            
        except Exception as e:
            logger.error(f"READ操作失败: {e}")
            raise
        finally:
            cursor.close()
    
    def test_update_operations(self, test_ids):
        """测试UPDATE操作"""
        logger.info("=== 测试UPDATE操作 ===")
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 更新设备状态
            cursor.execute("""
                UPDATE device_info 
                SET status = 'MAINTENANCE', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (test_ids['device_id'],))
            logger.info("✓ 更新设备状态为维护中")
            
            # 2. 更新客户信息
            cursor.execute("""
                UPDATE customer 
                SET phone = %s, email = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, ('13900139000', 'test@example.com', test_ids['customer_id']))
            logger.info("✓ 更新客户联系信息")
            
            # 3. 批量更新电表读数状态（模拟数据校正）
            cursor.execute("""
                UPDATE meter_reading 
                SET voltage = voltage * 1.02
                WHERE device_id = %s AND voltage < 220
            """, (test_ids['device_id'],))
            updated_count = cursor.rowcount
            logger.info(f"✓ 批量校正电表读数，更新{updated_count}条记录")
            
            # 4. 更新告警状态
            cursor.execute("""
                UPDATE alarm_info 
                SET status = 'RESOLVED', resolved_time = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (test_ids['alarm_id'],))
            logger.info("✓ 更新告警状态为已解决")
            
            # 5. 条件更新
            cursor.execute("""
                UPDATE substation 
                SET status = 'MAINTENANCE'
                WHERE id = %s AND EXISTS (
                    SELECT 1 FROM device_info 
                    WHERE substation_id = %s AND status = 'MAINTENANCE'
                )
            """, (test_ids['substation_id'], test_ids['substation_id']))
            
            if cursor.rowcount > 0:
                logger.info("✓ 变电站状态更新为维护中（因为有设备在维护）")
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"UPDATE操作失败: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def test_delete_operations(self, test_ids):
        """测试DELETE操作"""
        logger.info("=== 测试DELETE操作 ===")
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 软删除设备（更新状态而不是物理删除）
            cursor.execute("""
                UPDATE device_info 
                SET status = 'DELETED', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (test_ids['device_id'],))
            logger.info("✓ 软删除设备（状态更新为DELETED）")
            
            # 2. 删除客户-设备关系
            cursor.execute("""
                DELETE FROM customer_device 
                WHERE id = %s
            """, (test_ids['relation_id'],))
            logger.info("✓ 删除客户-设备关系")
            
            # 3. 删除旧的电表读数（保留最近7天）
            cursor.execute("""
                DELETE FROM meter_reading 
                WHERE device_id = %s AND created_at < CURRENT_TIMESTAMP - INTERVAL '7 days'
            """, (test_ids['device_id'],))
            deleted_count = cursor.rowcount
            logger.info(f"✓ 删除旧电表读数，删除{deleted_count}条记录")
            
            # 4. 删除已解决的告警（保留最近30天）
            cursor.execute("""
                DELETE FROM alarm_info 
                WHERE status = 'RESOLVED' 
                AND resolved_time < CURRENT_TIMESTAMP - INTERVAL '30 days'
            """)
            deleted_alarms = cursor.rowcount
            logger.info(f"✓ 清理旧告警记录，删除{deleted_alarms}条记录")
            
            # 5. 级联删除测试（删除客户时检查关联）
            cursor.execute("""
                SELECT COUNT(*) FROM customer_device WHERE customer_id = %s
            """, (test_ids['customer_id'],))
            
            remaining_relations = cursor.fetchone()[0]
            if remaining_relations == 0:
                cursor.execute("DELETE FROM customer WHERE id = %s", (test_ids['customer_id'],))
                logger.info("✓ 删除客户（无关联设备）")
            else:
                logger.info(f"⚠ 客户仍有{remaining_relations}个设备关联，跳过删除")
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"DELETE操作失败: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def test_transaction_operations(self):
        """测试事务操作"""
        logger.info("=== 测试事务操作 ===")
        
        try:
            cursor = self.conn.cursor()
            
            # 开始事务
            cursor.execute("BEGIN")
            
            # 1. 创建变电站
            cursor.execute("""
                INSERT INTO substation (name, location, voltage_level, capacity)
                VALUES ('事务测试变电站', '事务测试地址', 220, 500.0)
                RETURNING id
            """)
            substation_id = cursor.fetchone()[0]
            
            # 2. 创建多个设备
            device_ids = []
            for i in range(3):
                cursor.execute("""
                    INSERT INTO device_info (device_code, device_name, device_type, substation_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (f'TXN_DEV_{i:03d}', f'事务测试设备{i+1}', 'METER', substation_id))
                device_ids.append(cursor.fetchone()[0])
            
            # 3. 为每个设备创建电表读数
            for device_id in device_ids:
                cursor.execute("""
                    INSERT INTO meter_reading 
                    (device_id, reading_time, voltage, current, power, energy, power_factor, frequency, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (device_id, datetime.now(), 220.0, 15.0, 3300.0, 1000.0, 0.95, 50.0, 45.0))
            
            # 提交事务
            cursor.execute("COMMIT")
            logger.info(f"✓ 事务提交成功：创建1个变电站和{len(device_ids)}个设备")
            
            # 测试回滚
            cursor.execute("BEGIN")
            
            # 尝试插入无效数据（违反约束）
            try:
                cursor.execute("""
                    INSERT INTO device_info (device_code, device_name, device_type, substation_id)
                    VALUES ('TXN_DEV_001', '重复设备代码', 'METER', %s)
                """, (substation_id,))
                cursor.execute("COMMIT")
            except psycopg2.IntegrityError:
                cursor.execute("ROLLBACK")
                logger.info("✓ 事务回滚成功：检测到重复设备代码")
            
        except Exception as e:
            logger.error(f"事务操作失败: {e}")
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()
    
    def run_all_tests(self):
        """运行所有CRUD测试"""
        logger.info("开始运行CRUD测试...")
        
        try:
            # CREATE测试
            test_ids = self.test_create_operations()
            time.sleep(2)
            
            # READ测试
            self.test_read_operations(test_ids)
            time.sleep(2)
            
            # UPDATE测试
            self.test_update_operations(test_ids)
            time.sleep(2)
            
            # READ测试（验证更新结果）
            logger.info("=== 验证UPDATE结果 ===")
            cursor = self.conn.cursor()
            cursor.execute("SELECT status FROM device_info WHERE id = %s", (test_ids['device_id'],))
            status = cursor.fetchone()[0]
            logger.info(f"✓ 设备状态已更新为: {status}")
            cursor.close()
            time.sleep(2)
            
            # DELETE测试
            self.test_delete_operations(test_ids)
            time.sleep(2)
            
            # 事务测试
            self.test_transaction_operations()
            
            logger.info("=== 所有CRUD测试完成 ===")
            
        except Exception as e:
            logger.error(f"CRUD测试异常: {e}")
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    tester = CRUDTester()
    tester.run_all_tests()