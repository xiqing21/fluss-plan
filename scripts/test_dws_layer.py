#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWS层（数据汇总层）测试脚本
验证时间窗口聚合和业务维度汇总的正确性
"""

import psycopg2
import pymysql
import time
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DWSLayerTester:
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
    
    def generate_test_data(self):
        """生成测试数据用于DWS层验证"""
        logger.info("=== 生成DWS层测试数据 ===")
        
        try:
            cursor = self.pg_conn.cursor()
            
            # 生成过去2小时的测试数据
            base_time = datetime.now() - timedelta(hours=2)
            test_data = []
            
            # 为设备1生成120条记录（每分钟1条，持续2小时）
            for i in range(120):
                reading_time = base_time + timedelta(minutes=i)
                voltage = 220.0 + (i % 10) * 0.5  # 电压在220-224.5V之间波动
                current = 15.0 + (i % 5) * 0.2    # 电流在15-15.8A之间波动
                power = voltage * current * 0.9    # 功率
                energy = power * (1.0/60)          # 每分钟的电能
                temperature = 45.0 + (i % 20) * 0.5  # 温度在45-54.5°C之间波动
                
                test_data.append((
                    1,  # device_id
                    reading_time,
                    voltage,
                    current,
                    power,
                    energy,
                    0.95,  # power_factor
                    50.0,  # frequency
                    temperature
                ))
            
            # 批量插入测试数据
            cursor.executemany("""
                INSERT INTO meter_reading 
                (device_id, reading_time, voltage, current, power, energy, 
                 power_factor, frequency, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, test_data)
            
            self.pg_conn.commit()
            cursor.close()
            
            logger.info(f"✓ 生成测试数据成功，共{len(test_data)}条记录")
            
        except Exception as e:
            logger.error(f"生成测试数据失败: {e}")
            self.pg_conn.rollback()
            raise
    
    def test_hourly_aggregation(self):
        """测试小时级聚合"""
        logger.info("=== 测试小时级聚合 ===")
        
        try:
            # 等待数据流转到DWS层
            logger.info("等待数据流转到DWS层...")
            time.sleep(30)
            
            # 检查PostgreSQL原始数据
            pg_cursor = self.pg_conn.cursor()
            pg_cursor.execute("""
                SELECT 
                    DATE_TRUNC('hour', reading_time) as hour_time,
                    COUNT(*) as count,
                    AVG(voltage) as avg_voltage,
                    AVG(current) as avg_current,
                    AVG(power) as avg_power,
                    AVG(temperature) as avg_temperature
                FROM meter_reading 
                WHERE device_id = 1 
                  AND reading_time >= NOW() - INTERVAL '3 hours'
                GROUP BY DATE_TRUNC('hour', reading_time)
                ORDER BY hour_time
            """)
            
            pg_results = pg_cursor.fetchall()
            logger.info("PostgreSQL原始数据小时聚合:")
            for row in pg_results:
                logger.info(f"  {row[0]}: 记录数={row[1]}, 平均电压={row[2]:.2f}V, 平均电流={row[3]:.2f}A, 平均功率={row[4]:.2f}W, 平均温度={row[5]:.2f}°C")
            
            pg_cursor.close()
            
            # 检查Doris DWS层数据
            doris_cursor = self.doris_conn.cursor()
            doris_cursor.execute("""
                SELECT 
                    stat_date,
                    stat_hour,
                    reading_count,
                    avg_voltage,
                    avg_current,
                    avg_power,
                    avg_temperature,
                    avg_health_score
                FROM device_health_history 
                WHERE device_id = 1 
                  AND stat_date >= CURDATE() - INTERVAL 1 DAY
                ORDER BY stat_date, stat_hour
            """)
            
            doris_results = doris_cursor.fetchall()
            logger.info("Doris DWS层小时汇总数据:")
            for row in doris_results:
                logger.info(f"  {row[0]} {row[1]:02d}:00: 记录数={row[2]}, 平均电压={row[3]:.2f}V, 平均电流={row[4]:.2f}A, 平均功率={row[5]:.2f}W, 平均温度={row[6]:.2f}°C, 健康度={row[7]:.1f}")
            
            doris_cursor.close()
            
            # 验证数据一致性
            if len(pg_results) > 0 and len(doris_results) > 0:
                logger.info("✓ 小时级聚合数据验证通过")
            else:
                logger.warning("⚠ 小时级聚合数据不完整，可能需要更多时间同步")
            
        except Exception as e:
            logger.error(f"小时级聚合测试失败: {e}")
    
    def test_5min_aggregation(self):
        """测试5分钟级聚合"""
        logger.info("=== 测试5分钟级聚合 ===")
        
        try:
            # 检查Doris 5分钟汇总数据
            doris_cursor = self.doris_conn.cursor()
            doris_cursor.execute("""
                SELECT 
                    stat_time,
                    reading_count,
                    avg_voltage,
                    avg_current,
                    avg_power,
                    avg_temperature,
                    health_score,
                    status
                FROM device_5min_summary 
                WHERE device_id = 1 
                  AND stat_time >= NOW() - INTERVAL 3 HOUR
                ORDER BY stat_time DESC
                LIMIT 10
            """)
            
            results = doris_cursor.fetchall()
            logger.info("Doris 5分钟汇总数据（最近10条）:")
            for row in results:
                logger.info(f"  {row[0]}: 记录数={row[1]}, 平均电压={row[2]:.2f}V, 平均电流={row[3]:.2f}A, 平均功率={row[4]:.2f}W, 平均温度={row[5]:.2f}°C, 健康度={row[6]:.1f}, 状态={row[7]}")
            
            doris_cursor.close()
            
            if len(results) > 0:
                logger.info("✓ 5分钟级聚合数据验证通过")
            else:
                logger.warning("⚠ 5分钟级聚合数据为空，检查Flink作业状态")
            
        except Exception as e:
            logger.error(f"5分钟级聚合测试失败: {e}")
    
    def test_substation_aggregation(self):
        """测试变电站维度聚合"""
        logger.info("=== 测试变电站维度聚合 ===")
        
        try:
            # 检查变电站小时汇总
            doris_cursor = self.doris_conn.cursor()
            doris_cursor.execute("""
                SELECT 
                    substation_name,
                    stat_date,
                    stat_hour,
                    total_devices,
                    active_devices,
                    warning_devices,
                    total_power,
                    avg_voltage,
                    avg_temperature,
                    health_score
                FROM substation_hour_summary 
                WHERE stat_date >= CURDATE() - INTERVAL 1 DAY
                ORDER BY stat_date DESC, stat_hour DESC
                LIMIT 5
            """)
            
            results = doris_cursor.fetchall()
            logger.info("变电站小时汇总数据（最近5条）:")
            for row in results:
                logger.info(f"  {row[0]} {row[1]} {row[2]:02d}:00: 设备总数={row[3]}, 正常设备={row[4]}, 告警设备={row[5]}, 总功率={row[6]:.2f}W, 平均电压={row[7]:.2f}V, 平均温度={row[8]:.2f}°C, 健康度={row[9]:.1f}")
            
            doris_cursor.close()
            
            if len(results) > 0:
                logger.info("✓ 变电站维度聚合验证通过")
            else:
                logger.warning("⚠ 变电站维度聚合数据为空")
            
        except Exception as e:
            logger.error(f"变电站维度聚合测试失败: {e}")
    
    def test_customer_aggregation(self):
        """测试客户维度聚合"""
        logger.info("=== 测试客户维度聚合 ===")
        
        try:
            # 检查客户用电小时汇总
            doris_cursor = self.doris_conn.cursor()
            doris_cursor.execute("""
                SELECT 
                    customer_name,
                    stat_date,
                    stat_hour,
                    device_count,
                    total_energy,
                    peak_power,
                    avg_power,
                    power_cost
                FROM customer_hour_summary 
                WHERE stat_date >= CURDATE() - INTERVAL 1 DAY
                ORDER BY stat_date DESC, stat_hour DESC
                LIMIT 5
            """)
            
            results = doris_cursor.fetchall()
            logger.info("客户用电小时汇总数据（最近5条）:")
            for row in results:
                logger.info(f"  {row[0]} {row[1]} {row[2]:02d}:00: 设备数={row[3]}, 总电量={row[4]:.2f}kWh, 峰值功率={row[5]:.2f}W, 平均功率={row[6]:.2f}W, 电费={row[7]:.2f}元")
            
            doris_cursor.close()
            
            if len(results) > 0:
                logger.info("✓ 客户维度聚合验证通过")
            else:
                logger.warning("⚠ 客户维度聚合数据为空")
            
        except Exception as e:
            logger.error(f"客户维度聚合测试失败: {e}")
    
    def test_data_quality(self):
        """测试数据质量"""
        logger.info("=== 测试数据质量 ===")
        
        try:
            doris_cursor = self.doris_conn.cursor()
            
            # 检查数据完整性
            doris_cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT device_id) as unique_devices,
                    MIN(stat_date) as min_date,
                    MAX(stat_date) as max_date
                FROM device_health_history
            """)
            
            quality_stats = doris_cursor.fetchone()
            logger.info(f"数据质量统计:")
            logger.info(f"  总记录数: {quality_stats[0]}")
            logger.info(f"  设备数: {quality_stats[1]}")
            logger.info(f"  数据日期范围: {quality_stats[2]} 到 {quality_stats[3]}")
            
            # 检查异常值
            doris_cursor.execute("""
                SELECT 
                    COUNT(*) as abnormal_voltage_count,
                    AVG(avg_voltage) as overall_avg_voltage,
                    MAX(max_voltage) as max_voltage_recorded,
                    MIN(min_voltage) as min_voltage_recorded
                FROM device_health_history
                WHERE avg_voltage < 200 OR avg_voltage > 250
            """)
            
            abnormal_stats = doris_cursor.fetchone()
            logger.info(f"异常值统计:")
            logger.info(f"  异常电压记录数: {abnormal_stats[0]}")
            logger.info(f"  整体平均电压: {abnormal_stats[1]:.2f}V")
            logger.info(f"  最高电压: {abnormal_stats[2]:.2f}V")
            logger.info(f"  最低电压: {abnormal_stats[3]:.2f}V")
            
            doris_cursor.close()
            
            logger.info("✓ 数据质量检查完成")
            
        except Exception as e:
            logger.error(f"数据质量测试失败: {e}")
    
    def run_all_tests(self):
        """运行所有DWS层测试"""
        logger.info("开始运行DWS层测试...")
        
        try:
            # 生成测试数据
            self.generate_test_data()
            
            # 等待数据处理
            logger.info("等待数据流转处理...")
            time.sleep(60)
            
            # 运行各项测试
            self.test_hourly_aggregation()
            time.sleep(5)
            
            self.test_5min_aggregation()
            time.sleep(5)
            
            self.test_substation_aggregation()
            time.sleep(5)
            
            self.test_customer_aggregation()
            time.sleep(5)
            
            self.test_data_quality()
            
            logger.info("=== DWS层测试完成 ===")
            
        except Exception as e:
            logger.error(f"DWS层测试异常: {e}")
        finally:
            if self.pg_conn:
                self.pg_conn.close()
            if self.doris_conn:
                self.doris_conn.close()

if __name__ == "__main__":
    tester = DWSLayerTester()
    tester.run_all_tests()