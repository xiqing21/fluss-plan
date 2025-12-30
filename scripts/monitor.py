#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网实时数仓监控脚本
监控系统状态和数据流转
"""

import psutil
import psycopg2
import pymysql
import requests
import time
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SystemMonitor:
    def __init__(self):
        self.pg_conn = None
        self.doris_conn = None
        self.setup_connections()
    
    def setup_connections(self):
        """建立数据库连接"""
        try:
            self.pg_conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='power_grid',
                user='postgres',
                password='postgres'
            )
            
            self.doris_conn = pymysql.connect(
                host='localhost',
                port=9030,
                user='root',
                password='',
                database='power_grid_dw'
            )
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
    
    def check_system_resources(self):
        """检查系统资源"""
        logger.info("=== 系统资源监控 ===")
        
        # CPU使用率
        cpu_percent = psutil.cpu_percent(interval=1)
        logger.info(f"CPU使用率: {cpu_percent}%")
        
        # 内存使用率
        memory = psutil.virtual_memory()
        logger.info(f"内存使用率: {memory.percent}% ({memory.used // 1024 // 1024}MB / {memory.total // 1024 // 1024}MB)")
        
        # 磁盘使用率
        disk = psutil.disk_usage('/')
        logger.info(f"磁盘使用率: {disk.percent}% ({disk.used // 1024 // 1024 // 1024}GB / {disk.total // 1024 // 1024 // 1024}GB)")
        
        # 网络连接数
        connections = len(psutil.net_connections())
        logger.info(f"网络连接数: {connections}")
    
    def check_services(self):
        """检查服务状态"""
        logger.info("=== 服务状态监控 ===")
        
        services = [
            ('Flink JobManager', 'http://localhost:8081/overview'),
            ('Fluss', 'http://localhost:8084'),
            ('Doris FE', 'http://localhost:8030'),
            ('Grafana', 'http://localhost:3000/api/health')
        ]
        
        for name, url in services:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"✓ {name}: 正常")
                else:
                    logger.warning(f"✗ {name}: 异常 (状态码: {response.status_code})")
            except Exception as e:
                logger.error(f"✗ {name}: 不可访问 ({e})")
    
    def check_data_flow(self):
        """检查数据流转"""
        logger.info("=== 数据流转监控 ===")
        
        try:
            # 检查PostgreSQL数据
            pg_cursor = self.pg_conn.cursor()
            
            # 最近1小时的电表读数
            pg_cursor.execute("""
                SELECT COUNT(*) FROM meter_reading 
                WHERE created_at > NOW() - INTERVAL '1 hour'
            """)
            recent_readings = pg_cursor.fetchone()[0]
            logger.info(f"最近1小时电表读数: {recent_readings}条")
            
            # 活跃告警数量
            pg_cursor.execute("SELECT COUNT(*) FROM alarm_info WHERE status = 'ACTIVE'")
            active_alarms = pg_cursor.fetchone()[0]
            logger.info(f"活跃告警数量: {active_alarms}条")
            
            pg_cursor.close()
            
            # 检查Doris数据
            if self.doris_conn:
                doris_cursor = self.doris_conn.cursor()
                
                # 实时监控数据
                doris_cursor.execute("SELECT COUNT(*) FROM device_realtime_monitor")
                monitor_records = doris_cursor.fetchone()[0]
                logger.info(f"设备实时监控记录: {monitor_records}条")
                
                doris_cursor.close()
            
        except Exception as e:
            logger.error(f"数据流转检查失败: {e}")
    
    def check_flink_jobs(self):
        """检查Flink作业状态"""
        logger.info("=== Flink作业监控 ===")
        
        try:
            response = requests.get('http://localhost:8081/jobs', timeout=10)
            if response.status_code == 200:
                jobs = response.json()
                logger.info(f"Flink作业总数: {len(jobs.get('jobs', []))}")
                
                for job in jobs.get('jobs', []):
                    job_id = job['id']
                    job_status = job['status']
                    logger.info(f"作业 {job_id}: {job_status}")
                    
                    # 获取作业详细信息
                    detail_response = requests.get(f'http://localhost:8081/jobs/{job_id}', timeout=5)
                    if detail_response.status_code == 200:
                        detail = detail_response.json()
                        duration = detail.get('duration', 0) // 1000  # 转换为秒
                        logger.info(f"  运行时长: {duration}秒")
            else:
                logger.error(f"获取Flink作业信息失败: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Flink作业检查失败: {e}")
    
    def generate_report(self):
        """生成监控报告"""
        logger.info("=== 监控报告 ===")
        
        report_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"报告时间: {report_time}")
        
        try:
            # 数据统计
            pg_cursor = self.pg_conn.cursor()
            
            # 设备总数
            pg_cursor.execute("SELECT COUNT(*) FROM device_info WHERE status != 'DELETED'")
            total_devices = pg_cursor.fetchone()[0]
            
            # 今日电表读数
            pg_cursor.execute("""
                SELECT COUNT(*) FROM meter_reading 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            today_readings = pg_cursor.fetchone()[0]
            
            # 今日告警数
            pg_cursor.execute("""
                SELECT COUNT(*) FROM alarm_info 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            today_alarms = pg_cursor.fetchone()[0]
            
            pg_cursor.close()
            
            logger.info(f"设备总数: {total_devices}")
            logger.info(f"今日电表读数: {today_readings}条")
            logger.info(f"今日告警数: {today_alarms}条")
            
        except Exception as e:
            logger.error(f"生成报告失败: {e}")
    
    def run_monitoring(self, interval=60):
        """运行监控循环"""
        logger.info(f"开始监控，检查间隔: {interval}秒")
        
        while True:
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"监控时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                self.check_system_resources()
                self.check_services()
                self.check_data_flow()
                self.check_flink_jobs()
                self.generate_report()
                
                logger.info(f"{'='*50}\n")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("监控停止")
                break
            except Exception as e:
                logger.error(f"监控异常: {e}")
                time.sleep(10)
        
        # 关闭连接
        if self.pg_conn:
            self.pg_conn.close()
        if self.doris_conn:
            self.doris_conn.close()

if __name__ == "__main__":
    monitor = SystemMonitor()
    monitor.run_monitoring()