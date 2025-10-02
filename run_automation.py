#!/usr/bin/env python3
"""
Data Quality Automation Runner
Main script to run the automated data quality system
"""

import sys
import os
import argparse
import json
import logging
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.data_quality_engine import DataQualityEngine
from automation.automation_manager import AutomationManager
from pyspark.sql import SparkSession

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_quality_automation.log'),
            logging.StreamHandler()
        ]
    )

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Data Quality Automation System')
    parser.add_argument('--config', '-c', help='Configuration file path')
    parser.add_argument('--mode', '-m', choices=['start', 'stop', 'status', 'run-check', 'report'], 
                       default='start', help='Operation mode')
    parser.add_argument('--check-name', help='Quality check name (for run-check mode)')
    parser.add_argument('--setup-defaults', action='store_true', 
                       help='Setup default quality checks and alert rules')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize Spark session
        logger.info("Initializing Spark session...")
        spark = SparkSession.builder.appName("DataQualityAutomation").getOrCreate()
        
        # Initialize data quality engine
        logger.info("Initializing data quality engine...")
        dq_engine = DataQualityEngine(spark)
        
        # Initialize automation manager
        logger.info("Initializing automation manager...")
        automation_manager = AutomationManager(dq_engine, args.config)
        
        if args.setup_defaults:
            logger.info("Setting up default configurations...")
            automation_manager.setup_default_quality_checks()
            automation_manager.setup_default_alert_rules()
            logger.info("Default configurations setup complete")
            
        if args.mode == 'start':
            logger.info("Starting data quality automation system...")
            automation_manager.start_automation()
            logger.info("Data quality automation system started successfully")
            
            # Keep the process running
            try:
                while True:
                    import time
                    time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping automation...")
                automation_manager.stop_automation()
                
        elif args.mode == 'stop':
            logger.info("Stopping data quality automation system...")
            automation_manager.stop_automation()
            logger.info("Data quality automation system stopped")
            
        elif args.mode == 'status':
            logger.info("Getting system status...")
            status = automation_manager.get_system_status()
            print(json.dumps(status, indent=2))
            
        elif args.mode == 'run-check':
            if not args.check_name:
                logger.error("Check name is required for run-check mode")
                sys.exit(1)
            logger.info(f"Running quality check: {args.check_name}")
            result = automation_manager.run_manual_check(args.check_name)
            print(json.dumps(result, indent=2))
            
        elif args.mode == 'report':
            logger.info("Generating automation report...")
            report = automation_manager.generate_automation_report()
            print(json.dumps(report, indent=2))
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()