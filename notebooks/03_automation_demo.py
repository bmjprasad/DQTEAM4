# Databricks notebook source
"""
Data Quality Automation Demo
This notebook demonstrates the automated data quality system
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Automation Demo
# MAGIC 
# MAGIC This notebook demonstrates the automated data quality system including:
# MAGIC - Automated quality checks
# MAGIC - Alerting and notifications
# MAGIC - Monitoring and reporting
# MAGIC - Configuration management

# COMMAND ----------

# Import required libraries
import sys
sys.path.append('/workspace/src')

from core.data_quality_engine import DataQualityEngine
from automation.automation_manager import AutomationManager
from automation.scheduler import ScheduleType
from automation.alerting import AlertSeverity, AlertChannel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize Automation System

# COMMAND ----------

# Initialize Spark session and data quality engine
spark = SparkSession.builder.appName("DataQualityAutomationDemo").getOrCreate()
dq_engine = DataQualityEngine(spark)

# Initialize automation manager
automation_manager = AutomationManager(dq_engine)

print("Automation system initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup Sample Data

# COMMAND ----------

# Create sample customer data with some quality issues
customer_data = [
    (1, "John", "Doe", "john.doe@email.com", "+1234567890", 25),
    (2, "Jane", "Smith", "jane.smith@email.com", "+1234567891", 30),
    (3, "Bob", "Johnson", "invalid-email", "+1234567892", 35),
    (4, "Alice", "Brown", "alice.brown@email.com", None, 28),
    (5, "Charlie", "Wilson", "charlie.wilson@email.com", "+1234567893", -5),  # Invalid age
    (6, "Diana", "Davis", "diana.davis@email.com", "+1234567894", 40),
    (7, "John", "Doe", "john.doe@email.com", "+1234567890", 25),  # Duplicate
    (8, "Eve", "Miller", "eve.miller@email.com", "+1234567895", 32),
    (9, "Frank", "Garcia", "frank.garcia@email.com", "+1234567896", 45),
    (10, "Grace", "Lee", "grace.lee@email.com", "+1234567897", 38)
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True)
])

df_customers = spark.createDataFrame(customer_data, customer_schema)
df_customers.createOrReplaceTempView("customers")

# Create sample sales data
sales_data = [
    (1, 1, 101, "2024-01-15", 150.00, "USD"),
    (2, 2, 102, "2024-01-16", 250.50, "USD"),
    (3, 3, 103, "2024-01-17", 75.25, "USD"),
    (4, 4, 104, "2024-01-18", 300.00, "USD"),
    (5, 5, 105, "2024-01-19", 125.75, "USD"),
    (6, 1, 101, "2024-01-20", 200.00, "USD"),
    (7, 6, 106, "2024-01-21", 500.00, "USD"),
    (8, 7, 107, "2024-01-22", 175.50, "USD"),
    (9, 8, 108, "2024-01-23", 225.00, "USD"),
    (10, 9, 109, "2024-01-24", 350.00, "USD")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True)
])

df_sales = spark.createDataFrame(sales_data, sales_schema)
df_sales.createOrReplaceTempView("sales")

print("Sample data created and registered as tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup Default Quality Checks

# COMMAND ----------

# Setup default quality checks
automation_manager.setup_default_quality_checks()

# Setup default alert rules
automation_manager.setup_default_alert_rules()

print("Default quality checks and alert rules configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Configure Alert Channels (Demo)

# COMMAND ----------

# Configure email alerts (demo configuration)
automation_manager.configure_email_alerts(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    username="demo@example.com",
    password="demo-password",
    from_email="data-quality@example.com",
    to_emails=["admin@example.com", "data-team@example.com"]
)

# Configure Slack alerts (demo configuration)
automation_manager.configure_slack_alerts(
    webhook_url="https://hooks.slack.com/services/DEMO/SLACK/WEBHOOK"
)

# Configure webhook alerts (demo configuration)
automation_manager.configure_webhook_alerts(
    webhook_url="https://your-monitoring-system.com/webhook/data-quality"
)

print("Alert channels configured (demo mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run Manual Quality Checks

# COMMAND ----------

# Run customer data quality check manually
print("Running customer data quality check...")
customer_result = automation_manager.run_manual_check("customer_data_quality")
print(f"Customer check result: {customer_result}")

# COMMAND ----------

# Run sales data quality check manually
print("Running sales data quality check...")
sales_result = automation_manager.run_manual_check("sales_data_quality")
print(f"Sales check result: {sales_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check System Status

# COMMAND ----------

# Get system status
print("Getting system status...")
status = automation_manager.get_system_status()
print(json.dumps(status, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. View Recent Alerts

# COMMAND ----------

# Get recent alerts
print("Getting recent alerts...")
alerts = automation_manager.get_recent_alerts(limit=10)
print(f"Found {len(alerts)} recent alerts")

for alert in alerts:
    print(f"- {alert['severity'].upper()}: {alert['message'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Add Custom Quality Check

# COMMAND ----------

# Add a custom quality check for data freshness
custom_rules = {
    "data_freshness_check": {
        "type": "custom_sql",
        "sql_condition": "sale_date >= CURRENT_DATE - INTERVAL 7 DAYS"
    },
    "high_value_sales_check": {
        "type": "custom_sql",
        "sql_condition": "amount <= 1000.00"
    }
}

automation_manager.add_custom_quality_check(
    name="sales_freshness_quality",
    dataset="sales",
    rules_config=custom_rules,
    schedule=ScheduleType.DAILY,
    schedule_time="11:00",
    alert_threshold=75.0
)

print("Custom quality check added: sales_freshness_quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Add Custom Alert Rule

# COMMAND ----------

# Add a custom alert rule for specific business scenarios
automation_manager.add_custom_alert_rule(
    name="business_critical_alerts",
    condition="validation_status == 'FAIL' AND failed_rules > 3",
    severity=AlertSeverity.CRITICAL,
    channels=[AlertChannel.EMAIL, AlertChannel.SLACK, AlertChannel.WEBHOOK],
    cooldown_minutes=5
)

print("Custom alert rule added: business_critical_alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Generate Automation Report

# COMMAND ----------

# Generate comprehensive automation report
print("Generating automation report...")
report = automation_manager.generate_automation_report()

print("=== AUTOMATION REPORT ===")
print(f"Report timestamp: {report['report_timestamp']}")
print(f"System health: {report['summary']['system_health']}")
print(f"Total quality checks: {report['summary']['total_quality_checks']}")
print(f"Active checks: {report['summary']['active_checks']}")
print(f"Total alerts: {report['summary']['total_alerts']}")
print(f"Unresolved alerts: {report['summary']['unresolved_alerts']}")

print("\nAlert trends:")
for date, trend in report['alert_trends'].items():
    print(f"  {date}: {trend['total']} alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save Configuration

# COMMAND ----------

# Save automation configuration
config_path = "/workspace/config/automation_demo_config.json"
automation_manager.save_configuration(config_path)
print(f"Automation configuration saved to: {config_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Demonstrate Automated Monitoring

# COMMAND ----------

# Start automation system (in a real scenario, this would run continuously)
print("Starting automation system...")
automation_manager.start_automation()

# Let it run for a few seconds to demonstrate
import time
print("Automation system running... (will stop after 10 seconds)")
time.sleep(10)

# Stop automation system
print("Stopping automation system...")
automation_manager.stop_automation()

print("Automation system stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Summary

# COMMAND ----------

print("=== DATA QUALITY AUTOMATION DEMO COMPLETED ===")
print("\nThis demo showcased:")
print("✅ Automated quality checks with scheduling")
print("✅ Multi-channel alerting system")
print("✅ Custom quality checks and alert rules")
print("✅ System monitoring and reporting")
print("✅ Configuration management")
print("✅ Real-time quality monitoring")

print("\nThe automation system provides:")
print("- Scheduled data quality validation")
print("- Intelligent alerting with severity levels")
print("- Multiple notification channels")
print("- Comprehensive monitoring and reporting")
print("- Flexible configuration management")
print("- Scalable architecture for enterprise use")

print("\nTo run the automation system in production:")
print("1. Configure your alert channels (email, Slack, webhooks)")
print("2. Customize quality checks for your datasets")
print("3. Set up appropriate schedules and thresholds")
print("4. Run: python run_automation.py --mode start --config config/automation_config.json")