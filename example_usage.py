"""
Example Usage of Auto Data Quality Rules Generator

This script demonstrates how to use the data quality rules generator
with both automatic data profiling and manual metadata definition.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data_quality_generator import DataQualityRuleGenerator, TableMetadata, ColumnMetadata
from data_profile_analyzer import DataProfileAnalyzer, create_sample_data


def example_automatic_profiling():
    """Example: Automatic data profiling and rule generation"""
    print("=" * 60)
    print("EXAMPLE 1: Automatic Data Profiling and Rule Generation")
    print("=" * 60)
    
    # Create sample data
    print("Creating sample data...")
    sample_data = create_sample_data()
    
    # Initialize analyzer
    analyzer = DataProfileAnalyzer()
    
    # Analyze each table
    print("\nAnalyzing tables...")
    for table_name, df in sample_data.items():
        print(f"  Analyzing {table_name} table ({len(df)} rows)...")
        table_metadata = analyzer.analyze_dataframe(df, table_name)
        
        print(f"    Columns found:")
        for col in table_metadata.columns:
            print(f"      - {col.name}: {col.data_type}, nullable={col.nullable}")
            if col.pattern:
                print(f"        Pattern: {col.pattern}")
            if col.business_rules:
                print(f"        Business rules: {col.business_rules}")
    
    # Generate data quality rules
    print("\nGenerating data quality rules...")
    spark_sql, python_code = analyzer.generate_rules_from_profiles(
        "auto_generated_dq_rules.sql",
        "auto_generated_dq_pipeline.py"
    )
    
    print(f"✓ Generated Spark SQL rules: auto_generated_dq_rules.sql")
    print(f"✓ Generated Python pipeline code: auto_generated_dq_pipeline.py")
    
    return analyzer.analyzed_metadata


def example_manual_metadata():
    """Example: Manual metadata definition and rule generation"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Manual Metadata Definition and Rule Generation")
    print("=" * 60)
    
    # Define custom table metadata
    print("Defining custom table metadata...")
    
    # Products table
    products_columns = [
        ColumnMetadata(
            name="product_id",
            data_type="int",
            nullable=False,
            is_primary_key=True,
            min_value=1,
            max_value=1000000
        ),
        ColumnMetadata(
            name="product_name",
            data_type="string",
            nullable=False,
            business_rules=["LENGTH(product_name) >= 3", "LENGTH(product_name) <= 100"]
        ),
        ColumnMetadata(
            name="price",
            data_type="decimal",
            nullable=False,
            min_value=0.01,
            max_value=10000.00,
            business_rules=["price > 0"]
        ),
        ColumnMetadata(
            name="category",
            data_type="string",
            nullable=False,
            unique_values=["Electronics", "Clothing", "Books", "Home", "Sports"]
        ),
        ColumnMetadata(
            name="sku",
            data_type="string",
            nullable=False,
            pattern=r"^[A-Z]{2,3}-\d{4,6}$"  # Format: ABC-123456
        ),
        ColumnMetadata(
            name="created_at",
            data_type="timestamp",
            nullable=False
        ),
        ColumnMetadata(
            name="is_active",
            data_type="boolean",
            nullable=False
        )
    ]
    
    products_table = TableMetadata(
        name="products",
        columns=products_columns,
        row_count=5000,
        last_updated=datetime.now() - timedelta(hours=1),
        expected_freshness_hours=12,
        sla_hours=2
    )
    
    # Order items table
    order_items_columns = [
        ColumnMetadata(
            name="order_item_id",
            data_type="int",
            nullable=False,
            is_primary_key=True
        ),
        ColumnMetadata(
            name="order_id",
            data_type="int",
            nullable=False,
            is_foreign_key=True,
            referenced_table="orders",
            referenced_column="order_id"
        ),
        ColumnMetadata(
            name="product_id",
            data_type="int",
            nullable=False,
            is_foreign_key=True,
            referenced_table="products",
            referenced_column="product_id"
        ),
        ColumnMetadata(
            name="quantity",
            data_type="int",
            nullable=False,
            min_value=1,
            max_value=100,
            business_rules=["quantity > 0"]
        ),
        ColumnMetadata(
            name="unit_price",
            data_type="decimal",
            nullable=False,
            min_value=0.01,
            max_value=10000.00
        ),
        ColumnMetadata(
            name="total_price",
            data_type="decimal",
            nullable=False,
            business_rules=["total_price = quantity * unit_price"]
        )
    ]
    
    order_items_table = TableMetadata(
        name="order_items",
        columns=order_items_columns,
        row_count=25000,
        last_updated=datetime.now() - timedelta(minutes=30),
        expected_freshness_hours=6,
        sla_hours=1
    )
    
    # Generate rules
    print("Generating data quality rules...")
    dq_generator = DataQualityRuleGenerator()
    dq_generator.add_table_metadata(products_table)
    dq_generator.add_table_metadata(order_items_table)
    
    rules = dq_generator.generate_all_rules()
    print(f"  Generated {len(rules)} data quality rules")
    
    # Export rules
    spark_sql = dq_generator.export_spark_sql_commands("manual_dq_rules.sql")
    python_code = dq_generator.export_python_pipeline_code("manual_dq_pipeline.py")
    
    print(f"✓ Generated Spark SQL rules: manual_dq_rules.sql")
    print(f"✓ Generated Python pipeline code: manual_dq_pipeline.py")
    
    return rules


def example_pipeline_integration():
    """Example: Pipeline integration with PySpark"""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Pipeline Integration with PySpark")
    print("=" * 60)
    
    # This would be used in a real PySpark environment
    pipeline_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from data_quality_pipeline import DataQualityPipeline

def run_data_quality_checks():
    """Example pipeline integration"""
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()
    
    try:
        # Load data
        print("Loading data...")
        users_df = spark.read.table("users")
        orders_df = spark.read.table("orders")
        
        # Initialize data quality pipeline
        dq_pipeline = DataQualityPipeline(spark)
        
        # Execute data quality rules
        print("Executing data quality rules...")
        
        # Example: Check email format validation
        email_result = dq_pipeline.execute_rule(users_df, validate_email_format)
        if email_result and email_result['accuracy_percentage'] < 95:
            print(f"WARNING: Email format accuracy is {email_result['accuracy_percentage']}%")
        
        # Example: Check null values in critical fields
        age_result = dq_pipeline.execute_rule(users_df, check_age_completeness)
        if age_result and age_result['null_percentage'] > 10:
            print(f"WARNING: Age field has {age_result['null_percentage']}% null values")
        
        # Example: Check foreign key integrity
        fk_result = dq_pipeline.execute_rule(orders_df, check_user_id_foreign_key_integrity, users_df)
        if fk_result and fk_result['invalid_fk_count'] > 0:
            print(f"ERROR: Found {fk_result['invalid_fk_count']} invalid foreign key references")
        
        # Generate comprehensive report
        report = dq_pipeline.generate_report()
        print("\\nData Quality Report:")
        print(report)
        
        # Save results to monitoring system
        results_df = spark.createDataFrame(dq_pipeline.get_all_results())
        results_df.write.mode("overwrite").saveAsTable("data_quality_results")
        
        print("\\nData quality check completed successfully!")
        
    except Exception as e:
        print(f"Error in data quality pipeline: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    run_data_quality_checks()
    '''
    
    print("Example pipeline integration code:")
    print(pipeline_code)
    
    # Save example pipeline
    with open("example_pipeline.py", "w") as f:
        f.write(pipeline_code)
    
    print(f"✓ Example pipeline saved to: example_pipeline.py")


def example_monitoring_integration():
    """Example: Monitoring and alerting integration"""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Monitoring and Alerting Integration")
    print("=" * 60)
    
    monitoring_code = '''
import json
import requests
from datetime import datetime
from data_quality_pipeline import DataQualityPipeline

class DataQualityMonitor:
    """Data Quality Monitoring and Alerting"""
    
    def __init__(self, webhook_url=None, slack_webhook=None):
        self.webhook_url = webhook_url
        self.slack_webhook = slack_webhook
        self.alert_thresholds = {
            'critical': 90,  # Alert if quality < 90%
            'warning': 95   # Alert if quality < 95%
        }
    
    def check_quality_thresholds(self, results):
        """Check results against quality thresholds"""
        alerts = []
        
        for result in results:
            if result['dimension'] == 'accuracy':
                quality_pct = result.get('accuracy_percentage', 100)
            elif result['dimension'] == 'completeness':
                quality_pct = 100 - result.get('null_percentage', 0)
            else:
                continue
            
            if quality_pct < self.alert_thresholds['critical']:
                alerts.append({
                    'severity': 'critical',
                    'rule_id': result['rule_id'],
                    'table': result['table_name'],
                    'quality_pct': quality_pct,
                    'message': f"CRITICAL: {result['rule_id']} quality is {quality_pct}%"
                })
            elif quality_pct < self.alert_thresholds['warning']:
                alerts.append({
                    'severity': 'warning',
                    'rule_id': result['rule_id'],
                    'table': result['table_name'],
                    'quality_pct': quality_pct,
                    'message': f"WARNING: {result['rule_id']} quality is {quality_pct}%"
                })
        
        return alerts
    
    def send_alert(self, alert):
        """Send alert to monitoring systems"""
        print(f"[{alert['severity'].upper()}] {alert['message']}")
        
        # Send to webhook
        if self.webhook_url:
            try:
                requests.post(self.webhook_url, json=alert, timeout=10)
            except Exception as e:
                print(f"Failed to send webhook alert: {e}")
        
        # Send to Slack
        if self.slack_webhook:
            try:
                slack_message = {
                    "text": alert['message'],
                    "color": "danger" if alert['severity'] == 'critical' else "warning"
                }
                requests.post(self.slack_webhook, json=slack_message, timeout=10)
            except Exception as e:
                print(f"Failed to send Slack alert: {e}")
    
    def monitor_data_quality(self, dq_pipeline):
        """Monitor data quality and send alerts"""
        results = dq_pipeline.get_all_results()
        alerts = self.check_quality_thresholds(results)
        
        for alert in alerts:
            self.send_alert(alert)
        
        return len(alerts)

# Example usage
def run_monitoring_example():
    """Example monitoring setup"""
    
    # Initialize monitoring
    monitor = DataQualityMonitor(
        webhook_url="https://your-monitoring-system.com/webhook",
        slack_webhook="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    )
    
    # Run data quality checks (in real scenario, this would be in your pipeline)
    # dq_pipeline = DataQualityPipeline(spark)
    # ... execute rules ...
    
    # Monitor and alert
    # alert_count = monitor.monitor_data_quality(dq_pipeline)
    # print(f"Sent {alert_count} alerts")
    '''
    
    print("Example monitoring integration code:")
    print(monitoring_code)
    
    # Save example monitoring code
    with open("example_monitoring.py", "w") as f:
        f.write(monitoring_code)
    
    print(f"✓ Example monitoring code saved to: example_monitoring.py")


def main():
    """Run all examples"""
    print("Auto Data Quality Rules Generator - Examples")
    print("=" * 60)
    
    # Example 1: Automatic profiling
    auto_metadata = example_automatic_profiling()
    
    # Example 2: Manual metadata
    manual_rules = example_manual_metadata()
    
    # Example 3: Pipeline integration
    example_pipeline_integration()
    
    # Example 4: Monitoring integration
    example_monitoring_integration()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("✓ Automatic data profiling example completed")
    print("✓ Manual metadata definition example completed")
    print("✓ Pipeline integration example completed")
    print("✓ Monitoring integration example completed")
    print("\nGenerated files:")
    print("  - auto_generated_dq_rules.sql")
    print("  - auto_generated_dq_pipeline.py")
    print("  - manual_dq_rules.sql")
    print("  - manual_dq_pipeline.py")
    print("  - example_pipeline.py")
    print("  - example_monitoring.py")
    
    print(f"\nTotal rules generated: {len(manual_rules)}")
    print("\nData quality dimensions covered:")
    print("  ✓ Accuracy (format validation, range checks)")
    print("  ✓ Completeness (null checks, required fields)")
    print("  ✓ Consistency (referential integrity, uniqueness)")
    print("  ✓ Timeliness (freshness checks, SLA validation)")
    print("  ✓ Validity (business rules, data type validation)")


if __name__ == "__main__":
    main()