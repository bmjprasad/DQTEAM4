# Databricks notebook source
"""
Data Quality Framework Demo
This notebook demonstrates the Ataccama-like data quality framework for Databricks Spark
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Framework Demo
# MAGIC 
# MAGIC This notebook demonstrates the comprehensive data quality framework built for Databricks Spark, inspired by Ataccama's data quality platform.

# COMMAND ----------

# Import the data quality framework
import sys
sys.path.append('/workspace/src')

from core.data_quality_engine import DataQualityEngine
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize Data Quality Engine

# COMMAND ----------

# Initialize Spark session and data quality engine
spark = SparkSession.builder.appName("DataQualityDemo").getOrCreate()
dq_engine = DataQualityEngine(spark)

print("Data Quality Engine initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Sample Data

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

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True)
])

df_customers = spark.createDataFrame(customer_data, schema)
df_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Profiling

# COMMAND ----------

# Run comprehensive data profiling
print("Running data profiling...")
profile = dq_engine.profile_data(df_customers)

# Display profiling results
print("\n=== DATA PROFILE SUMMARY ===")
print(f"Total rows: {profile['dataset_info']['row_count']}")
print(f"Total columns: {profile['dataset_info']['column_count']}")

print("\n=== COLUMN PROFILES ===")
for column, col_profile in profile['column_profiles'].items():
    print(f"\nColumn: {column}")
    print(f"  Data type: {col_profile['data_type']}")
    print(f"  Null percentage: {col_profile['null_percentage']:.1f}%")
    print(f"  Distinct percentage: {col_profile['distinct_percentage']:.1f}%")
    
    if 'data_quality_issues' in col_profile and col_profile['data_quality_issues']:
        print(f"  Issues: {', '.join(col_profile['data_quality_issues'])}")

print("\n=== QUALITY METRICS ===")
metrics = profile['data_quality_metrics']
print(f"Overall quality score: {metrics['overall_quality_score']['overall_score']:.1f}")
print(f"Grade: {metrics['overall_quality_score']['grade']}")

print("\n=== RECOMMENDATIONS ===")
for rec in profile['recommendations']:
    print(f"- {rec}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Validation

# COMMAND ----------

# Define validation rules
validation_rules = {
    "not_null_constraints": {
        "type": "not_null",
        "columns": ["customer_id", "first_name", "last_name", "email"]
    },
    "email_format_validation": {
        "type": "pattern",
        "column": "email",
        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    },
    "age_range_validation": {
        "type": "range",
        "column": "age",
        "min_value": 0,
        "max_value": 120
    },
    "unique_customer_id": {
        "type": "unique",
        "columns": ["customer_id"]
    }
}

# Run validation
print("Running data validation...")
validation_results = dq_engine.validate_data(df_customers, validation_rules)

# Display validation results
print("\n=== VALIDATION RESULTS ===")
print(f"Overall status: {validation_results['summary']['overall_status']}")
print(f"Total rules: {validation_results['summary']['total_rules']}")
print(f"Passed rules: {validation_results['summary']['passed_rules']}")
print(f"Failed rules: {validation_results['summary']['failed_rules']}")

print("\n=== RULE DETAILS ===")
for rule in validation_results['rules']:
    status_icon = "✅" if rule['status'] == "PASS" else "❌"
    print(f"{status_icon} {rule['rule_name']}: {rule['message']}")
    if rule['violation_count'] > 0:
        print(f"   Violations: {rule['violation_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Cleansing

# COMMAND ----------

# Define cleansing rules
cleansing_rules = {
    "null_handling": {
        "phone": {
            "strategy": "fill",
            "method": "constant",
            "value": "N/A"
        }
    },
    "type_corrections": {
        "customer_id": "string"
    },
    "standardization": {
        "first_name": {
            "case_standardization": "title"
        },
        "last_name": {
            "case_standardization": "title"
        },
        "email": {
            "case_standardization": "lower"
        }
    }
}

# Clean the data
print("Running data cleansing...")
cleaned_df = dq_engine.cleanse_data(df_customers, cleansing_rules)

print("\n=== CLEANED DATA ===")
cleaned_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Quality Report

# COMMAND ----------

# Generate comprehensive quality report
print("Generating quality report...")
quality_report = dq_engine.generate_quality_report(df_customers, validation_rules)

print("\n=== QUALITY REPORT ===")
print(f"Report timestamp: {quality_report['report_timestamp']}")
print(f"Dataset: {quality_report['dataset_info']['row_count']} rows, {quality_report['dataset_info']['column_count']} columns")

print(f"\nOverall quality score: {quality_report['quality_score']['overall_score']:.1f}")
print(f"Grade: {quality_report['quality_score']['grade']}")

print("\nComponent scores:")
for component, score in quality_report['quality_score']['component_scores'].items():
    print(f"  {component}: {score:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Anomaly Detection

# COMMAND ----------

# Run anomaly detection
print("Running anomaly detection...")
anomaly_results = dq_engine.anomaly_detector.detect_anomalies(df_customers, ["statistical"])

print("\n=== ANOMALY DETECTION RESULTS ===")
print(f"Methods used: {anomaly_results['methods_used']}")

if 'anomaly_summary' in anomaly_results:
    summary = anomaly_results['anomaly_summary']
    print(f"Total anomalies detected: {summary.get('total_anomalies_detected', 0)}")
    print(f"Overall anomaly percentage: {summary.get('overall_anomaly_percentage', 0):.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualize Quality Metrics

# COMMAND ----------

# Create quality metrics visualization
import matplotlib.pyplot as plt
import pandas as pd

# Extract quality scores
quality_scores = quality_report['quality_score']['component_scores']
components = list(quality_scores.keys())
scores = list(quality_scores.values())

# Create bar chart
plt.figure(figsize=(10, 6))
bars = plt.bar(components, scores, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'])
plt.title('Data Quality Component Scores')
plt.xlabel('Quality Components')
plt.ylabel('Score')
plt.ylim(0, 100)

# Add value labels on bars
for bar, score in zip(bars, scores):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
             f'{score:.1f}', ha='center', va='bottom')

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=== DATA QUALITY FRAMEWORK DEMO COMPLETED ===")
print("\nThis demo showcased:")
print("✅ Comprehensive data profiling")
print("✅ Rule-based validation")
print("✅ Data cleansing capabilities")
print("✅ Quality metrics calculation")
print("✅ Anomaly detection")
print("✅ Quality reporting")

print(f"\nFinal quality score: {quality_report['quality_score']['overall_score']:.1f} ({quality_report['quality_score']['grade']})")

print("\nThe framework provides Ataccama-like capabilities for:")
print("- Data profiling and analysis")
print("- Custom validation rules")
print("- Automated data cleansing")
print("- Quality monitoring and alerting")
print("- Comprehensive reporting")