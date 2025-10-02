# Databricks notebook source
"""
Advanced Data Validation Examples
This notebook demonstrates advanced validation scenarios using the data quality framework
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Data Validation Examples
# MAGIC 
# MAGIC This notebook demonstrates advanced validation scenarios including:
# MAGIC - Complex business rules
# MAGIC - Cross-table validation
# MAGIC - Custom SQL validation
# MAGIC - Referential integrity checks

# COMMAND ----------

# Import required libraries
import sys
sys.path.append('/workspace/src')

from core.data_quality_engine import DataQualityEngine
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Sample Data

# COMMAND ----------

# Initialize Spark session and data quality engine
spark = SparkSession.builder.appName("AdvancedValidationDemo").getOrCreate()
dq_engine = DataQualityEngine(spark)

# Create sample sales data
sales_data = [
    (1, 101, 201, "2024-01-15", 150.00, "USD"),
    (2, 102, 202, "2024-01-16", 250.50, "USD"),
    (3, 103, 203, "2024-01-17", 75.25, "USD"),
    (4, 104, 204, "2024-01-18", 300.00, "USD"),
    (5, 105, 205, "2024-01-19", 125.75, "USD"),
    (6, 101, 201, "2024-01-20", 200.00, "USD"),  # Duplicate customer-product
    (7, 106, 206, "2024-01-21", 500.00, "USD"),
    (8, 107, 207, "2024-01-22", 175.50, "USD"),
    (9, 108, 208, "2024-01-23", 225.00, "USD"),
    (10, 109, 209, "2024-01-24", 350.00, "USD")
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

# Create sample customer data
customer_data = [
    (101, "John", "Doe", "Premium"),
    (102, "Jane", "Smith", "Standard"),
    (103, "Bob", "Johnson", "Premium"),
    (104, "Alice", "Brown", "Standard"),
    (105, "Charlie", "Wilson", "Premium"),
    (106, "Diana", "Davis", "Standard"),
    (107, "Eve", "Miller", "Premium"),
    (108, "Frank", "Garcia", "Standard"),
    (109, "Grace", "Lee", "Premium")
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("tier", StringType(), True)
])

df_customers = spark.createDataFrame(customer_data, customer_schema)

# Create sample product data
product_data = [
    (201, "Laptop", "Electronics", 999.99),
    (202, "Phone", "Electronics", 699.99),
    (203, "Book", "Education", 29.99),
    (204, "Headphones", "Electronics", 199.99),
    (205, "Tablet", "Electronics", 499.99),
    (206, "Monitor", "Electronics", 299.99),
    (207, "Keyboard", "Electronics", 79.99),
    (208, "Mouse", "Electronics", 49.99),
    (209, "Speaker", "Electronics", 149.99)
]

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("base_price", DoubleType(), True)
])

df_products = spark.createDataFrame(product_data, product_schema)

# Register tables for referential integrity checks
df_customers.createOrReplaceTempView("customers")
df_products.createOrReplaceTempView("products")
df_sales.createOrReplaceTempView("sales")

print("Sample data created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic Validation Rules

# COMMAND ----------

# Define basic validation rules for sales data
basic_rules = {
    "not_null_sales": {
        "type": "not_null",
        "columns": ["sale_id", "customer_id", "product_id", "sale_date", "amount"]
    },
    "amount_range": {
        "type": "range",
        "column": "amount",
        "min_value": 0.01,
        "max_value": 10000.00
    },
    "unique_sale_id": {
        "type": "unique",
        "columns": ["sale_id"]
    },
    "currency_format": {
        "type": "pattern",
        "column": "currency",
        "pattern": "^[A-Z]{3}$"
    }
}

print("Running basic validation rules...")
basic_results = dq_engine.validate_data(df_sales, basic_rules)

print(f"Basic validation status: {basic_results['summary']['overall_status']}")
print(f"Rules passed: {basic_results['summary']['passed_rules']}/{basic_results['summary']['total_rules']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Advanced Business Rules

# COMMAND ----------

# Define advanced business rules
advanced_rules = {
    "premium_customer_minimum": {
        "type": "custom_sql",
        "sql_condition": """
            NOT EXISTS (
                SELECT 1 FROM customers c 
                WHERE c.customer_id = sales.customer_id 
                AND c.tier = 'Premium' 
                AND sales.amount < 100.00
            )
        """
    },
    "weekend_sales_validation": {
        "type": "custom_sql",
        "sql_condition": """
            NOT (DAYOFWEEK(CAST(sale_date AS DATE)) IN (1, 7) AND amount > 500.00)
        """
    },
    "high_value_sales_validation": {
        "type": "custom_sql",
        "sql_condition": """
            NOT (amount > 400.00 AND currency != 'USD')
        """
    }
}

print("Running advanced business rules...")
advanced_results = dq_engine.validate_data(df_sales, advanced_rules)

print(f"Advanced validation status: {advanced_results['summary']['overall_status']}")
print(f"Rules passed: {advanced_results['summary']['passed_rules']}/{advanced_results['summary']['total_rules']}")

# Display detailed results
for rule in advanced_results['rules']:
    status_icon = "✅" if rule['status'] == "PASS" else "❌"
    print(f"{status_icon} {rule['rule_name']}: {rule['message']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Referential Integrity Validation

# COMMAND ----------

# Define referential integrity rules
referential_rules = {
    "customer_id_fk": {
        "type": "referential_integrity",
        "column": "customer_id",
        "reference_table": "customers",
        "reference_column": "customer_id"
    },
    "product_id_fk": {
        "type": "referential_integrity",
        "column": "product_id",
        "reference_table": "products",
        "reference_column": "product_id"
    }
}

print("Running referential integrity validation...")
referential_results = dq_engine.validate_data(df_sales, referential_rules)

print(f"Referential integrity status: {referential_results['summary']['overall_status']}")
print(f"Rules passed: {referential_results['summary']['passed_rules']}/{referential_results['summary']['total_rules']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cross-Table Validation

# COMMAND ----------

# Define cross-table validation rules
cross_table_rules = {
    "sales_amount_vs_base_price": {
        "type": "custom_sql",
        "sql_condition": """
            sales.amount <= (products.base_price * 1.5)
        """
    },
    "customer_tier_validation": {
        "type": "custom_sql",
        "sql_condition": """
            NOT (customers.tier = 'Standard' AND sales.amount > 300.00)
        """
    }
}

print("Running cross-table validation...")
cross_table_results = dq_engine.validate_data(df_sales, cross_table_rules)

print(f"Cross-table validation status: {cross_table_results['summary']['overall_status']}")
print(f"Rules passed: {cross_table_results['summary']['passed_rules']}/{cross_table_results['summary']['total_rules']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Complex Pattern Validation

# COMMAND ----------

# Add some data with complex patterns for validation
complex_data = [
    (11, 101, 201, "2024-01-25", 150.00, "USD", "CASH"),
    (12, 102, 202, "2024-01-26", 250.50, "USD", "CREDIT_CARD"),
    (13, 103, 203, "2024-01-27", 75.25, "USD", "DEBIT_CARD"),
    (14, 104, 204, "2024-01-28", 300.00, "USD", "PAYPAL"),
    (15, 105, 205, "2024-01-29", 125.75, "USD", "BANK_TRANSFER")
]

complex_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True)
])

df_complex = spark.createDataFrame(complex_data, complex_schema)

# Define complex pattern validation rules
pattern_rules = {
    "payment_method_validation": {
        "type": "pattern",
        "column": "payment_method",
        "pattern": "^(CASH|CREDIT_CARD|DEBIT_CARD|PAYPAL|BANK_TRANSFER)$"
    },
    "date_format_validation": {
        "type": "pattern",
        "column": "sale_date",
        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
    },
    "amount_precision_validation": {
        "type": "custom_sql",
        "sql_condition": "amount = ROUND(amount, 2)"
    }
}

print("Running complex pattern validation...")
pattern_results = dq_engine.validate_data(df_complex, pattern_rules)

print(f"Pattern validation status: {pattern_results['summary']['overall_status']}")
print(f"Rules passed: {pattern_results['summary']['passed_rules']}/{pattern_results['summary']['total_rules']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Metrics for Multiple Tables

# COMMAND ----------

# Calculate quality metrics for all tables
tables = {
    "sales": df_sales,
    "customers": df_customers,
    "products": df_products
}

print("Calculating quality metrics for all tables...")
print("=" * 50)

for table_name, df in tables.items():
    print(f"\n{table_name.upper()} TABLE:")
    print("-" * 30)
    
    metrics = dq_engine.metrics.calculate_basic_metrics(df)
    
    print(f"Overall quality score: {metrics['overall_quality_score']['overall_score']:.1f}")
    print(f"Grade: {metrics['overall_quality_score']['grade']}")
    
    print("\nComponent scores:")
    for component, score in metrics['overall_quality_score']['component_scores'].items():
        print(f"  {component}: {score:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Comprehensive Validation Report

# COMMAND ----------

# Combine all validation results
all_rules = {**basic_rules, **advanced_rules, **referential_rules, **cross_table_rules, **pattern_rules}

print("Running comprehensive validation...")
comprehensive_results = dq_engine.validate_data(df_sales, all_rules)

print("=" * 60)
print("COMPREHENSIVE VALIDATION REPORT")
print("=" * 60)

print(f"Overall Status: {comprehensive_results['summary']['overall_status']}")
print(f"Total Rules: {comprehensive_results['summary']['total_rules']}")
print(f"Passed Rules: {comprehensive_results['summary']['passed_rules']}")
print(f"Failed Rules: {comprehensive_results['summary']['failed_rules']}")

print("\nDetailed Results:")
print("-" * 40)

for rule in comprehensive_results['rules']:
    status_icon = "✅" if rule['status'] == "PASS" else "❌"
    print(f"{status_icon} {rule['rule_name']}")
    print(f"   Type: {rule['rule_type']}")
    print(f"   Status: {rule['status']}")
    print(f"   Violations: {rule['violation_count']}")
    print(f"   Message: {rule['message']}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=== ADVANCED VALIDATION DEMO COMPLETED ===")
print("\nThis demo showcased:")
print("✅ Basic validation rules (not null, range, unique, pattern)")
print("✅ Advanced business rules using custom SQL")
print("✅ Referential integrity validation")
print("✅ Cross-table validation")
print("✅ Complex pattern validation")
print("✅ Multi-table quality metrics")
print("✅ Comprehensive validation reporting")

print(f"\nFinal validation status: {comprehensive_results['summary']['overall_status']}")
print(f"Success rate: {(comprehensive_results['summary']['passed_rules']/comprehensive_results['summary']['total_rules'])*100:.1f}%")

print("\nThe framework provides enterprise-grade validation capabilities:")
print("- Flexible rule definition")
print("- SQL-based custom validation")
print("- Cross-table integrity checks")
print("- Comprehensive reporting")
print("- Scalable architecture for large datasets")