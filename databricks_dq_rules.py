"""
Databricks Data Quality Rules Generator

This module generates data quality rules as SQL functions for Databricks,
covering all five data quality dimensions with recommended rules in table format.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class DataQualityRule:
    """Data Quality Rule Definition for Databricks"""
    rule_id: str
    rule_name: str
    dimension: str  # accuracy, completeness, consistency, timeliness, validity
    severity: str  # critical, warning, info
    description: str
    sql_function: str
    example_usage: str
    threshold: Optional[float] = None
    column_name: Optional[str] = None
    table_name: Optional[str] = None


class DatabricksDataQualityGenerator:
    """Generates data quality rules as SQL functions for Databricks"""
    
    def __init__(self):
        self.rules: List[DataQualityRule] = []
        self._generate_all_rules()
    
    def _generate_all_rules(self):
        """Generate all data quality rules"""
        self._generate_accuracy_rules()
        self._generate_completeness_rules()
        self._generate_consistency_rules()
        self._generate_timeliness_rules()
        self._generate_validity_rules()
    
    def _generate_accuracy_rules(self):
        """Generate accuracy rules for data correctness and format validation"""
        
        # Format validation rules
        self.rules.extend([
            DataQualityRule(
                rule_id="email_format_validation",
                rule_name="Email Format Validation",
                dimension="accuracy",
                severity="critical",
                description="Validates email format using regex pattern",
                sql_function="is_valid_email(column_name)",
                example_usage="SELECT is_valid_email(email) FROM users",
                column_name="email"
            ),
            DataQualityRule(
                rule_id="phone_format_validation",
                rule_name="Phone Number Format Validation",
                dimension="accuracy",
                severity="warning",
                description="Validates phone number format",
                sql_function="is_valid_phone(column_name)",
                example_usage="SELECT is_valid_phone(phone_number) FROM contacts",
                column_name="phone_number"
            ),
            DataQualityRule(
                rule_id="uuid_format_validation",
                rule_name="UUID Format Validation",
                dimension="accuracy",
                severity="critical",
                description="Validates UUID format",
                sql_function="is_valid_uuid(column_name)",
                example_usage="SELECT is_valid_uuid(id) FROM transactions",
                column_name="id"
            ),
            DataQualityRule(
                rule_id="date_format_validation",
                rule_name="Date Format Validation",
                dimension="accuracy",
                severity="warning",
                description="Validates date format (YYYY-MM-DD)",
                sql_function="is_valid_date(column_name)",
                example_usage="SELECT is_valid_date(created_date) FROM orders",
                column_name="created_date"
            ),
            DataQualityRule(
                rule_id="credit_card_format_validation",
                rule_name="Credit Card Format Validation",
                dimension="accuracy",
                severity="critical",
                description="Validates credit card number format",
                sql_function="is_valid_credit_card(column_name)",
                example_usage="SELECT is_valid_credit_card(card_number) FROM payments",
                column_name="card_number"
            ),
            DataQualityRule(
                rule_id="ssn_format_validation",
                rule_name="SSN Format Validation",
                dimension="accuracy",
                severity="critical",
                description="Validates SSN format (XXX-XX-XXXX)",
                sql_function="is_valid_ssn(column_name)",
                example_usage="SELECT is_valid_ssn(ssn) FROM employees",
                column_name="ssn"
            )
        ])
        
        # Range validation rules
        self.rules.extend([
            DataQualityRule(
                rule_id="numeric_range_validation",
                rule_name="Numeric Range Validation",
                dimension="accuracy",
                severity="warning",
                description="Validates numeric value is within specified range",
                sql_function="is_in_range(column_name, min_value, max_value)",
                example_usage="SELECT is_in_range(age, 0, 120) FROM users",
                column_name="age"
            ),
            DataQualityRule(
                rule_id="positive_number_validation",
                rule_name="Positive Number Validation",
                dimension="accuracy",
                severity="warning",
                description="Validates number is positive",
                sql_function="is_positive(column_name)",
                example_usage="SELECT is_positive(amount) FROM transactions",
                column_name="amount"
            ),
            DataQualityRule(
                rule_id="non_negative_number_validation",
                rule_name="Non-Negative Number Validation",
                dimension="accuracy",
                severity="warning",
                description="Validates number is non-negative",
                sql_function="is_non_negative(column_name)",
                example_usage="SELECT is_non_negative(quantity) FROM order_items",
                column_name="quantity"
            )
        ])
    
    def _generate_completeness_rules(self):
        """Generate completeness rules for missing data detection"""
        
        self.rules.extend([
            DataQualityRule(
                rule_id="null_check",
                rule_name="Null Value Check",
                dimension="completeness",
                severity="critical",
                description="Checks for null values in column",
                sql_function="is_null(column_name)",
                example_usage="SELECT is_null(email) FROM users",
                column_name="email"
            ),
            DataQualityRule(
                rule_id="not_null_check",
                rule_name="Not Null Check",
                dimension="completeness",
                severity="critical",
                description="Checks for non-null values in column",
                sql_function="is_not_null(column_name)",
                example_usage="SELECT is_not_null(user_id) FROM users",
                column_name="user_id"
            ),
            DataQualityRule(
                rule_id="empty_string_check",
                rule_name="Empty String Check",
                dimension="completeness",
                severity="warning",
                description="Checks for empty or whitespace-only strings",
                sql_function="is_empty_string(column_name)",
                example_usage="SELECT is_empty_string(name) FROM customers",
                column_name="name"
            ),
            DataQualityRule(
                rule_id="not_empty_string_check",
                rule_name="Not Empty String Check",
                dimension="completeness",
                severity="critical",
                description="Checks for non-empty strings",
                sql_function="is_not_empty_string(column_name)",
                example_usage="SELECT is_not_empty_string(description) FROM products",
                column_name="description"
            ),
            DataQualityRule(
                rule_id="whitespace_check",
                rule_name="Whitespace Check",
                dimension="completeness",
                severity="warning",
                description="Checks for leading/trailing whitespace",
                sql_function="has_whitespace(column_name)",
                example_usage="SELECT has_whitespace(name) FROM customers",
                column_name="name"
            ),
            DataQualityRule(
                rule_id="required_field_check",
                rule_name="Required Field Check",
                dimension="completeness",
                severity="critical",
                description="Checks if required field has value",
                sql_function="is_required_field(column_name)",
                example_usage="SELECT is_required_field(email) FROM users",
                column_name="email"
            )
        ])
    
    def _generate_consistency_rules(self):
        """Generate consistency rules for cross-table validation"""
        
        self.rules.extend([
            DataQualityRule(
                rule_id="unique_value_check",
                rule_name="Unique Value Check",
                dimension="consistency",
                severity="critical",
                description="Checks if column values are unique",
                sql_function="is_unique(column_name)",
                example_usage="SELECT is_unique(email) FROM users",
                column_name="email"
            ),
            DataQualityRule(
                rule_id="duplicate_check",
                rule_name="Duplicate Value Check",
                dimension="consistency",
                severity="warning",
                description="Checks for duplicate values in column",
                sql_function="has_duplicates(column_name)",
                example_usage="SELECT has_duplicates(phone) FROM contacts",
                column_name="phone"
            ),
            DataQualityRule(
                rule_id="primary_key_check",
                rule_name="Primary Key Check",
                dimension="consistency",
                severity="critical",
                description="Validates primary key uniqueness",
                sql_function="is_valid_primary_key(column_name)",
                example_usage="SELECT is_valid_primary_key(user_id) FROM users",
                column_name="user_id"
            ),
            DataQualityRule(
                rule_id="foreign_key_check",
                rule_name="Foreign Key Check",
                dimension="consistency",
                severity="critical",
                description="Validates foreign key references",
                sql_function="is_valid_foreign_key(column_name, referenced_table, referenced_column)",
                example_usage="SELECT is_valid_foreign_key(user_id, 'users', 'id') FROM orders",
                column_name="user_id"
            ),
            DataQualityRule(
                rule_id="referential_integrity_check",
                rule_name="Referential Integrity Check",
                dimension="consistency",
                severity="critical",
                description="Checks referential integrity between tables",
                sql_function="check_referential_integrity(table1, column1, table2, column2)",
                example_usage="SELECT check_referential_integrity('orders', 'user_id', 'users', 'id')",
                column_name="user_id"
            ),
            DataQualityRule(
                rule_id="data_type_consistency_check",
                rule_name="Data Type Consistency Check",
                dimension="consistency",
                severity="warning",
                description="Checks data type consistency across similar columns",
                sql_function="is_consistent_data_type(column_name, expected_type)",
                example_usage="SELECT is_consistent_data_type(amount, 'decimal') FROM transactions",
                column_name="amount"
            )
        ])
    
    def _generate_timeliness_rules(self):
        """Generate timeliness rules for data freshness validation"""
        
        self.rules.extend([
            DataQualityRule(
                rule_id="data_freshness_check",
                rule_name="Data Freshness Check",
                dimension="timeliness",
                severity="critical",
                description="Checks if data is fresh within specified hours",
                sql_function="is_fresh_data(timestamp_column, hours_threshold)",
                example_usage="SELECT is_fresh_data(updated_at, 24) FROM users",
                column_name="updated_at"
            ),
            DataQualityRule(
                rule_id="sla_compliance_check",
                rule_name="SLA Compliance Check",
                dimension="timeliness",
                severity="critical",
                description="Checks if data meets SLA requirements",
                sql_function="meets_sla(timestamp_column, sla_hours)",
                example_usage="SELECT meets_sla(created_at, 6) FROM orders",
                column_name="created_at"
            ),
            DataQualityRule(
                rule_id="stale_data_check",
                rule_name="Stale Data Check",
                dimension="timeliness",
                severity="warning",
                description="Identifies stale data beyond threshold",
                sql_function="is_stale_data(timestamp_column, stale_hours)",
                example_usage="SELECT is_stale_data(last_login, 30) FROM users",
                column_name="last_login"
            ),
            DataQualityRule(
                rule_id="future_date_check",
                rule_name="Future Date Check",
                dimension="timeliness",
                severity="warning",
                description="Checks for future dates in timestamp columns",
                sql_function="has_future_date(timestamp_column)",
                example_usage="SELECT has_future_date(created_at) FROM orders",
                column_name="created_at"
            ),
            DataQualityRule(
                rule_id="data_age_check",
                rule_name="Data Age Check",
                dimension="timeliness",
                severity="info",
                description="Calculates age of data in days",
                sql_function="get_data_age(timestamp_column)",
                example_usage="SELECT get_data_age(updated_at) FROM products",
                column_name="updated_at"
            )
        ])
    
    def _generate_validity_rules(self):
        """Generate validity rules for business rule validation"""
        
        self.rules.extend([
            DataQualityRule(
                rule_id="enum_value_check",
                rule_name="Enum Value Check",
                dimension="validity",
                severity="warning",
                description="Validates column values against allowed enum values",
                sql_function="is_valid_enum(column_name, allowed_values)",
                example_usage="SELECT is_valid_enum(status, ['active', 'inactive', 'pending']) FROM users",
                column_name="status"
            ),
            DataQualityRule(
                rule_id="length_validation",
                rule_name="String Length Validation",
                dimension="validity",
                severity="warning",
                description="Validates string length within bounds",
                sql_function="is_valid_length(column_name, min_length, max_length)",
                example_usage="SELECT is_valid_length(name, 2, 50) FROM customers",
                column_name="name"
            ),
            DataQualityRule(
                rule_id="regex_validation",
                rule_name="Regex Pattern Validation",
                dimension="validity",
                severity="critical",
                description="Validates column against regex pattern",
                sql_function="matches_regex(column_name, pattern)",
                example_usage="SELECT matches_regex(postal_code, '^[0-9]{5}(-[0-9]{4})?$') FROM addresses",
                column_name="postal_code"
            ),
            DataQualityRule(
                rule_id="business_rule_validation",
                rule_name="Business Rule Validation",
                dimension="validity",
                severity="warning",
                description="Validates custom business rules",
                sql_function="satisfies_business_rule(column_name, rule_expression)",
                example_usage="SELECT satisfies_business_rule(age, 'age >= 18 AND age <= 65') FROM employees",
                column_name="age"
            ),
            DataQualityRule(
                rule_id="data_type_validation",
                rule_name="Data Type Validation",
                dimension="validity",
                severity="critical",
                description="Validates data type of column",
                sql_function="is_valid_data_type(column_name, expected_type)",
                example_usage="SELECT is_valid_data_type(amount, 'decimal') FROM transactions",
                column_name="amount"
            ),
            DataQualityRule(
                rule_id="constraint_validation",
                rule_name="Constraint Validation",
                dimension="validity",
                severity="critical",
                description="Validates data constraints",
                sql_function="satisfies_constraint(column_name, constraint_expression)",
                example_usage="SELECT satisfies_constraint(quantity, 'quantity > 0') FROM order_items",
                column_name="quantity"
            )
        ])
    
    def get_rules_table(self) -> str:
        """Generate rules as a table for Databricks"""
        
        rules_data = []
        for rule in self.rules:
            rules_data.append({
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'dimension': rule.dimension,
                'severity': rule.severity,
                'description': rule.description,
                'sql_function': rule.sql_function,
                'example_usage': rule.example_usage,
                'threshold': rule.threshold,
                'column_name': rule.column_name,
                'table_name': rule.table_name
            })
        
        return rules_data
    
    def generate_databricks_notebook(self) -> str:
        """Generate complete Databricks notebook with all SQL functions"""
        
        notebook_content = f'''# Databricks Data Quality Rules
# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Rules for Databricks
# MAGIC 
# MAGIC This notebook contains comprehensive data quality rules covering all five dimensions:
# MAGIC - **Accuracy**: Data correctness and format validation
# MAGIC - **Completeness**: Missing data detection
# MAGIC - **Consistency**: Cross-table and referential integrity validation
# MAGIC - **Timeliness**: Data freshness and SLA validation
# MAGIC - **Validity**: Business rule and data type validation
# MAGIC 
# MAGIC Each rule is implemented as a SQL function that can be used directly in your queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Accuracy Rules - Data Correctness and Format Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Email format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_email(email STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phone number format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_phone(phone STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   phone IS NOT NULL AND phone RLIKE '^\\+?[\\d\\s\\-\\(\\)]{{10,}}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UUID format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_uuid(uuid STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   uuid IS NOT NULL AND uuid RLIKE '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date format validation (YYYY-MM-DD)
# MAGIC CREATE OR REPLACE FUNCTION is_valid_date(date_str STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   date_str IS NOT NULL AND date_str RLIKE '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' AND 
# MAGIC   TRY_CAST(date_str AS DATE) IS NOT NULL
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Credit card format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_credit_card(card STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   card IS NOT NULL AND card RLIKE '^\\d{{4}}[\\s\\-]?\\d{{4}}[\\s\\-]?\\d{{4}}[\\s\\-]?\\d{{4}}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SSN format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_ssn(ssn STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   ssn IS NOT NULL AND ssn RLIKE '^\\d{{3}}-\\d{{2}}-\\d{{4}}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Numeric range validation
# MAGIC CREATE OR REPLACE FUNCTION is_in_range(value DOUBLE, min_val DOUBLE, max_val DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value >= min_val AND value <= max_val
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Positive number validation
# MAGIC CREATE OR REPLACE FUNCTION is_positive(value DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value > 0
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-negative number validation
# MAGIC CREATE OR REPLACE FUNCTION is_non_negative(value DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value >= 0
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Completeness Rules - Missing Data Detection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Null value check
# MAGIC CREATE OR REPLACE FUNCTION is_null(value ANY)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NULL
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Not null check
# MAGIC CREATE OR REPLACE FUNCTION is_not_null(value ANY)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Empty string check
# MAGIC CREATE OR REPLACE FUNCTION is_empty_string(value STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND (value = '' OR TRIM(value) = '')
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Not empty string check
# MAGIC CREATE OR REPLACE FUNCTION is_not_empty_string(value STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value != '' AND TRIM(value) != ''
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Whitespace check
# MAGIC CREATE OR REPLACE FUNCTION has_whitespace(value STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND (value != TRIM(value) OR value RLIKE '\\s')
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Required field check
# MAGIC CREATE OR REPLACE FUNCTION is_required_field(value ANY)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND (value != '' OR value IS NOT NULL)
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consistency Rules - Cross-table Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unique value check
# MAGIC CREATE OR REPLACE FUNCTION is_unique(column_name STRING, table_name STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   (SELECT COUNT(*) FROM (SELECT DISTINCT column_name FROM table_name) t) = 
# MAGIC   (SELECT COUNT(*) FROM table_name)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicate check
# MAGIC CREATE OR REPLACE FUNCTION has_duplicates(column_name STRING, table_name STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   (SELECT COUNT(*) FROM (SELECT DISTINCT column_name FROM table_name) t) < 
# MAGIC   (SELECT COUNT(*) FROM table_name)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Primary key check
# MAGIC CREATE OR REPLACE FUNCTION is_valid_primary_key(column_name STRING, table_name STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   (SELECT COUNT(*) FROM (SELECT DISTINCT column_name FROM table_name WHERE column_name IS NOT NULL) t) = 
# MAGIC   (SELECT COUNT(*) FROM table_name WHERE column_name IS NOT NULL)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Foreign key check
# MAGIC CREATE OR REPLACE FUNCTION is_valid_foreign_key(fk_column STRING, fk_table STRING, ref_table STRING, ref_column STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   NOT EXISTS (
# MAGIC     SELECT 1 FROM fk_table fk 
# MAGIC     WHERE fk.fk_column IS NOT NULL 
# MAGIC     AND fk.fk_column NOT IN (SELECT ref_column FROM ref_table WHERE ref_column IS NOT NULL)
# MAGIC   )
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Referential integrity check
# MAGIC CREATE OR REPLACE FUNCTION check_referential_integrity(table1 STRING, column1 STRING, table2 STRING, column2 STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   NOT EXISTS (
# MAGIC     SELECT 1 FROM table1 t1 
# MAGIC     WHERE t1.column1 IS NOT NULL 
# MAGIC     AND t1.column1 NOT IN (SELECT column2 FROM table2 WHERE column2 IS NOT NULL)
# MAGIC   )
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data type consistency check
# MAGIC CREATE OR REPLACE FUNCTION is_consistent_data_type(column_name STRING, table_name STRING, expected_type STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   CASE 
# MAGIC     WHEN expected_type = 'INTEGER' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS INT) IS NULL)
# MAGIC     WHEN expected_type = 'DOUBLE' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS DOUBLE) IS NULL)
# MAGIC     WHEN expected_type = 'DATE' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS DATE) IS NULL)
# MAGIC     WHEN expected_type = 'TIMESTAMP' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS TIMESTAMP) IS NULL)
# MAGIC     ELSE TRUE
# MAGIC   END
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timeliness Rules - Data Freshness Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data freshness check
# MAGIC CREATE OR REPLACE FUNCTION is_fresh_data(timestamp_col TIMESTAMP, hours_threshold INT)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   timestamp_col IS NOT NULL AND 
# MAGIC   HOUR(CURRENT_TIMESTAMP() - timestamp_col) <= hours_threshold
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SLA compliance check
# MAGIC CREATE OR REPLACE FUNCTION meets_sla(timestamp_col TIMESTAMP, sla_hours INT)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   timestamp_col IS NOT NULL AND 
# MAGIC   HOUR(CURRENT_TIMESTAMP() - timestamp_col) <= sla_hours
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stale data check
# MAGIC CREATE OR REPLACE FUNCTION is_stale_data(timestamp_col TIMESTAMP, stale_hours INT)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   timestamp_col IS NOT NULL AND 
# MAGIC   HOUR(CURRENT_TIMESTAMP() - timestamp_col) > stale_hours
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Future date check
# MAGIC CREATE OR REPLACE FUNCTION has_future_date(timestamp_col TIMESTAMP)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   timestamp_col IS NOT NULL AND 
# MAGIC   timestamp_col > CURRENT_TIMESTAMP()
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data age check
# MAGIC CREATE OR REPLACE FUNCTION get_data_age(timestamp_col TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   CASE 
# MAGIC     WHEN timestamp_col IS NULL THEN NULL
# MAGIC     ELSE DATEDIFF(CURRENT_TIMESTAMP(), timestamp_col)
# MAGIC   END
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validity Rules - Business Rule Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enum value check
# MAGIC CREATE OR REPLACE FUNCTION is_valid_enum(value STRING, allowed_values ARRAY<STRING>)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value IN (SELECT explode(allowed_values))
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- String length validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_length(value STRING, min_length INT, max_length INT)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND LENGTH(value) >= min_length AND LENGTH(value) <= max_length
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Regex pattern validation
# MAGIC CREATE OR REPLACE FUNCTION matches_regex(value STRING, pattern STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   value IS NOT NULL AND value RLIKE pattern
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business rule validation
# MAGIC CREATE OR REPLACE FUNCTION satisfies_business_rule(column_name STRING, table_name STRING, rule_expression STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   NOT EXISTS (
# MAGIC     SELECT 1 FROM table_name 
# MAGIC     WHERE NOT (rule_expression)
# MAGIC   )
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data type validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_data_type(column_name STRING, table_name STRING, expected_type STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   CASE 
# MAGIC     WHEN expected_type = 'INTEGER' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS INT) IS NULL)
# MAGIC     WHEN expected_type = 'DOUBLE' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS DOUBLE) IS NULL)
# MAGIC     WHEN expected_type = 'DATE' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS DATE) IS NULL)
# MAGIC     WHEN expected_type = 'TIMESTAMP' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS TIMESTAMP) IS NULL)
# MAGIC     WHEN expected_type = 'BOOLEAN' THEN 
# MAGIC       NOT EXISTS (SELECT 1 FROM table_name WHERE column_name IS NOT NULL AND TRY_CAST(column_name AS BOOLEAN) IS NULL)
# MAGIC     ELSE TRUE
# MAGIC   END
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Constraint validation
# MAGIC CREATE OR REPLACE FUNCTION satisfies_constraint(column_name STRING, table_name STRING, constraint_expression STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   NOT EXISTS (
# MAGIC     SELECT 1 FROM table_name 
# MAGIC     WHERE column_name IS NOT NULL AND NOT (constraint_expression)
# MAGIC   )
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Rules Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create data quality rules table
# MAGIC CREATE OR REPLACE TABLE data_quality_rules (
# MAGIC   rule_id STRING,
# MAGIC   rule_name STRING,
# MAGIC   dimension STRING,
# MAGIC   severity STRING,
# MAGIC   description STRING,
# MAGIC   sql_function STRING,
# MAGIC   example_usage STRING,
# MAGIC   threshold DOUBLE,
# MAGIC   column_name STRING,
# MAGIC   table_name STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert all data quality rules
# MAGIC INSERT INTO data_quality_rules VALUES
{self._generate_rules_insert_statements()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Usage Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Check email format validation
# MAGIC SELECT 
# MAGIC   email,
# MAGIC   is_valid_email(email) as is_valid,
# MAGIC   CASE WHEN is_valid_email(email) THEN 'Valid' ELSE 'Invalid' END as status
# MAGIC FROM users
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Check data completeness
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN is_null(email) THEN 1 ELSE 0 END) as null_emails,
# MAGIC   SUM(CASE WHEN is_empty_string(name) THEN 1 ELSE 0 END) as empty_names,
# MAGIC   ROUND(SUM(CASE WHEN is_not_null(email) AND is_not_empty_string(name) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completeness_percentage
# MAGIC FROM users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Check data freshness
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) as fresh_rows,
# MAGIC   SUM(CASE WHEN is_stale_data(updated_at, 30) THEN 1 ELSE 0 END) as stale_rows,
# MAGIC   ROUND(SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as freshness_percentage
# MAGIC FROM users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Comprehensive data quality report
# MAGIC WITH quality_metrics AS (
# MAGIC   SELECT 
# MAGIC     'users' as table_name,
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN is_not_null(email) THEN 1 ELSE 0 END) as valid_emails,
# MAGIC     SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as accurate_emails,
# MAGIC     SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) as fresh_data,
# MAGIC     SUM(CASE WHEN is_unique(user_id, 'users') THEN 1 ELSE 0 END) as unique_ids
# MAGIC   FROM users
# MAGIC )
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   total_rows,
# MAGIC   ROUND(valid_emails * 100.0 / total_rows, 2) as completeness_percentage,
# MAGIC   ROUND(accurate_emails * 100.0 / total_rows, 2) as accuracy_percentage,
# MAGIC   ROUND(fresh_data * 100.0 / total_rows, 2) as timeliness_percentage,
# MAGIC   ROUND(unique_ids * 100.0 / total_rows, 2) as consistency_percentage
# MAGIC FROM quality_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monitoring and Alerting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create data quality monitoring view
# MAGIC CREATE OR REPLACE VIEW data_quality_monitoring AS
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP() as check_timestamp,
# MAGIC   'users' as table_name,
# MAGIC   'email_format' as rule_name,
# MAGIC   'accuracy' as dimension,
# MAGIC   'critical' as severity,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_rows,
# MAGIC   ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_percentage,
# MAGIC   CASE 
# MAGIC     WHEN ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 THEN 'ALERT'
# MAGIC     ELSE 'OK'
# MAGIC   END as status
# MAGIC FROM users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query monitoring results
# MAGIC SELECT * FROM data_quality_monitoring;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Rule Execution and Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execute all data quality rules and store results
# MAGIC CREATE OR REPLACE TABLE data_quality_results AS
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP() as execution_time,
# MAGIC   'users' as table_name,
# MAGIC   'email_format_validation' as rule_id,
# MAGIC   'Email Format Validation' as rule_name,
# MAGIC   'accuracy' as dimension,
# MAGIC   'critical' as severity,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_rows,
# MAGIC   ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_percentage,
# MAGIC   CASE 
# MAGIC     WHEN ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 THEN 'FAIL'
# MAGIC     ELSE 'PASS'
# MAGIC   END as result_status
# MAGIC FROM users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data quality results
# MAGIC SELECT * FROM data_quality_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary
# MAGIC 
# MAGIC This notebook provides a comprehensive set of data quality rules implemented as SQL functions for Databricks. The rules cover:
# MAGIC 
# MAGIC - **{len([r for r in self.rules if r.dimension == 'accuracy'])} Accuracy Rules**: Format validation, range checks
# MAGIC - **{len([r for r in self.rules if r.dimension == 'completeness'])} Completeness Rules**: Null checks, empty string detection
# MAGIC - **{len([r for r in self.rules if r.dimension == 'consistency'])} Consistency Rules**: Uniqueness, foreign key validation
# MAGIC - **{len([r for r in self.rules if r.dimension == 'timeliness'])} Timeliness Rules**: Data freshness, SLA compliance
# MAGIC - **{len([r for r in self.rules if r.dimension == 'validity'])} Validity Rules**: Business rules, data type validation
# MAGIC 
# MAGIC **Total Rules**: {len(self.rules)}
# MAGIC 
# MAGIC All functions can be used directly in your SQL queries and are optimized for Databricks SQL execution.
'''
        
        return notebook_content
    
    def _generate_rules_insert_statements(self) -> str:
        """Generate INSERT statements for rules table"""
        insert_statements = []
        
        for rule in self.rules:
            values = f"('{rule.rule_id}', '{rule.rule_name}', '{rule.dimension}', '{rule.severity}', '{rule.description}', '{rule.sql_function}', '{rule.example_usage}', {rule.threshold or 'NULL'}, {f"'{rule.column_name}'" if rule.column_name else 'NULL'}, {f"'{rule.table_name}'" if rule.table_name else 'NULL'})"
            insert_statements.append(values)
        
        return ',\n'.join(insert_statements)
    
    def export_rules_table(self, output_file: str = "databricks_dq_rules.json") -> str:
        """Export rules as JSON table"""
        rules_data = self.get_rules_table()
        
        with open(output_file, 'w') as f:
            json.dump(rules_data, f, indent=2)
        
        return output_file
    
    def export_databricks_notebook(self, output_file: str = "databricks_dq_notebook.py") -> str:
        """Export complete Databricks notebook"""
        notebook_content = self.generate_databricks_notebook()
        
        with open(output_file, 'w') as f:
            f.write(notebook_content)
        
        return output_file


def main():
    """Generate Databricks data quality rules"""
    print("Generating Databricks Data Quality Rules...")
    
    # Initialize generator
    dq_generator = DatabricksDataQualityGenerator()
    
    # Get rules count by dimension
    dimensions = {}
    for rule in dq_generator.rules:
        if rule.dimension not in dimensions:
            dimensions[rule.dimension] = 0
        dimensions[rule.dimension] += 1
    
    print(f"\nGenerated {len(dq_generator.rules)} data quality rules:")
    for dimension, count in dimensions.items():
        print(f"  - {dimension.title()}: {count} rules")
    
    # Export rules table
    json_file = dq_generator.export_rules_table()
    print(f"\n✓ Rules table exported to: {json_file}")
    
    # Export Databricks notebook
    notebook_file = dq_generator.export_databricks_notebook()
    print(f"✓ Databricks notebook exported to: {notebook_file}")
    
    # Print sample rules
    print(f"\nSample rules:")
    for rule in dq_generator.rules[:5]:
        print(f"  - {rule.rule_name}: {rule.sql_function}")
    
    print(f"\nAll rules are ready for use in Databricks!")


if __name__ == "__main__":
    main()