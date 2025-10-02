# Databricks Data Quality Rules
# Generated on 2025-10-02 16:38:51

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
# MAGIC   email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phone number format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_phone(phone STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   phone IS NOT NULL AND phone RLIKE '^\+?[\d\s\-\(\)]{10,}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UUID format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_uuid(uuid STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   uuid IS NOT NULL AND uuid RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date format validation (YYYY-MM-DD)
# MAGIC CREATE OR REPLACE FUNCTION is_valid_date(date_str STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   date_str IS NOT NULL AND date_str RLIKE '^\d{4}-\d{2}-\d{2}$' AND 
# MAGIC   TRY_CAST(date_str AS DATE) IS NOT NULL
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Credit card format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_credit_card(card STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   card IS NOT NULL AND card RLIKE '^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SSN format validation
# MAGIC CREATE OR REPLACE FUNCTION is_valid_ssn(ssn STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   ssn IS NOT NULL AND ssn RLIKE '^\d{3}-\d{2}-\d{4}$'
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
# MAGIC   value IS NOT NULL AND (value != TRIM(value) OR value RLIKE '\s')
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
('email_format_validation', 'Email Format Validation', 'accuracy', 'critical', 'Validates email format using regex pattern', 'is_valid_email(column_name)', 'SELECT is_valid_email(email) FROM users', NULL, 'email', NULL),
('phone_format_validation', 'Phone Number Format Validation', 'accuracy', 'warning', 'Validates phone number format', 'is_valid_phone(column_name)', 'SELECT is_valid_phone(phone_number) FROM contacts', NULL, 'phone_number', NULL),
('uuid_format_validation', 'UUID Format Validation', 'accuracy', 'critical', 'Validates UUID format', 'is_valid_uuid(column_name)', 'SELECT is_valid_uuid(id) FROM transactions', NULL, 'id', NULL),
('date_format_validation', 'Date Format Validation', 'accuracy', 'warning', 'Validates date format (YYYY-MM-DD)', 'is_valid_date(column_name)', 'SELECT is_valid_date(created_date) FROM orders', NULL, 'created_date', NULL),
('credit_card_format_validation', 'Credit Card Format Validation', 'accuracy', 'critical', 'Validates credit card number format', 'is_valid_credit_card(column_name)', 'SELECT is_valid_credit_card(card_number) FROM payments', NULL, 'card_number', NULL),
('ssn_format_validation', 'SSN Format Validation', 'accuracy', 'critical', 'Validates SSN format (XXX-XX-XXXX)', 'is_valid_ssn(column_name)', 'SELECT is_valid_ssn(ssn) FROM employees', NULL, 'ssn', NULL),
('numeric_range_validation', 'Numeric Range Validation', 'accuracy', 'warning', 'Validates numeric value is within specified range', 'is_in_range(column_name, min_value, max_value)', 'SELECT is_in_range(age, 0, 120) FROM users', NULL, 'age', NULL),
('positive_number_validation', 'Positive Number Validation', 'accuracy', 'warning', 'Validates number is positive', 'is_positive(column_name)', 'SELECT is_positive(amount) FROM transactions', NULL, 'amount', NULL),
('non_negative_number_validation', 'Non-Negative Number Validation', 'accuracy', 'warning', 'Validates number is non-negative', 'is_non_negative(column_name)', 'SELECT is_non_negative(quantity) FROM order_items', NULL, 'quantity', NULL),
('null_check', 'Null Value Check', 'completeness', 'critical', 'Checks for null values in column', 'is_null(column_name)', 'SELECT is_null(email) FROM users', NULL, 'email', NULL),
('not_null_check', 'Not Null Check', 'completeness', 'critical', 'Checks for non-null values in column', 'is_not_null(column_name)', 'SELECT is_not_null(user_id) FROM users', NULL, 'user_id', NULL),
('empty_string_check', 'Empty String Check', 'completeness', 'warning', 'Checks for empty or whitespace-only strings', 'is_empty_string(column_name)', 'SELECT is_empty_string(name) FROM customers', NULL, 'name', NULL),
('not_empty_string_check', 'Not Empty String Check', 'completeness', 'critical', 'Checks for non-empty strings', 'is_not_empty_string(column_name)', 'SELECT is_not_empty_string(description) FROM products', NULL, 'description', NULL),
('whitespace_check', 'Whitespace Check', 'completeness', 'warning', 'Checks for leading/trailing whitespace', 'has_whitespace(column_name)', 'SELECT has_whitespace(name) FROM customers', NULL, 'name', NULL),
('required_field_check', 'Required Field Check', 'completeness', 'critical', 'Checks if required field has value', 'is_required_field(column_name)', 'SELECT is_required_field(email) FROM users', NULL, 'email', NULL),
('unique_value_check', 'Unique Value Check', 'consistency', 'critical', 'Checks if column values are unique', 'is_unique(column_name)', 'SELECT is_unique(email) FROM users', NULL, 'email', NULL),
('duplicate_check', 'Duplicate Value Check', 'consistency', 'warning', 'Checks for duplicate values in column', 'has_duplicates(column_name)', 'SELECT has_duplicates(phone) FROM contacts', NULL, 'phone', NULL),
('primary_key_check', 'Primary Key Check', 'consistency', 'critical', 'Validates primary key uniqueness', 'is_valid_primary_key(column_name)', 'SELECT is_valid_primary_key(user_id) FROM users', NULL, 'user_id', NULL),
('foreign_key_check', 'Foreign Key Check', 'consistency', 'critical', 'Validates foreign key references', 'is_valid_foreign_key(column_name, referenced_table, referenced_column)', 'SELECT is_valid_foreign_key(user_id, 'users', 'id') FROM orders', NULL, 'user_id', NULL),
('referential_integrity_check', 'Referential Integrity Check', 'consistency', 'critical', 'Checks referential integrity between tables', 'check_referential_integrity(table1, column1, table2, column2)', 'SELECT check_referential_integrity('orders', 'user_id', 'users', 'id')', NULL, 'user_id', NULL),
('data_type_consistency_check', 'Data Type Consistency Check', 'consistency', 'warning', 'Checks data type consistency across similar columns', 'is_consistent_data_type(column_name, expected_type)', 'SELECT is_consistent_data_type(amount, 'decimal') FROM transactions', NULL, 'amount', NULL),
('data_freshness_check', 'Data Freshness Check', 'timeliness', 'critical', 'Checks if data is fresh within specified hours', 'is_fresh_data(timestamp_column, hours_threshold)', 'SELECT is_fresh_data(updated_at, 24) FROM users', NULL, 'updated_at', NULL),
('sla_compliance_check', 'SLA Compliance Check', 'timeliness', 'critical', 'Checks if data meets SLA requirements', 'meets_sla(timestamp_column, sla_hours)', 'SELECT meets_sla(created_at, 6) FROM orders', NULL, 'created_at', NULL),
('stale_data_check', 'Stale Data Check', 'timeliness', 'warning', 'Identifies stale data beyond threshold', 'is_stale_data(timestamp_column, stale_hours)', 'SELECT is_stale_data(last_login, 30) FROM users', NULL, 'last_login', NULL),
('future_date_check', 'Future Date Check', 'timeliness', 'warning', 'Checks for future dates in timestamp columns', 'has_future_date(timestamp_column)', 'SELECT has_future_date(created_at) FROM orders', NULL, 'created_at', NULL),
('data_age_check', 'Data Age Check', 'timeliness', 'info', 'Calculates age of data in days', 'get_data_age(timestamp_column)', 'SELECT get_data_age(updated_at) FROM products', NULL, 'updated_at', NULL),
('enum_value_check', 'Enum Value Check', 'validity', 'warning', 'Validates column values against allowed enum values', 'is_valid_enum(column_name, allowed_values)', 'SELECT is_valid_enum(status, ['active', 'inactive', 'pending']) FROM users', NULL, 'status', NULL),
('length_validation', 'String Length Validation', 'validity', 'warning', 'Validates string length within bounds', 'is_valid_length(column_name, min_length, max_length)', 'SELECT is_valid_length(name, 2, 50) FROM customers', NULL, 'name', NULL),
('regex_validation', 'Regex Pattern Validation', 'validity', 'critical', 'Validates column against regex pattern', 'matches_regex(column_name, pattern)', 'SELECT matches_regex(postal_code, '^[0-9]{5}(-[0-9]{4})?$') FROM addresses', NULL, 'postal_code', NULL),
('business_rule_validation', 'Business Rule Validation', 'validity', 'warning', 'Validates custom business rules', 'satisfies_business_rule(column_name, rule_expression)', 'SELECT satisfies_business_rule(age, 'age >= 18 AND age <= 65') FROM employees', NULL, 'age', NULL),
('data_type_validation', 'Data Type Validation', 'validity', 'critical', 'Validates data type of column', 'is_valid_data_type(column_name, expected_type)', 'SELECT is_valid_data_type(amount, 'decimal') FROM transactions', NULL, 'amount', NULL),
('constraint_validation', 'Constraint Validation', 'validity', 'critical', 'Validates data constraints', 'satisfies_constraint(column_name, constraint_expression)', 'SELECT satisfies_constraint(quantity, 'quantity > 0') FROM order_items', NULL, 'quantity', NULL)

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
# MAGIC - **9 Accuracy Rules**: Format validation, range checks
# MAGIC - **6 Completeness Rules**: Null checks, empty string detection
# MAGIC - **6 Consistency Rules**: Uniqueness, foreign key validation
# MAGIC - **5 Timeliness Rules**: Data freshness, SLA compliance
# MAGIC - **6 Validity Rules**: Business rules, data type validation
# MAGIC 
# MAGIC **Total Rules**: 32
# MAGIC 
# MAGIC All functions can be used directly in your SQL queries and are optimized for Databricks SQL execution.
