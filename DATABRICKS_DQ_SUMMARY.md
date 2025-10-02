# Databricks Data Quality Rules - Complete Solution

## 🎯 Overview

I've created a comprehensive data quality rules system specifically designed for **Databricks** that generates **32 SQL functions** covering all five data quality dimensions. The system provides both a **rules table** and **ready-to-use SQL functions** that can be directly executed in Databricks.

## 📊 Generated Rules Summary

| Dimension | Count | Description |
|-----------|-------|-------------|
| **Accuracy** | 9 rules | Format validation, range checks, data correctness |
| **Completeness** | 6 rules | Null checks, empty string detection, required fields |
| **Consistency** | 6 rules | Uniqueness, foreign key validation, referential integrity |
| **Timeliness** | 5 rules | Data freshness, SLA compliance, stale data detection |
| **Validity** | 6 rules | Business rules, data type validation, constraints |
| **Total** | **32 rules** | Complete data quality coverage |

## 🚀 Key Features

### 1. **SQL Functions Ready for Databricks**
All rules are implemented as SQL functions that can be used directly in your queries:

```sql
-- Example usage
SELECT 
  email,
  is_valid_email(email) as is_valid,
  is_null(email) as is_missing,
  is_fresh_data(updated_at, 24) as is_fresh
FROM users;
```

### 2. **Comprehensive Rule Coverage**
- **Format Validation**: Email, phone, UUID, date, credit card, SSN
- **Range Validation**: Numeric ranges, positive numbers, non-negative values
- **Completeness**: Null checks, empty strings, required fields
- **Consistency**: Uniqueness, duplicates, primary/foreign keys
- **Timeliness**: Freshness, SLA compliance, stale data detection
- **Validity**: Business rules, data types, constraints

### 3. **Rules Table for Management**
All rules are stored in a structured table format for easy management and monitoring.

## 📁 Generated Files

1. **`databricks_dq_rules.json`** - Rules table in JSON format
2. **`databricks_dq_notebook.py`** - Complete Databricks notebook
3. **`databricks_dq_rules.py`** - Python generator code

## 🔧 SQL Functions by Category

### Accuracy Rules (9 functions)
```sql
is_valid_email(column_name)                    -- Email format validation
is_valid_phone(column_name)                    -- Phone number format
is_valid_uuid(column_name)                     -- UUID format validation
is_valid_date(column_name)                     -- Date format (YYYY-MM-DD)
is_valid_credit_card(column_name)              -- Credit card format
is_valid_ssn(column_name)                      -- SSN format (XXX-XX-XXXX)
is_in_range(column_name, min_value, max_value) -- Numeric range validation
is_positive(column_name)                       -- Positive number check
is_non_negative(column_name)                   -- Non-negative number check
```

### Completeness Rules (6 functions)
```sql
is_null(column_name)                           -- Null value check
is_not_null(column_name)                       -- Not null check
is_empty_string(column_name)                   -- Empty string check
is_not_empty_string(column_name)               -- Not empty string check
has_whitespace(column_name)                    -- Whitespace check
is_required_field(column_name)                 -- Required field check
```

### Consistency Rules (6 functions)
```sql
is_unique(column_name, table_name)             -- Unique value check
has_duplicates(column_name, table_name)        -- Duplicate check
is_valid_primary_key(column_name, table_name)  -- Primary key validation
is_valid_foreign_key(fk_col, fk_table, ref_table, ref_col) -- Foreign key check
check_referential_integrity(table1, col1, table2, col2)    -- Referential integrity
is_consistent_data_type(column_name, table_name, expected_type) -- Data type consistency
```

### Timeliness Rules (5 functions)
```sql
is_fresh_data(timestamp_column, hours_threshold) -- Data freshness check
meets_sla(timestamp_column, sla_hours)           -- SLA compliance check
is_stale_data(timestamp_column, stale_hours)     -- Stale data check
has_future_date(timestamp_column)                -- Future date check
get_data_age(timestamp_column)                   -- Data age calculation
```

### Validity Rules (6 functions)
```sql
is_valid_enum(column_name, allowed_values)      -- Enum value validation
is_valid_length(column_name, min_len, max_len)  -- String length validation
matches_regex(column_name, pattern)             -- Regex pattern validation
satisfies_business_rule(column_name, table_name, rule_expression) -- Business rule validation
is_valid_data_type(column_name, table_name, expected_type) -- Data type validation
satisfies_constraint(column_name, table_name, constraint_expression) -- Constraint validation
```

## 🎯 Usage Examples

### 1. Basic Data Quality Check
```sql
-- Check email format validation
SELECT 
  email,
  is_valid_email(email) as is_valid,
  CASE WHEN is_valid_email(email) THEN 'Valid' ELSE 'Invalid' END as status
FROM users
LIMIT 10;
```

### 2. Comprehensive Quality Report
```sql
-- Generate comprehensive data quality report
WITH quality_metrics AS (
  SELECT 
    'users' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN is_not_null(email) THEN 1 ELSE 0 END) as valid_emails,
    SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as accurate_emails,
    SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) as fresh_data,
    SUM(CASE WHEN is_unique(user_id, 'users') THEN 1 ELSE 0 END) as unique_ids
  FROM users
)
SELECT 
  table_name,
  total_rows,
  ROUND(valid_emails * 100.0 / total_rows, 2) as completeness_percentage,
  ROUND(accurate_emails * 100.0 / total_rows, 2) as accuracy_percentage,
  ROUND(fresh_data * 100.0 / total_rows, 2) as timeliness_percentage,
  ROUND(unique_ids * 100.0 / total_rows, 2) as consistency_percentage
FROM quality_metrics;
```

### 3. Data Quality Monitoring
```sql
-- Create monitoring view
CREATE OR REPLACE VIEW data_quality_monitoring AS
SELECT 
  CURRENT_TIMESTAMP() as check_timestamp,
  'users' as table_name,
  'email_format' as rule_name,
  'accuracy' as dimension,
  'critical' as severity,
  COUNT(*) as total_rows,
  SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_rows,
  ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_percentage,
  CASE 
    WHEN ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 THEN 'ALERT'
    ELSE 'OK'
  END as status
FROM users;
```

## 🚀 Getting Started

### 1. Import the Databricks Notebook
- Copy the content from `databricks_dq_notebook.py`
- Create a new Databricks notebook
- Paste the content and run all cells

### 2. Use the SQL Functions
```sql
-- Example: Check data quality for a specific table
SELECT 
  COUNT(*) as total_rows,
  SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_emails,
  SUM(CASE WHEN is_null(phone) THEN 1 ELSE 0 END) as missing_phones,
  SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) as fresh_records
FROM your_table_name;
```

### 3. Set Up Monitoring
```sql
-- Create a monitoring table
CREATE OR REPLACE TABLE data_quality_results AS
SELECT 
  CURRENT_TIMESTAMP() as execution_time,
  'your_table' as table_name,
  'email_format_validation' as rule_id,
  COUNT(*) as total_rows,
  SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_rows,
  ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_percentage
FROM your_table;
```

## 📈 Benefits

1. **Ready-to-Use**: All functions are immediately usable in Databricks
2. **Comprehensive**: Covers all five data quality dimensions
3. **Scalable**: Functions work with any table size
4. **Maintainable**: Centralized rule management
5. **Monitorable**: Built-in monitoring and alerting capabilities
6. **Flexible**: Easy to customize and extend

## 🔧 Customization

You can easily customize the rules by:
1. Modifying the `databricks_dq_rules.py` file
2. Adding new rules to specific dimensions
3. Adjusting severity levels and thresholds
4. Creating custom business rules

## 📊 Rule Management

The rules are stored in a structured table format that allows you to:
- Query all available rules
- Filter by dimension or severity
- Track rule usage and performance
- Generate reports and dashboards

## 🎯 Next Steps

1. **Import the notebook** into your Databricks workspace
2. **Run the setup cells** to create all SQL functions
3. **Test the functions** with your data
4. **Set up monitoring** for continuous quality checks
5. **Customize rules** based on your specific requirements

This solution provides everything you need to implement comprehensive data quality monitoring in Databricks with minimal setup and maximum flexibility.