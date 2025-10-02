# Databricks Data Quality Functions - Quick Reference

## 🚀 Quick Start

```sql
-- 1. Create all functions (run once)
-- Copy and paste the content from databricks_dq_notebook.py

-- 2. Use functions in your queries
SELECT 
  email,
  is_valid_email(email) as email_valid,
  is_null(phone) as phone_missing,
  is_fresh_data(updated_at, 24) as data_fresh
FROM users;
```

## 📋 Complete Function Reference

### Accuracy Functions
| Function | Purpose | Example |
|----------|---------|---------|
| `is_valid_email(email)` | Email format validation | `SELECT is_valid_email('user@example.com')` |
| `is_valid_phone(phone)` | Phone number format | `SELECT is_valid_phone('+1-555-123-4567')` |
| `is_valid_uuid(uuid)` | UUID format validation | `SELECT is_valid_uuid('123e4567-e89b-12d3-a456-426614174000')` |
| `is_valid_date(date_str)` | Date format (YYYY-MM-DD) | `SELECT is_valid_date('2023-12-25')` |
| `is_valid_credit_card(card)` | Credit card format | `SELECT is_valid_credit_card('1234-5678-9012-3456')` |
| `is_valid_ssn(ssn)` | SSN format (XXX-XX-XXXX) | `SELECT is_valid_ssn('123-45-6789')` |
| `is_in_range(value, min, max)` | Numeric range check | `SELECT is_in_range(age, 0, 120)` |
| `is_positive(value)` | Positive number check | `SELECT is_positive(amount)` |
| `is_non_negative(value)` | Non-negative number check | `SELECT is_non_negative(quantity)` |

### Completeness Functions
| Function | Purpose | Example |
|----------|---------|---------|
| `is_null(value)` | Check for null values | `SELECT is_null(email)` |
| `is_not_null(value)` | Check for non-null values | `SELECT is_not_null(user_id)` |
| `is_empty_string(value)` | Check for empty strings | `SELECT is_empty_string(name)` |
| `is_not_empty_string(value)` | Check for non-empty strings | `SELECT is_not_empty_string(description)` |
| `has_whitespace(value)` | Check for whitespace | `SELECT has_whitespace(name)` |
| `is_required_field(value)` | Required field check | `SELECT is_required_field(email)` |

### Consistency Functions
| Function | Purpose | Example |
|----------|---------|---------|
| `is_unique(column, table)` | Unique value check | `SELECT is_unique(email, 'users')` |
| `has_duplicates(column, table)` | Duplicate check | `SELECT has_duplicates(phone, 'contacts')` |
| `is_valid_primary_key(column, table)` | Primary key validation | `SELECT is_valid_primary_key(user_id, 'users')` |
| `is_valid_foreign_key(fk_col, fk_table, ref_table, ref_col)` | Foreign key check | `SELECT is_valid_foreign_key(user_id, 'orders', 'users', 'id')` |
| `check_referential_integrity(table1, col1, table2, col2)` | Referential integrity | `SELECT check_referential_integrity('orders', 'user_id', 'users', 'id')` |
| `is_consistent_data_type(column, table, type)` | Data type consistency | `SELECT is_consistent_data_type(amount, 'transactions', 'DOUBLE')` |

### Timeliness Functions
| Function | Purpose | Example |
|----------|---------|---------|
| `is_fresh_data(timestamp, hours)` | Data freshness check | `SELECT is_fresh_data(updated_at, 24)` |
| `meets_sla(timestamp, sla_hours)` | SLA compliance check | `SELECT meets_sla(created_at, 6)` |
| `is_stale_data(timestamp, stale_hours)` | Stale data check | `SELECT is_stale_data(last_login, 30)` |
| `has_future_date(timestamp)` | Future date check | `SELECT has_future_date(created_at)` |
| `get_data_age(timestamp)` | Data age in days | `SELECT get_data_age(updated_at)` |

### Validity Functions
| Function | Purpose | Example |
|----------|---------|---------|
| `is_valid_enum(value, allowed_values)` | Enum value validation | `SELECT is_valid_enum(status, ['active', 'inactive'])` |
| `is_valid_length(value, min_len, max_len)` | String length validation | `SELECT is_valid_length(name, 2, 50)` |
| `matches_regex(value, pattern)` | Regex pattern validation | `SELECT matches_regex(postal_code, '^[0-9]{5}$')` |
| `satisfies_business_rule(column, table, rule)` | Business rule validation | `SELECT satisfies_business_rule('age', 'users', 'age >= 18')` |
| `is_valid_data_type(column, table, type)` | Data type validation | `SELECT is_valid_data_type('amount', 'transactions', 'DOUBLE')` |
| `satisfies_constraint(column, table, constraint)` | Constraint validation | `SELECT satisfies_constraint('quantity', 'orders', 'quantity > 0')` |

## 🎯 Common Use Cases

### 1. Data Quality Dashboard
```sql
-- Create a comprehensive data quality dashboard
SELECT 
  'users' as table_name,
  COUNT(*) as total_rows,
  ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as email_accuracy,
  ROUND(SUM(CASE WHEN is_not_null(phone) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as phone_completeness,
  ROUND(SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as data_freshness,
  ROUND(SUM(CASE WHEN is_unique(user_id, 'users') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as id_uniqueness
FROM users;
```

### 2. Data Quality Alerts
```sql
-- Find records that fail quality checks
SELECT 
  user_id,
  email,
  CASE 
    WHEN NOT is_valid_email(email) THEN 'Invalid Email'
    WHEN is_null(phone) THEN 'Missing Phone'
    WHEN NOT is_fresh_data(updated_at, 24) THEN 'Stale Data'
    ELSE 'OK'
  END as quality_issue
FROM users
WHERE NOT is_valid_email(email) 
   OR is_null(phone) 
   OR NOT is_fresh_data(updated_at, 24);
```

### 3. Data Quality Monitoring
```sql
-- Create a monitoring table
CREATE OR REPLACE TABLE dq_monitoring AS
SELECT 
  CURRENT_TIMESTAMP() as check_time,
  'users' as table_name,
  'email_validation' as rule_name,
  COUNT(*) as total_rows,
  SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_rows,
  ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_percentage,
  CASE 
    WHEN ROUND(SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 THEN 'ALERT'
    ELSE 'OK'
  END as status
FROM users;
```

### 4. Data Quality Report
```sql
-- Generate a detailed quality report
WITH quality_metrics AS (
  SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN is_valid_email(email) THEN 1 ELSE 0 END) as valid_emails,
    SUM(CASE WHEN is_not_null(phone) THEN 1 ELSE 0 END) as valid_phones,
    SUM(CASE WHEN is_fresh_data(updated_at, 24) THEN 1 ELSE 0 END) as fresh_data,
    SUM(CASE WHEN is_unique(user_id, 'users') THEN 1 ELSE 0 END) as unique_ids
  FROM users
)
SELECT 
  total_rows,
  ROUND(valid_emails * 100.0 / total_rows, 2) as email_accuracy_pct,
  ROUND(valid_phones * 100.0 / total_rows, 2) as phone_completeness_pct,
  ROUND(fresh_data * 100.0 / total_rows, 2) as data_freshness_pct,
  ROUND(unique_ids * 100.0 / total_rows, 2) as id_uniqueness_pct
FROM quality_metrics;
```

## 🔧 Customization Examples

### 1. Custom Business Rules
```sql
-- Check custom business rules
SELECT 
  user_id,
  age,
  CASE 
    WHEN age < 18 THEN 'Underage'
    WHEN age > 65 THEN 'Senior'
    ELSE 'Valid Age'
  END as age_category
FROM users
WHERE age IS NOT NULL;
```

### 2. Custom Format Validation
```sql
-- Custom postal code validation
SELECT 
  postal_code,
  CASE 
    WHEN postal_code RLIKE '^[0-9]{5}(-[0-9]{4})?$' THEN 'Valid US ZIP'
    WHEN postal_code RLIKE '^[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$' THEN 'Valid Canadian Postal'
    ELSE 'Invalid Format'
  END as postal_validation
FROM addresses;
```

### 3. Custom Monitoring Thresholds
```sql
-- Custom quality thresholds
SELECT 
  table_name,
  rule_name,
  quality_percentage,
  CASE 
    WHEN quality_percentage >= 99 THEN 'EXCELLENT'
    WHEN quality_percentage >= 95 THEN 'GOOD'
    WHEN quality_percentage >= 90 THEN 'WARNING'
    ELSE 'CRITICAL'
  END as quality_grade
FROM dq_monitoring;
```

## 📊 Performance Tips

1. **Use indexes** on columns you're validating
2. **Filter data** before applying quality checks
3. **Use sampling** for large tables during development
4. **Cache results** for frequently used quality checks
5. **Schedule monitoring** during off-peak hours

## 🚨 Error Handling

```sql
-- Safe function calls with error handling
SELECT 
  user_id,
  CASE 
    WHEN email IS NULL THEN 'NULL'
    WHEN TRY_CAST(email AS STRING) IS NULL THEN 'INVALID_TYPE'
    WHEN is_valid_email(email) THEN 'VALID'
    ELSE 'INVALID_FORMAT'
  END as email_status
FROM users;
```

This quick reference provides everything you need to start using the data quality functions immediately in your Databricks environment!