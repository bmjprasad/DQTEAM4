-- Range validation for user_id
-- Validate user_id is within expected range
SELECT 
                        'users_user_id_range_validation' as rule_id,
                        'users' as table_name,
                        'user_id' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN user_id IS NOT NULL AND (user_id >= 1 AND user_id <= 1000) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN user_id IS NOT NULL AND (user_id >= 1 AND user_id <= 1000) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM users

-- Format validation for email
-- Validate email matches expected format pattern
SELECT 
                        'users_email_format_validation' as rule_id,
                        'users' as table_name,
                        'email' as column_name,
                        'accuracy' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM users

-- Range validation for age
-- Validate age is within expected range
SELECT 
                        'users_age_range_validation' as rule_id,
                        'users' as table_name,
                        'age' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN age IS NOT NULL AND (age >= 18.0 AND age <= 79.0) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN age IS NOT NULL AND (age >= 18.0 AND age <= 79.0) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM users

-- Format validation for phone
-- Validate phone matches expected format pattern
SELECT 
                        'users_phone_format_validation' as rule_id,
                        'users' as table_name,
                        'phone' as column_name,
                        'accuracy' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN phone IS NOT NULL AND phone RLIKE '^\+?[\d\s\-\(\)]{10,}$' THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN phone IS NOT NULL AND phone RLIKE '^\+?[\d\s\-\(\)]{10,}$' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM users

-- Null value check for user_id
-- Check for null values in user_id
SELECT 
                    'users_user_id_null_check' as rule_id,
                    'users' as table_name,
                    'user_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Null value check for email
-- Check for null values in email
SELECT 
                    'users_email_null_check' as rule_id,
                    'users' as table_name,
                    'email' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Empty string check for email
-- Check for empty strings in email
SELECT 
                        'users_email_empty_string_check' as rule_id,
                        'users' as table_name,
                        'email' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN email = '' OR TRIM(email) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN email = '' OR TRIM(email) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM users

-- Null value check for age
-- Check for null values in age
SELECT 
                    'users_age_null_check' as rule_id,
                    'users' as table_name,
                    'age' as column_name,
                    'completeness' as dimension,
                    'warning' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Null value check for created_at
-- Check for null values in created_at
SELECT 
                    'users_created_at_null_check' as rule_id,
                    'users' as table_name,
                    'created_at' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Null value check for status
-- Check for null values in status
SELECT 
                    'users_status_null_check' as rule_id,
                    'users' as table_name,
                    'status' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Empty string check for status
-- Check for empty strings in status
SELECT 
                        'users_status_empty_string_check' as rule_id,
                        'users' as table_name,
                        'status' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN status = '' OR TRIM(status) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN status = '' OR TRIM(status) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM users

-- Null value check for phone
-- Check for null values in phone
SELECT 
                    'users_phone_null_check' as rule_id,
                    'users' as table_name,
                    'phone' as column_name,
                    'completeness' as dimension,
                    'warning' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM users

-- Empty string check for phone
-- Check for empty strings in phone
SELECT 
                        'users_phone_empty_string_check' as rule_id,
                        'users' as table_name,
                        'phone' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN phone = '' OR TRIM(phone) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN phone = '' OR TRIM(phone) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM users

-- Data freshness check for users
-- Check if data is fresh within 24 hours
SELECT 
                    'users_data_freshness' as rule_id,
                    'users' as table_name,
                    'data_freshness' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:57.989861') <= 24 THEN 1 ELSE 0 END) as fresh_rows,
                    DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:57.989861') as hours_since_update
                FROM users

-- SLA compliance check for users
-- Check if data meets SLA of 6 hours
SELECT 
                    'users_sla_compliance' as rule_id,
                    'users' as table_name,
                    'sla_compliance' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:57.989861') <= 6 THEN 1 ELSE 0 END as sla_met
                FROM users

-- Data type validation for user_id
-- Validate user_id has correct data type
SELECT 
                    'users_user_id_data_type_validation' as rule_id,
                    'users' as table_name,
                    'user_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN user_id IS NULL OR CAST(user_id AS smallint) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Business rule validation for user_id
-- Validate business rule: user_id >= 0
SELECT 
                            'users_user_id_business_rule_1' as rule_id,
                            'users' as table_name,
                            'user_id' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN user_id >= 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Data type validation for email
-- Validate email has correct data type
SELECT 
                    'users_email_data_type_validation' as rule_id,
                    'users' as table_name,
                    'email' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN email IS NULL OR CAST(email AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Business rule validation for email
-- Validate business rule: LENGTH(email) >= 17
SELECT 
                            'users_email_business_rule_1' as rule_id,
                            'users' as table_name,
                            'email' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(email) >= 17 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Business rule validation for email
-- Validate business rule: LENGTH(email) <= 20
SELECT 
                            'users_email_business_rule_2' as rule_id,
                            'users' as table_name,
                            'email' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(email) <= 20 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Data type validation for age
-- Validate age has correct data type
SELECT 
                    'users_age_data_type_validation' as rule_id,
                    'users' as table_name,
                    'age' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN age IS NULL OR CAST(age AS double) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Business rule validation for age
-- Validate business rule: age >= 0
SELECT 
                            'users_age_business_rule_1' as rule_id,
                            'users' as table_name,
                            'age' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN age >= 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Data type validation for created_at
-- Validate created_at has correct data type
SELECT 
                    'users_created_at_data_type_validation' as rule_id,
                    'users' as table_name,
                    'created_at' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN created_at IS NULL OR CAST(created_at AS timestamp) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Data type validation for status
-- Validate status has correct data type
SELECT 
                    'users_status_data_type_validation' as rule_id,
                    'users' as table_name,
                    'status' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN status IS NULL OR CAST(status AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Business rule validation for status
-- Validate business rule: LENGTH(status) >= 6
SELECT 
                            'users_status_business_rule_1' as rule_id,
                            'users' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(status) >= 6 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Business rule validation for status
-- Validate business rule: LENGTH(status) <= 8
SELECT 
                            'users_status_business_rule_2' as rule_id,
                            'users' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(status) <= 8 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Business rule validation for status
-- Validate business rule: status IN ('active', 'inactive', 'pending')
SELECT 
                            'users_status_business_rule_3' as rule_id,
                            'users' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN status IN ('active', 'inactive', 'pending') THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Data type validation for phone
-- Validate phone has correct data type
SELECT 
                    'users_phone_data_type_validation' as rule_id,
                    'users' as table_name,
                    'phone' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN phone IS NULL OR CAST(phone AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM users

-- Business rule validation for phone
-- Validate business rule: LENGTH(phone) >= 15
SELECT 
                            'users_phone_business_rule_1' as rule_id,
                            'users' as table_name,
                            'phone' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(phone) >= 15 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Business rule validation for phone
-- Validate business rule: LENGTH(phone) <= 15
SELECT 
                            'users_phone_business_rule_2' as rule_id,
                            'users' as table_name,
                            'phone' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(phone) <= 15 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM users

-- Range validation for order_id
-- Validate order_id is within expected range
SELECT 
                        'orders_order_id_range_validation' as rule_id,
                        'orders' as table_name,
                        'order_id' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN order_id IS NOT NULL AND (order_id >= 1 AND order_id <= 5000) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN order_id IS NOT NULL AND (order_id >= 1 AND order_id <= 5000) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM orders

-- Range validation for user_id
-- Validate user_id is within expected range
SELECT 
                        'orders_user_id_range_validation' as rule_id,
                        'orders' as table_name,
                        'user_id' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN user_id IS NOT NULL AND (user_id >= 1 AND user_id <= 99999) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN user_id IS NOT NULL AND (user_id >= 1 AND user_id <= 99999) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM orders

-- Range validation for amount
-- Validate amount is within expected range
SELECT 
                        'orders_amount_range_validation' as rule_id,
                        'orders' as table_name,
                        'amount' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN amount IS NOT NULL AND (amount >= 10.18 AND amount <= 999.74) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN amount IS NOT NULL AND (amount >= 10.18 AND amount <= 999.74) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM orders

-- Null value check for order_id
-- Check for null values in order_id
SELECT 
                    'orders_order_id_null_check' as rule_id,
                    'orders' as table_name,
                    'order_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM orders

-- Null value check for user_id
-- Check for null values in user_id
SELECT 
                    'orders_user_id_null_check' as rule_id,
                    'orders' as table_name,
                    'user_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM orders

-- Null value check for amount
-- Check for null values in amount
SELECT 
                    'orders_amount_null_check' as rule_id,
                    'orders' as table_name,
                    'amount' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM orders

-- Null value check for order_date
-- Check for null values in order_date
SELECT 
                    'orders_order_date_null_check' as rule_id,
                    'orders' as table_name,
                    'order_date' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM orders

-- Null value check for status
-- Check for null values in status
SELECT 
                    'orders_status_null_check' as rule_id,
                    'orders' as table_name,
                    'status' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM orders

-- Empty string check for status
-- Check for empty strings in status
SELECT 
                        'orders_status_empty_string_check' as rule_id,
                        'orders' as table_name,
                        'status' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN status = '' OR TRIM(status) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN status = '' OR TRIM(status) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM orders

-- Data freshness check for orders
-- Check if data is fresh within 24 hours
SELECT 
                    'orders_data_freshness' as rule_id,
                    'orders' as table_name,
                    'data_freshness' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:58.011165') <= 24 THEN 1 ELSE 0 END) as fresh_rows,
                    DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:58.011165') as hours_since_update
                FROM orders

-- SLA compliance check for orders
-- Check if data meets SLA of 6 hours
SELECT 
                    'orders_sla_compliance' as rule_id,
                    'orders' as table_name,
                    'sla_compliance' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:32:58.011165') <= 6 THEN 1 ELSE 0 END as sla_met
                FROM orders

-- Data type validation for order_id
-- Validate order_id has correct data type
SELECT 
                    'orders_order_id_data_type_validation' as rule_id,
                    'orders' as table_name,
                    'order_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_id IS NULL OR CAST(order_id AS smallint) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM orders

-- Business rule validation for order_id
-- Validate business rule: order_id >= 0
SELECT 
                            'orders_order_id_business_rule_1' as rule_id,
                            'orders' as table_name,
                            'order_id' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN order_id >= 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders

-- Data type validation for user_id
-- Validate user_id has correct data type
SELECT 
                    'orders_user_id_data_type_validation' as rule_id,
                    'orders' as table_name,
                    'user_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN user_id IS NULL OR CAST(user_id AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM orders

-- Business rule validation for user_id
-- Validate business rule: user_id >= 0
SELECT 
                            'orders_user_id_business_rule_1' as rule_id,
                            'orders' as table_name,
                            'user_id' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN user_id >= 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders

-- Data type validation for amount
-- Validate amount has correct data type
SELECT 
                    'orders_amount_data_type_validation' as rule_id,
                    'orders' as table_name,
                    'amount' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN amount IS NULL OR CAST(amount AS double) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM orders

-- Business rule validation for amount
-- Validate business rule: amount >= 0
SELECT 
                            'orders_amount_business_rule_1' as rule_id,
                            'orders' as table_name,
                            'amount' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN amount >= 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders

-- Data type validation for order_date
-- Validate order_date has correct data type
SELECT 
                    'orders_order_date_data_type_validation' as rule_id,
                    'orders' as table_name,
                    'order_date' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_date IS NULL OR CAST(order_date AS timestamp) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM orders

-- Data type validation for status
-- Validate status has correct data type
SELECT 
                    'orders_status_data_type_validation' as rule_id,
                    'orders' as table_name,
                    'status' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN status IS NULL OR CAST(status AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM orders

-- Business rule validation for status
-- Validate business rule: LENGTH(status) >= 7
SELECT 
                            'orders_status_business_rule_1' as rule_id,
                            'orders' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(status) >= 7 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders

-- Business rule validation for status
-- Validate business rule: LENGTH(status) <= 9
SELECT 
                            'orders_status_business_rule_2' as rule_id,
                            'orders' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(status) <= 9 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders

-- Business rule validation for status
-- Validate business rule: status IN ('shipped', 'confirmed', 'cancelled', 'delivered', 'pending')
SELECT 
                            'orders_status_business_rule_3' as rule_id,
                            'orders' as table_name,
                            'status' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN status IN ('shipped', 'confirmed', 'cancelled', 'delivered', 'pending') THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM orders
