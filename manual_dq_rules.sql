-- Range validation for product_id
-- Validate product_id is within expected range
SELECT 
                        'products_product_id_range_validation' as rule_id,
                        'products' as table_name,
                        'product_id' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN product_id IS NOT NULL AND (product_id >= 1 AND product_id <= 1000000) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN product_id IS NOT NULL AND (product_id >= 1 AND product_id <= 1000000) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM products

-- Range validation for price
-- Validate price is within expected range
SELECT 
                        'products_price_range_validation' as rule_id,
                        'products' as table_name,
                        'price' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN price IS NOT NULL AND (price >= 0.01 AND price <= 10000.0) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN price IS NOT NULL AND (price >= 0.01 AND price <= 10000.0) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM products

-- Format validation for sku
-- Validate sku matches expected format pattern
SELECT 
                        'products_sku_format_validation' as rule_id,
                        'products' as table_name,
                        'sku' as column_name,
                        'accuracy' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN sku IS NOT NULL AND sku RLIKE '^[A-Z]{2,3}-\d{4,6}$' THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN sku IS NOT NULL AND sku RLIKE '^[A-Z]{2,3}-\d{4,6}$' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM products

-- Null value check for product_id
-- Check for null values in product_id
SELECT 
                    'products_product_id_null_check' as rule_id,
                    'products' as table_name,
                    'product_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Null value check for product_name
-- Check for null values in product_name
SELECT 
                    'products_product_name_null_check' as rule_id,
                    'products' as table_name,
                    'product_name' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Empty string check for product_name
-- Check for empty strings in product_name
SELECT 
                        'products_product_name_empty_string_check' as rule_id,
                        'products' as table_name,
                        'product_name' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN product_name = '' OR TRIM(product_name) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN product_name = '' OR TRIM(product_name) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM products

-- Null value check for price
-- Check for null values in price
SELECT 
                    'products_price_null_check' as rule_id,
                    'products' as table_name,
                    'price' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Null value check for category
-- Check for null values in category
SELECT 
                    'products_category_null_check' as rule_id,
                    'products' as table_name,
                    'category' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Empty string check for category
-- Check for empty strings in category
SELECT 
                        'products_category_empty_string_check' as rule_id,
                        'products' as table_name,
                        'category' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN category = '' OR TRIM(category) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN category = '' OR TRIM(category) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM products

-- Null value check for sku
-- Check for null values in sku
SELECT 
                    'products_sku_null_check' as rule_id,
                    'products' as table_name,
                    'sku' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN sku IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN sku IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Empty string check for sku
-- Check for empty strings in sku
SELECT 
                        'products_sku_empty_string_check' as rule_id,
                        'products' as table_name,
                        'sku' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN sku = '' OR TRIM(sku) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN sku = '' OR TRIM(sku) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM products

-- Null value check for created_at
-- Check for null values in created_at
SELECT 
                    'products_created_at_null_check' as rule_id,
                    'products' as table_name,
                    'created_at' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Null value check for is_active
-- Check for null values in is_active
SELECT 
                    'products_is_active_null_check' as rule_id,
                    'products' as table_name,
                    'is_active' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM products

-- Primary key uniqueness for product_id
-- Ensure primary key columns are unique
SELECT 
                    'products_primary_key_uniqueness' as rule_id,
                    'products' as table_name,
                    'primary_key' as column_name,
                    'consistency' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT product_id) as unique_rows,
                    CASE WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 1 ELSE 0 END as is_unique
                FROM products

-- Data freshness check for products
-- Check if data is fresh within 12 hours
SELECT 
                    'products_data_freshness' as rule_id,
                    'products' as table_name,
                    'data_freshness' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 15:32:58.012337') <= 12 THEN 1 ELSE 0 END) as fresh_rows,
                    DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 15:32:58.012337') as hours_since_update
                FROM products

-- SLA compliance check for products
-- Check if data meets SLA of 2 hours
SELECT 
                    'products_sla_compliance' as rule_id,
                    'products' as table_name,
                    'sla_compliance' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 15:32:58.012337') <= 2 THEN 1 ELSE 0 END as sla_met
                FROM products

-- Data type validation for product_id
-- Validate product_id has correct data type
SELECT 
                    'products_product_id_data_type_validation' as rule_id,
                    'products' as table_name,
                    'product_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_id IS NULL OR CAST(product_id AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Data type validation for product_name
-- Validate product_name has correct data type
SELECT 
                    'products_product_name_data_type_validation' as rule_id,
                    'products' as table_name,
                    'product_name' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_name IS NULL OR CAST(product_name AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Business rule validation for product_name
-- Validate business rule: LENGTH(product_name) >= 3
SELECT 
                            'products_product_name_business_rule_1' as rule_id,
                            'products' as table_name,
                            'product_name' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(product_name) >= 3 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM products

-- Business rule validation for product_name
-- Validate business rule: LENGTH(product_name) <= 100
SELECT 
                            'products_product_name_business_rule_2' as rule_id,
                            'products' as table_name,
                            'product_name' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN LENGTH(product_name) <= 100 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM products

-- Data type validation for price
-- Validate price has correct data type
SELECT 
                    'products_price_data_type_validation' as rule_id,
                    'products' as table_name,
                    'price' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN price IS NULL OR CAST(price AS decimal) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Business rule validation for price
-- Validate business rule: price > 0
SELECT 
                            'products_price_business_rule_1' as rule_id,
                            'products' as table_name,
                            'price' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM products

-- Data type validation for category
-- Validate category has correct data type
SELECT 
                    'products_category_data_type_validation' as rule_id,
                    'products' as table_name,
                    'category' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN category IS NULL OR CAST(category AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Data type validation for sku
-- Validate sku has correct data type
SELECT 
                    'products_sku_data_type_validation' as rule_id,
                    'products' as table_name,
                    'sku' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN sku IS NULL OR CAST(sku AS string) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Data type validation for created_at
-- Validate created_at has correct data type
SELECT 
                    'products_created_at_data_type_validation' as rule_id,
                    'products' as table_name,
                    'created_at' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN created_at IS NULL OR CAST(created_at AS timestamp) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Data type validation for is_active
-- Validate is_active has correct data type
SELECT 
                    'products_is_active_data_type_validation' as rule_id,
                    'products' as table_name,
                    'is_active' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN is_active IS NULL OR CAST(is_active AS boolean) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM products

-- Range validation for quantity
-- Validate quantity is within expected range
SELECT 
                        'order_items_quantity_range_validation' as rule_id,
                        'order_items' as table_name,
                        'quantity' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN quantity IS NOT NULL AND (quantity >= 1 AND quantity <= 100) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN quantity IS NOT NULL AND (quantity >= 1 AND quantity <= 100) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM order_items

-- Range validation for unit_price
-- Validate unit_price is within expected range
SELECT 
                        'order_items_unit_price_range_validation' as rule_id,
                        'order_items' as table_name,
                        'unit_price' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN unit_price IS NOT NULL AND (unit_price >= 0.01 AND unit_price <= 10000.0) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN unit_price IS NOT NULL AND (unit_price >= 0.01 AND unit_price <= 10000.0) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM order_items

-- Null value check for order_item_id
-- Check for null values in order_item_id
SELECT 
                    'order_items_order_item_id_null_check' as rule_id,
                    'order_items' as table_name,
                    'order_item_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Null value check for order_id
-- Check for null values in order_id
SELECT 
                    'order_items_order_id_null_check' as rule_id,
                    'order_items' as table_name,
                    'order_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Null value check for product_id
-- Check for null values in product_id
SELECT 
                    'order_items_product_id_null_check' as rule_id,
                    'order_items' as table_name,
                    'product_id' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Null value check for quantity
-- Check for null values in quantity
SELECT 
                    'order_items_quantity_null_check' as rule_id,
                    'order_items' as table_name,
                    'quantity' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Null value check for unit_price
-- Check for null values in unit_price
SELECT 
                    'order_items_unit_price_null_check' as rule_id,
                    'order_items' as table_name,
                    'unit_price' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Null value check for total_price
-- Check for null values in total_price
SELECT 
                    'order_items_total_price_null_check' as rule_id,
                    'order_items' as table_name,
                    'total_price' as column_name,
                    'completeness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN total_price IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN total_price IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM order_items

-- Primary key uniqueness for order_item_id
-- Ensure primary key columns are unique
SELECT 
                    'order_items_primary_key_uniqueness' as rule_id,
                    'order_items' as table_name,
                    'primary_key' as column_name,
                    'consistency' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT order_item_id) as unique_rows,
                    CASE WHEN COUNT(*) = COUNT(DISTINCT order_item_id) THEN 1 ELSE 0 END as is_unique
                FROM order_items

-- Foreign key integrity for order_id
-- Validate foreign key references to orders.order_id
SELECT 
                        'order_items_order_id_foreign_key_integrity' as rule_id,
                        'order_items' as table_name,
                        'order_id' as column_name,
                        'consistency' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_fk_count,
                        SUM(CASE WHEN order_id IS NOT NULL AND order_id NOT IN (
                            SELECT DISTINCT order_id 
                            FROM orders 
                            WHERE order_id IS NOT NULL
                        ) THEN 1 ELSE 0 END) as invalid_fk_count
                    FROM order_items

-- Foreign key integrity for product_id
-- Validate foreign key references to products.product_id
SELECT 
                        'order_items_product_id_foreign_key_integrity' as rule_id,
                        'order_items' as table_name,
                        'product_id' as column_name,
                        'consistency' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_fk_count,
                        SUM(CASE WHEN product_id IS NOT NULL AND product_id NOT IN (
                            SELECT DISTINCT product_id 
                            FROM products 
                            WHERE product_id IS NOT NULL
                        ) THEN 1 ELSE 0 END) as invalid_fk_count
                    FROM order_items

-- Data freshness check for order_items
-- Check if data is fresh within 6 hours
SELECT 
                    'order_items_data_freshness' as rule_id,
                    'order_items' as table_name,
                    'data_freshness' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:02:58.012371') <= 6 THEN 1 ELSE 0 END) as fresh_rows,
                    DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:02:58.012371') as hours_since_update
                FROM order_items

-- SLA compliance check for order_items
-- Check if data meets SLA of 1 hours
SELECT 
                    'order_items_sla_compliance' as rule_id,
                    'order_items' as table_name,
                    'sla_compliance' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '2025-10-02 16:02:58.012371') <= 1 THEN 1 ELSE 0 END as sla_met
                FROM order_items

-- Data type validation for order_item_id
-- Validate order_item_id has correct data type
SELECT 
                    'order_items_order_item_id_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'order_item_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_item_id IS NULL OR CAST(order_item_id AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Data type validation for order_id
-- Validate order_id has correct data type
SELECT 
                    'order_items_order_id_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'order_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN order_id IS NULL OR CAST(order_id AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Data type validation for product_id
-- Validate product_id has correct data type
SELECT 
                    'order_items_product_id_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'product_id' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN product_id IS NULL OR CAST(product_id AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Data type validation for quantity
-- Validate quantity has correct data type
SELECT 
                    'order_items_quantity_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'quantity' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN quantity IS NULL OR CAST(quantity AS int) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Business rule validation for quantity
-- Validate business rule: quantity > 0
SELECT 
                            'order_items_quantity_business_rule_1' as rule_id,
                            'order_items' as table_name,
                            'quantity' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN quantity > 0 THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM order_items

-- Data type validation for unit_price
-- Validate unit_price has correct data type
SELECT 
                    'order_items_unit_price_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'unit_price' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN unit_price IS NULL OR CAST(unit_price AS decimal) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Data type validation for total_price
-- Validate total_price has correct data type
SELECT 
                    'order_items_total_price_data_type_validation' as rule_id,
                    'order_items' as table_name,
                    'total_price' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN total_price IS NULL OR CAST(total_price AS decimal) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM order_items

-- Business rule validation for total_price
-- Validate business rule: total_price = quantity * unit_price
SELECT 
                            'order_items_total_price_business_rule_1' as rule_id,
                            'order_items' as table_name,
                            'total_price' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN total_price = quantity * unit_price THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM order_items
