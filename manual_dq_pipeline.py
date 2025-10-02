
"""
Data Quality Pipeline Integration

This module provides functions to execute data quality rules in your data pipeline.
Import and use these functions in your PySpark data processing workflows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityPipeline:
    """Data Quality Pipeline Integration Class"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.results = []
    
    def execute_rule(self, df, rule_func, *args, **kwargs):
        """Execute a data quality rule and store results"""
        try:
            result = rule_func(df, *args, **kwargs)
            self.results.append(result)
            logger.info(f"Rule {result['rule_id']} executed successfully")
            return result
        except Exception as e:
            logger.error(f"Error executing rule: {str(e)}")
            return None
    
    def get_all_results(self):
        """Get all data quality results"""
        return self.results
    
    def generate_report(self):
        """Generate a data quality report"""
        if not self.results:
            return "No data quality results available"
        
        report = "Data Quality Report\n" + "="*50 + "\n"
        
        for result in self.results:
            report += f"Rule ID: {result['rule_id']}\n"
            report += f"Table: {result['table_name']}\n"
            report += f"Column: {result.get('column_name', 'N/A')}\n"
            report += f"Dimension: {result['dimension']}\n"
            report += f"Severity: {result['severity']}\n"
            report += f"Total Rows: {result['total_rows']}\n"
            
            # Add dimension-specific metrics
            if result['dimension'] == 'accuracy':
                report += f"Valid Rows: {result.get('valid_rows', 'N/A')}\n"
                report += f"Accuracy: {result.get('accuracy_percentage', 'N/A')}%\n"
            elif result['dimension'] == 'completeness':
                report += f"Null Count: {result.get('null_count', 'N/A')}\n"
                report += f"Null Percentage: {result.get('null_percentage', 'N/A')}%\n"
            elif result['dimension'] == 'consistency':
                report += f"Unique Rows: {result.get('unique_rows', 'N/A')}\n"
                report += f"Is Unique: {result.get('is_unique', 'N/A')}\n"
            elif result['dimension'] == 'timeliness':
                report += f"Fresh Rows: {result.get('fresh_rows', 'N/A')}\n"
                report += f"Hours Since Update: {result.get('hours_since_update', 'N/A')}\n"
            elif result['dimension'] == 'validity':
                report += f"Valid Type Rows: {result.get('valid_type_rows', 'N/A')}\n"
                report += f"Valid Rule Rows: {result.get('valid_rule_rows', 'N/A')}\n"
            
            report += "\n" + "-"*30 + "\n"
        
        return report

# Data Quality Rule Functions


def validate_product_id_range(df):
    """Validate product_id is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['product_id'].isNotNull())
    
    # Apply range validation
    range_condition = col('product_id') >= 1
    range_condition = range_condition & (col('product_id') <= 1000000)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_product_id_range_validation',
        'table_name': 'products',
        'column_name': 'product_id',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_price_range(df):
    """Validate price is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['price'].isNotNull())
    
    # Apply range validation
    range_condition = col('price') >= 0.01
    range_condition = range_condition & (col('price') <= 10000.0)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_price_range_validation',
        'table_name': 'products',
        'column_name': 'price',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_sku_format(df):
    """Validate sku format using regex pattern"""
    import re
    pattern = r'^[A-Z]{2,3}-\d{4,6}$'
    
    # Filter non-null values
    non_null_df = df.filter(df['sku'].isNotNull())
    
    # Apply regex validation
    valid_mask = non_null_df['sku'].rlike(pattern)
    
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_sku_format_validation',
        'table_name': 'products',
        'column_name': 'sku',
        'dimension': 'accuracy',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def check_product_id_completeness(df):
    """Check for null values in product_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('product_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_product_id_null_check',
        'table_name': 'products',
        'column_name': 'product_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_product_name_completeness(df):
    """Check for null values in product_name"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('product_name').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_product_name_null_check',
        'table_name': 'products',
        'column_name': 'product_name',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_product_name_empty_strings(df):
    """Check for empty strings in product_name"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('product_name') == '') | (trim(col('product_name')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_product_name_empty_string_check',
        'table_name': 'products',
        'column_name': 'product_name',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_price_completeness(df):
    """Check for null values in price"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('price').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_price_null_check',
        'table_name': 'products',
        'column_name': 'price',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_category_completeness(df):
    """Check for null values in category"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('category').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_category_null_check',
        'table_name': 'products',
        'column_name': 'category',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_category_empty_strings(df):
    """Check for empty strings in category"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('category') == '') | (trim(col('category')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_category_empty_string_check',
        'table_name': 'products',
        'column_name': 'category',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_sku_completeness(df):
    """Check for null values in sku"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('sku').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_sku_null_check',
        'table_name': 'products',
        'column_name': 'sku',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_sku_empty_strings(df):
    """Check for empty strings in sku"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('sku') == '') | (trim(col('sku')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_sku_empty_string_check',
        'table_name': 'products',
        'column_name': 'sku',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_created_at_completeness(df):
    """Check for null values in created_at"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('created_at').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_created_at_null_check',
        'table_name': 'products',
        'column_name': 'created_at',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_is_active_completeness(df):
    """Check for null values in is_active"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('is_active').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'products_is_active_null_check',
        'table_name': 'products',
        'column_name': 'is_active',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_primary_key_uniqueness(df):
    """Check primary key uniqueness"""
    pk_cols = ['product_id']
    
    total_count = df.count()
    unique_count = df.select(*pk_cols).distinct().count()
    is_unique = total_count == unique_count
    
    return {
        'rule_id': 'products_primary_key_uniqueness',
        'table_name': 'products',
        'column_name': 'primary_key',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'unique_rows': unique_count,
        'is_unique': is_unique
    }
                


def check_data_freshness(df):
    """Check data freshness"""
    from pyspark.sql.functions import current_timestamp, datediff, lit
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 15:32:58.012337', '%Y-%m-%d %H:%M:%S')
    expected_freshness_hours = 12
    
    # Calculate hours since last update
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    is_fresh = hours_since_update <= expected_freshness_hours
    fresh_rows = total_count if is_fresh else 0
    
    return {
        'rule_id': 'products_data_freshness',
        'table_name': 'products',
        'column_name': 'data_freshness',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'fresh_rows': fresh_rows,
        'hours_since_update': round(hours_since_update, 2),
        'is_fresh': is_fresh
    }
                


def check_sla_compliance(df):
    """Check SLA compliance"""
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 15:32:58.012337', '%Y-%m-%d %H:%M:%S')
    sla_hours = 2
    
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    sla_met = hours_since_update <= sla_hours
    
    return {
        'rule_id': 'products_sla_compliance',
        'table_name': 'products',
        'column_name': 'sla_compliance',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'sla_met': sla_met,
        'hours_since_update': round(hours_since_update, 2)
    }
                


def validate_product_id_data_type(df):
    """Validate product_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('product_id').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_product_id_data_type_validation',
        'table_name': 'products',
        'column_name': 'product_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_product_name_data_type(df):
    """Validate product_name has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('product_name').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_product_name_data_type_validation',
        'table_name': 'products',
        'column_name': 'product_name',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_product_name_business_rule_1(df):
    """Validate business rule for product_name: LENGTH(product_name) >= 3"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(product_name) >= 3')).count()
    
    return {
        'rule_id': 'products_product_name_business_rule_1',
        'table_name': 'products',
        'column_name': 'product_name',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(product_name) >= 3'
    }
                        


def validate_product_name_business_rule_2(df):
    """Validate business rule for product_name: LENGTH(product_name) <= 100"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(product_name) <= 100')).count()
    
    return {
        'rule_id': 'products_product_name_business_rule_2',
        'table_name': 'products',
        'column_name': 'product_name',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(product_name) <= 100'
    }
                        


def validate_price_data_type(df):
    """Validate price has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import DecimalType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('price').cast(DecimalType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_price_data_type_validation',
        'table_name': 'products',
        'column_name': 'price',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_price_business_rule_1(df):
    """Validate business rule for price: price > 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('price > 0')).count()
    
    return {
        'rule_id': 'products_price_business_rule_1',
        'table_name': 'products',
        'column_name': 'price',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'price > 0'
    }
                        


def validate_category_data_type(df):
    """Validate category has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('category').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_category_data_type_validation',
        'table_name': 'products',
        'column_name': 'category',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_sku_data_type(df):
    """Validate sku has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('sku').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_sku_data_type_validation',
        'table_name': 'products',
        'column_name': 'sku',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_created_at_data_type(df):
    """Validate created_at has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import TimestampType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('created_at').cast(TimestampType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_created_at_data_type_validation',
        'table_name': 'products',
        'column_name': 'created_at',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_is_active_data_type(df):
    """Validate is_active has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import BooleanType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('is_active').cast(BooleanType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'products_is_active_data_type_validation',
        'table_name': 'products',
        'column_name': 'is_active',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_quantity_range(df):
    """Validate quantity is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['quantity'].isNotNull())
    
    # Apply range validation
    range_condition = col('quantity') >= 1
    range_condition = range_condition & (col('quantity') <= 100)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_quantity_range_validation',
        'table_name': 'order_items',
        'column_name': 'quantity',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_unit_price_range(df):
    """Validate unit_price is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['unit_price'].isNotNull())
    
    # Apply range validation
    range_condition = col('unit_price') >= 0.01
    range_condition = range_condition & (col('unit_price') <= 10000.0)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_unit_price_range_validation',
        'table_name': 'order_items',
        'column_name': 'unit_price',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def check_order_item_id_completeness(df):
    """Check for null values in order_item_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('order_item_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_order_item_id_null_check',
        'table_name': 'order_items',
        'column_name': 'order_item_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_order_id_completeness(df):
    """Check for null values in order_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('order_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_order_id_null_check',
        'table_name': 'order_items',
        'column_name': 'order_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_product_id_completeness(df):
    """Check for null values in product_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('product_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_product_id_null_check',
        'table_name': 'order_items',
        'column_name': 'product_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_quantity_completeness(df):
    """Check for null values in quantity"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('quantity').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_quantity_null_check',
        'table_name': 'order_items',
        'column_name': 'quantity',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_unit_price_completeness(df):
    """Check for null values in unit_price"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('unit_price').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_unit_price_null_check',
        'table_name': 'order_items',
        'column_name': 'unit_price',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_total_price_completeness(df):
    """Check for null values in total_price"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('total_price').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'order_items_total_price_null_check',
        'table_name': 'order_items',
        'column_name': 'total_price',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_primary_key_uniqueness(df):
    """Check primary key uniqueness"""
    pk_cols = ['order_item_id']
    
    total_count = df.count()
    unique_count = df.select(*pk_cols).distinct().count()
    is_unique = total_count == unique_count
    
    return {
        'rule_id': 'order_items_primary_key_uniqueness',
        'table_name': 'order_items',
        'column_name': 'primary_key',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'unique_rows': unique_count,
        'is_unique': is_unique
    }
                


def check_foreign_key_integrity(df, referenced_df):
    """Check foreign key referential integrity"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Get valid foreign key values from referenced table
    valid_fk_values = referenced_df.select('order_id').distinct().rdd.map(lambda x: x[0]).collect()
    
    total_count = df.count()
    null_fk_count = df.filter(col('order_id').isNull()).count()
    invalid_fk_count = df.filter(
        col('order_id').isNotNull() & 
        ~col('order_id').isin(valid_fk_values)
    ).count()
    
    return {
        'rule_id': 'order_items_order_id_foreign_key_integrity',
        'table_name': 'order_items',
        'column_name': 'order_id',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'null_fk_count': null_fk_count,
        'invalid_fk_count': invalid_fk_count
    }
                    


def check_foreign_key_integrity(df, referenced_df):
    """Check foreign key referential integrity"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Get valid foreign key values from referenced table
    valid_fk_values = referenced_df.select('product_id').distinct().rdd.map(lambda x: x[0]).collect()
    
    total_count = df.count()
    null_fk_count = df.filter(col('product_id').isNull()).count()
    invalid_fk_count = df.filter(
        col('product_id').isNotNull() & 
        ~col('product_id').isin(valid_fk_values)
    ).count()
    
    return {
        'rule_id': 'order_items_product_id_foreign_key_integrity',
        'table_name': 'order_items',
        'column_name': 'product_id',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'null_fk_count': null_fk_count,
        'invalid_fk_count': invalid_fk_count
    }
                    


def check_data_freshness(df):
    """Check data freshness"""
    from pyspark.sql.functions import current_timestamp, datediff, lit
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 16:02:58.012371', '%Y-%m-%d %H:%M:%S')
    expected_freshness_hours = 6
    
    # Calculate hours since last update
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    is_fresh = hours_since_update <= expected_freshness_hours
    fresh_rows = total_count if is_fresh else 0
    
    return {
        'rule_id': 'order_items_data_freshness',
        'table_name': 'order_items',
        'column_name': 'data_freshness',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'fresh_rows': fresh_rows,
        'hours_since_update': round(hours_since_update, 2),
        'is_fresh': is_fresh
    }
                


def check_sla_compliance(df):
    """Check SLA compliance"""
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 16:02:58.012371', '%Y-%m-%d %H:%M:%S')
    sla_hours = 1
    
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    sla_met = hours_since_update <= sla_hours
    
    return {
        'rule_id': 'order_items_sla_compliance',
        'table_name': 'order_items',
        'column_name': 'sla_compliance',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'sla_met': sla_met,
        'hours_since_update': round(hours_since_update, 2)
    }
                


def validate_order_item_id_data_type(df):
    """Validate order_item_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('order_item_id').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_order_item_id_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'order_item_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_order_id_data_type(df):
    """Validate order_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('order_id').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_order_id_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'order_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_product_id_data_type(df):
    """Validate product_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('product_id').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_product_id_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'product_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_quantity_data_type(df):
    """Validate quantity has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('quantity').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_quantity_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'quantity',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_quantity_business_rule_1(df):
    """Validate business rule for quantity: quantity > 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('quantity > 0')).count()
    
    return {
        'rule_id': 'order_items_quantity_business_rule_1',
        'table_name': 'order_items',
        'column_name': 'quantity',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'quantity > 0'
    }
                        


def validate_unit_price_data_type(df):
    """Validate unit_price has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import DecimalType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('unit_price').cast(DecimalType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_unit_price_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'unit_price',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_total_price_data_type(df):
    """Validate total_price has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import DecimalType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('total_price').cast(DecimalType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'order_items_total_price_data_type_validation',
        'table_name': 'order_items',
        'column_name': 'total_price',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_total_price_business_rule_1(df):
    """Validate business rule for total_price: total_price = quantity * unit_price"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('total_price = quantity * unit_price')).count()
    
    return {
        'rule_id': 'order_items_total_price_business_rule_1',
        'table_name': 'order_items',
        'column_name': 'total_price',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'total_price = quantity * unit_price'
    }
                        

# Example usage:
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()
    
    # Load your data
    df = spark.read.table("your_table_name")
    
    # Initialize data quality pipeline
    dq_pipeline = DataQualityPipeline(spark)
    
    # Execute data quality rules
    # Example: dq_pipeline.execute_rule(df, validate_column_name_format)
    
    # Generate report
    report = dq_pipeline.generate_report()
    print(report)
    
    spark.stop()
