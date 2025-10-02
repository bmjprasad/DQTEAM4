
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


def validate_user_id_range(df):
    """Validate user_id is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['user_id'].isNotNull())
    
    # Apply range validation
    range_condition = col('user_id') >= 1
    range_condition = range_condition & (col('user_id') <= 1000)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_user_id_range_validation',
        'table_name': 'users',
        'column_name': 'user_id',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_email_format(df):
    """Validate email format using regex pattern"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Filter non-null values
    non_null_df = df.filter(df['email'].isNotNull())
    
    # Apply regex validation
    valid_mask = non_null_df['email'].rlike(pattern)
    
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_email_format_validation',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'accuracy',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_age_range(df):
    """Validate age is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['age'].isNotNull())
    
    # Apply range validation
    range_condition = col('age') >= 18.0
    range_condition = range_condition & (col('age') <= 79.0)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_age_range_validation',
        'table_name': 'users',
        'column_name': 'age',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_phone_format(df):
    """Validate phone format using regex pattern"""
    import re
    pattern = r'^\+?[\d\s\-\(\)]{10,}$'
    
    # Filter non-null values
    non_null_df = df.filter(df['phone'].isNotNull())
    
    # Apply regex validation
    valid_mask = non_null_df['phone'].rlike(pattern)
    
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_phone_format_validation',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'accuracy',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def check_user_id_completeness(df):
    """Check for null values in user_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('user_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_user_id_null_check',
        'table_name': 'users',
        'column_name': 'user_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_email_completeness(df):
    """Check for null values in email"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('email').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_email_null_check',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_email_empty_strings(df):
    """Check for empty strings in email"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('email') == '') | (trim(col('email')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_email_empty_string_check',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_age_completeness(df):
    """Check for null values in age"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('age').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_age_null_check',
        'table_name': 'users',
        'column_name': 'age',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_created_at_completeness(df):
    """Check for null values in created_at"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('created_at').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_created_at_null_check',
        'table_name': 'users',
        'column_name': 'created_at',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_status_completeness(df):
    """Check for null values in status"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('status').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_status_null_check',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_status_empty_strings(df):
    """Check for empty strings in status"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('status') == '') | (trim(col('status')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_status_empty_string_check',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_phone_completeness(df):
    """Check for null values in phone"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('phone').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_phone_null_check',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_phone_empty_strings(df):
    """Check for empty strings in phone"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('phone') == '') | (trim(col('phone')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'users_phone_empty_string_check',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_data_freshness(df):
    """Check data freshness"""
    from pyspark.sql.functions import current_timestamp, datediff, lit
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 16:32:57.989861', '%Y-%m-%d %H:%M:%S')
    expected_freshness_hours = 24
    
    # Calculate hours since last update
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    is_fresh = hours_since_update <= expected_freshness_hours
    fresh_rows = total_count if is_fresh else 0
    
    return {
        'rule_id': 'users_data_freshness',
        'table_name': 'users',
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
    
    last_updated = datetime.strptime('2025-10-02 16:32:57.989861', '%Y-%m-%d %H:%M:%S')
    sla_hours = 6
    
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    sla_met = hours_since_update <= sla_hours
    
    return {
        'rule_id': 'users_sla_compliance',
        'table_name': 'users',
        'column_name': 'sla_compliance',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'sla_met': sla_met,
        'hours_since_update': round(hours_since_update, 2)
    }
                


def validate_user_id_data_type(df):
    """Validate user_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import ShortType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('user_id').cast(ShortType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'users_user_id_data_type_validation',
        'table_name': 'users',
        'column_name': 'user_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_user_id_business_rule_1(df):
    """Validate business rule for user_id: user_id >= 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('user_id >= 0')).count()
    
    return {
        'rule_id': 'users_user_id_business_rule_1',
        'table_name': 'users',
        'column_name': 'user_id',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'user_id >= 0'
    }
                        


def validate_email_data_type(df):
    """Validate email has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('email').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'users_email_data_type_validation',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_email_business_rule_1(df):
    """Validate business rule for email: LENGTH(email) >= 17"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(email) >= 17')).count()
    
    return {
        'rule_id': 'users_email_business_rule_1',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(email) >= 17'
    }
                        


def validate_email_business_rule_2(df):
    """Validate business rule for email: LENGTH(email) <= 20"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(email) <= 20')).count()
    
    return {
        'rule_id': 'users_email_business_rule_2',
        'table_name': 'users',
        'column_name': 'email',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(email) <= 20'
    }
                        


def validate_age_data_type(df):
    """Validate age has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import DoubleType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('age').cast(DoubleType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'users_age_data_type_validation',
        'table_name': 'users',
        'column_name': 'age',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_age_business_rule_1(df):
    """Validate business rule for age: age >= 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('age >= 0')).count()
    
    return {
        'rule_id': 'users_age_business_rule_1',
        'table_name': 'users',
        'column_name': 'age',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'age >= 0'
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
        'rule_id': 'users_created_at_data_type_validation',
        'table_name': 'users',
        'column_name': 'created_at',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_status_data_type(df):
    """Validate status has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('status').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'users_status_data_type_validation',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_status_business_rule_1(df):
    """Validate business rule for status: LENGTH(status) >= 6"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(status) >= 6')).count()
    
    return {
        'rule_id': 'users_status_business_rule_1',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(status) >= 6'
    }
                        


def validate_status_business_rule_2(df):
    """Validate business rule for status: LENGTH(status) <= 8"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(status) <= 8')).count()
    
    return {
        'rule_id': 'users_status_business_rule_2',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(status) <= 8'
    }
                        


def validate_status_business_rule_3(df):
    """Validate business rule for status: status IN ('active', 'inactive', 'pending')"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('status IN ('active', 'inactive', 'pending')')).count()
    
    return {
        'rule_id': 'users_status_business_rule_3',
        'table_name': 'users',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'status IN ('active', 'inactive', 'pending')'
    }
                        


def validate_phone_data_type(df):
    """Validate phone has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('phone').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'users_phone_data_type_validation',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_phone_business_rule_1(df):
    """Validate business rule for phone: LENGTH(phone) >= 15"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(phone) >= 15')).count()
    
    return {
        'rule_id': 'users_phone_business_rule_1',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(phone) >= 15'
    }
                        


def validate_phone_business_rule_2(df):
    """Validate business rule for phone: LENGTH(phone) <= 15"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(phone) <= 15')).count()
    
    return {
        'rule_id': 'users_phone_business_rule_2',
        'table_name': 'users',
        'column_name': 'phone',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(phone) <= 15'
    }
                        


def validate_order_id_range(df):
    """Validate order_id is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['order_id'].isNotNull())
    
    # Apply range validation
    range_condition = col('order_id') >= 1
    range_condition = range_condition & (col('order_id') <= 5000)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_order_id_range_validation',
        'table_name': 'orders',
        'column_name': 'order_id',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_user_id_range(df):
    """Validate user_id is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['user_id'].isNotNull())
    
    # Apply range validation
    range_condition = col('user_id') >= 1
    range_condition = range_condition & (col('user_id') <= 99999)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_user_id_range_validation',
        'table_name': 'orders',
        'column_name': 'user_id',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def validate_amount_range(df):
    """Validate amount is within expected range"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['amount'].isNotNull())
    
    # Apply range validation
    range_condition = col('amount') >= 10.18
    range_condition = range_condition & (col('amount') <= 999.74)
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_amount_range_validation',
        'table_name': 'orders',
        'column_name': 'amount',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }
                    


def check_order_id_completeness(df):
    """Check for null values in order_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('order_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_order_id_null_check',
        'table_name': 'orders',
        'column_name': 'order_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_user_id_completeness(df):
    """Check for null values in user_id"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('user_id').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_user_id_null_check',
        'table_name': 'orders',
        'column_name': 'user_id',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_amount_completeness(df):
    """Check for null values in amount"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('amount').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_amount_null_check',
        'table_name': 'orders',
        'column_name': 'amount',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_order_date_completeness(df):
    """Check for null values in order_date"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('order_date').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_order_date_null_check',
        'table_name': 'orders',
        'column_name': 'order_date',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_status_completeness(df):
    """Check for null values in status"""
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('status').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_status_null_check',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'completeness',
        'severity': 'critical',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }
                


def check_status_empty_strings(df):
    """Check for empty strings in status"""
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('status') == '') | (trim(col('status')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {
        'rule_id': 'orders_status_empty_string_check',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }
                    


def check_data_freshness(df):
    """Check data freshness"""
    from pyspark.sql.functions import current_timestamp, datediff, lit
    from datetime import datetime
    
    last_updated = datetime.strptime('2025-10-02 16:32:58.011165', '%Y-%m-%d %H:%M:%S')
    expected_freshness_hours = 24
    
    # Calculate hours since last update
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    is_fresh = hours_since_update <= expected_freshness_hours
    fresh_rows = total_count if is_fresh else 0
    
    return {
        'rule_id': 'orders_data_freshness',
        'table_name': 'orders',
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
    
    last_updated = datetime.strptime('2025-10-02 16:32:58.011165', '%Y-%m-%d %H:%M:%S')
    sla_hours = 6
    
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    sla_met = hours_since_update <= sla_hours
    
    return {
        'rule_id': 'orders_sla_compliance',
        'table_name': 'orders',
        'column_name': 'sla_compliance',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'sla_met': sla_met,
        'hours_since_update': round(hours_since_update, 2)
    }
                


def validate_order_id_data_type(df):
    """Validate order_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import ShortType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('order_id').cast(ShortType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'orders_order_id_data_type_validation',
        'table_name': 'orders',
        'column_name': 'order_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_order_id_business_rule_1(df):
    """Validate business rule for order_id: order_id >= 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('order_id >= 0')).count()
    
    return {
        'rule_id': 'orders_order_id_business_rule_1',
        'table_name': 'orders',
        'column_name': 'order_id',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'order_id >= 0'
    }
                        


def validate_user_id_data_type(df):
    """Validate user_id has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import IntegerType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('user_id').cast(IntegerType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'orders_user_id_data_type_validation',
        'table_name': 'orders',
        'column_name': 'user_id',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_user_id_business_rule_1(df):
    """Validate business rule for user_id: user_id >= 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('user_id >= 0')).count()
    
    return {
        'rule_id': 'orders_user_id_business_rule_1',
        'table_name': 'orders',
        'column_name': 'user_id',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'user_id >= 0'
    }
                        


def validate_amount_data_type(df):
    """Validate amount has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import DoubleType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('amount').cast(DoubleType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'orders_amount_data_type_validation',
        'table_name': 'orders',
        'column_name': 'amount',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_amount_business_rule_1(df):
    """Validate business rule for amount: amount >= 0"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('amount >= 0')).count()
    
    return {
        'rule_id': 'orders_amount_business_rule_1',
        'table_name': 'orders',
        'column_name': 'amount',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'amount >= 0'
    }
                        


def validate_order_date_data_type(df):
    """Validate order_date has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import TimestampType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('order_date').cast(TimestampType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'orders_order_date_data_type_validation',
        'table_name': 'orders',
        'column_name': 'order_date',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_status_data_type(df):
    """Validate status has correct data type"""
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import StringType()
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('status').cast(StringType()).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {
        'rule_id': 'orders_status_data_type_validation',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }
                


def validate_status_business_rule_1(df):
    """Validate business rule for status: LENGTH(status) >= 7"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(status) >= 7')).count()
    
    return {
        'rule_id': 'orders_status_business_rule_1',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(status) >= 7'
    }
                        


def validate_status_business_rule_2(df):
    """Validate business rule for status: LENGTH(status) <= 9"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('LENGTH(status) <= 9')).count()
    
    return {
        'rule_id': 'orders_status_business_rule_2',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'LENGTH(status) <= 9'
    }
                        


def validate_status_business_rule_3(df):
    """Validate business rule for status: status IN ('shipped', 'confirmed', 'cancelled', 'delivered', 'pending')"""
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('status IN ('shipped', 'confirmed', 'cancelled', 'delivered', 'pending')')).count()
    
    return {
        'rule_id': 'orders_status_business_rule_3',
        'table_name': 'orders',
        'column_name': 'status',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': 'status IN ('shipped', 'confirmed', 'cancelled', 'delivered', 'pending')'
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
