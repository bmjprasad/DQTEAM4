"""
Auto Data Quality Rules Generator

This module generates data quality rules covering five key dimensions:
- Accuracy: Data correctness and format validation
- Completeness: Missing data detection
- Consistency: Cross-table and referential integrity validation
- Timeliness: Data freshness and SLA validation
- Validity: Business rule and data type validation

Generates both Spark SQL commands and Python pipeline integration code.
"""

import json
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import pandas as pd


@dataclass
class ColumnMetadata:
    """Metadata for a table column"""
    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    unique_values: Optional[List[Any]] = None
    null_count: int = 0
    total_count: int = 0
    pattern: Optional[str] = None  # Regex pattern for validation
    business_rules: Optional[List[str]] = None


@dataclass
class TableMetadata:
    """Metadata for a table"""
    name: str
    columns: List[ColumnMetadata]
    row_count: int
    last_updated: Optional[datetime] = None
    expected_freshness_hours: Optional[int] = None
    sla_hours: Optional[int] = None


@dataclass
class DataQualityRule:
    """A data quality rule definition"""
    rule_id: str
    rule_name: str
    dimension: str  # accuracy, completeness, consistency, timeliness, validity
    severity: str  # critical, warning, info
    description: str
    spark_sql: str
    python_code: str
    threshold: Optional[float] = None
    column_name: Optional[str] = None


class DataQualityRuleGenerator:
    """Main class for generating data quality rules"""
    
    def __init__(self):
        self.rules: List[DataQualityRule] = []
        self.table_metadata: Dict[str, TableMetadata] = {}
    
    def add_table_metadata(self, table_metadata: TableMetadata):
        """Add table metadata for rule generation"""
        self.table_metadata[table_metadata.name] = table_metadata
    
    def generate_all_rules(self) -> List[DataQualityRule]:
        """Generate all data quality rules for all tables"""
        self.rules = []
        
        for table_name, table_meta in self.table_metadata.items():
            # Generate rules for each dimension
            self._generate_accuracy_rules(table_meta)
            self._generate_completeness_rules(table_meta)
            self._generate_consistency_rules(table_meta)
            self._generate_timeliness_rules(table_meta)
            self._generate_validity_rules(table_meta)
        
        return self.rules
    
    def _generate_accuracy_rules(self, table_meta: TableMetadata):
        """Generate accuracy rules for data correctness and format validation"""
        table_name = table_meta.name
        
        for column in table_meta.columns:
            # Format validation rules
            if column.pattern:
                rule_id = f"{table_name}_{column.name}_format_validation"
                self.rules.append(DataQualityRule(
                    rule_id=rule_id,
                    rule_name=f"Format validation for {column.name}",
                    dimension="accuracy",
                    severity="critical",
                    description=f"Validate {column.name} matches expected format pattern",
                    spark_sql=f"""
                    SELECT 
                        '{rule_id}' as rule_id,
                        '{table_name}' as table_name,
                        '{column.name}' as column_name,
                        'accuracy' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {column.name} IS NOT NULL AND {column.name} RLIKE '{column.pattern}' THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN {column.name} IS NOT NULL AND {column.name} RLIKE '{column.pattern}' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM {table_name}
                    """,
                    python_code=f"""
def validate_{column.name}_format(df):
    \"\"\"Validate {column.name} format using regex pattern\"\"\"
    import re
    pattern = r'{column.pattern}'
    
    # Filter non-null values
    non_null_df = df.filter(df['{column.name}'].isNotNull())
    
    # Apply regex validation
    valid_mask = non_null_df['{column.name}'].rlike(pattern)
    
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'accuracy',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }}
                    """,
                    column_name=column.name
                ))
            
            # Range validation rules
            if column.min_value is not None or column.max_value is not None:
                rule_id = f"{table_name}_{column.name}_range_validation"
                range_condition = []
                if column.min_value is not None:
                    range_condition.append(f"{column.name} >= {column.min_value}")
                if column.max_value is not None:
                    range_condition.append(f"{column.name} <= {column.max_value}")
                
                range_sql = " AND ".join(range_condition)
                
                self.rules.append(DataQualityRule(
                    rule_id=rule_id,
                    rule_name=f"Range validation for {column.name}",
                    dimension="accuracy",
                    severity="warning",
                    description=f"Validate {column.name} is within expected range",
                    spark_sql=f"""
                    SELECT 
                        '{rule_id}' as rule_id,
                        '{table_name}' as table_name,
                        '{column.name}' as column_name,
                        'accuracy' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {column.name} IS NOT NULL AND ({range_sql}) THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN {column.name} IS NOT NULL AND ({range_sql}) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_percentage
                    FROM {table_name}
                    """,
                    python_code=f"""
def validate_{column.name}_range(df):
    \"\"\"Validate {column.name} is within expected range\"\"\"
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Filter non-null values
    non_null_df = df.filter(df['{column.name}'].isNotNull())
    
    # Apply range validation
    range_condition = col('{column.name}') >= {column.min_value if column.min_value is not None else 'float("-inf")'}
    {f"range_condition = range_condition & (col('{column.name}') <= {column.max_value})" if column.max_value is not None else ""}
    
    valid_mask = range_condition
    total_count = non_null_df.count()
    valid_count = non_null_df.filter(valid_mask).count()
    accuracy_percentage = (valid_count / total_count * 100) if total_count > 0 else 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'accuracy',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rows': valid_count,
        'accuracy_percentage': round(accuracy_percentage, 2)
    }}
                    """,
                    column_name=column.name
                ))
    
    def _generate_completeness_rules(self, table_meta: TableMetadata):
        """Generate completeness rules for missing data detection"""
        table_name = table_meta.name
        
        for column in table_meta.columns:
            # Null value checks
            rule_id = f"{table_name}_{column.name}_null_check"
            self.rules.append(DataQualityRule(
                rule_id=rule_id,
                rule_name=f"Null value check for {column.name}",
                dimension="completeness",
                severity="critical" if not column.nullable else "warning",
                description=f"Check for null values in {column.name}",
                spark_sql=f"""
                SELECT 
                    '{rule_id}' as rule_id,
                    '{table_name}' as table_name,
                    '{column.name}' as column_name,
                    'completeness' as dimension,
                    '{'critical' if not column.nullable else 'warning'}' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN {column.name} IS NULL THEN 1 ELSE 0 END) as null_count,
                    ROUND(SUM(CASE WHEN {column.name} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
                FROM {table_name}
                """,
                python_code=f"""
def check_{column.name}_completeness(df):
    \"\"\"Check for null values in {column.name}\"\"\"
    from pyspark.sql.functions import col, when, isnan, isnull
    
    total_count = df.count()
    null_count = df.filter(col('{column.name}').isNull()).count()
    null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'completeness',
        'severity': '{'critical' if not column.nullable else 'warning'}',
        'total_rows': total_count,
        'null_count': null_count,
        'null_percentage': round(null_percentage, 2)
    }}
                """,
                column_name=column.name
            ))
            
            # Empty string checks for string columns
            if column.data_type.lower() in ['string', 'varchar', 'text']:
                rule_id = f"{table_name}_{column.name}_empty_string_check"
                self.rules.append(DataQualityRule(
                    rule_id=rule_id,
                    rule_name=f"Empty string check for {column.name}",
                    dimension="completeness",
                    severity="warning",
                    description=f"Check for empty strings in {column.name}",
                    spark_sql=f"""
                    SELECT 
                        '{rule_id}' as rule_id,
                        '{table_name}' as table_name,
                        '{column.name}' as column_name,
                        'completeness' as dimension,
                        'warning' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {column.name} = '' OR TRIM({column.name}) = '' THEN 1 ELSE 0 END) as empty_count,
                        ROUND(SUM(CASE WHEN {column.name} = '' OR TRIM({column.name}) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as empty_percentage
                    FROM {table_name}
                    """,
                    python_code=f"""
def check_{column.name}_empty_strings(df):
    \"\"\"Check for empty strings in {column.name}\"\"\"
    from pyspark.sql.functions import col, trim, when
    
    total_count = df.count()
    empty_count = df.filter((col('{column.name}') == '') | (trim(col('{column.name}')) == '')).count()
    empty_percentage = (empty_count / total_count * 100) if total_count > 0 else 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'completeness',
        'severity': 'warning',
        'total_rows': total_count,
        'empty_count': empty_count,
        'empty_percentage': round(empty_percentage, 2)
    }}
                    """,
                    column_name=column.name
                ))
    
    def _generate_consistency_rules(self, table_meta: TableMetadata):
        """Generate consistency rules for cross-table validation"""
        table_name = table_meta.name
        
        # Primary key uniqueness
        pk_columns = [col for col in table_meta.columns if col.is_primary_key]
        if pk_columns:
            pk_cols = [col.name for col in pk_columns]
            rule_id = f"{table_name}_primary_key_uniqueness"
            self.rules.append(DataQualityRule(
                rule_id=rule_id,
                rule_name=f"Primary key uniqueness for {', '.join(pk_cols)}",
                dimension="consistency",
                severity="critical",
                description=f"Ensure primary key columns are unique",
                spark_sql=f"""
                SELECT 
                    '{rule_id}' as rule_id,
                    '{table_name}' as table_name,
                    'primary_key' as column_name,
                    'consistency' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {', '.join(pk_cols)}) as unique_rows,
                    CASE WHEN COUNT(*) = COUNT(DISTINCT {', '.join(pk_cols)}) THEN 1 ELSE 0 END as is_unique
                FROM {table_name}
                """,
                python_code=f"""
def check_primary_key_uniqueness(df):
    \"\"\"Check primary key uniqueness\"\"\"
    pk_cols = {pk_cols}
    
    total_count = df.count()
    unique_count = df.select(*pk_cols).distinct().count()
    is_unique = total_count == unique_count
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': 'primary_key',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'unique_rows': unique_count,
        'is_unique': is_unique
    }}
                """
            ))
        
        # Foreign key referential integrity
        for column in table_meta.columns:
            if column.is_foreign_key and column.referenced_table and column.referenced_column:
                rule_id = f"{table_name}_{column.name}_foreign_key_integrity"
                self.rules.append(DataQualityRule(
                    rule_id=rule_id,
                    rule_name=f"Foreign key integrity for {column.name}",
                    dimension="consistency",
                    severity="critical",
                    description=f"Validate foreign key references to {column.referenced_table}.{column.referenced_column}",
                    spark_sql=f"""
                    SELECT 
                        '{rule_id}' as rule_id,
                        '{table_name}' as table_name,
                        '{column.name}' as column_name,
                        'consistency' as dimension,
                        'critical' as severity,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {column.name} IS NULL THEN 1 ELSE 0 END) as null_fk_count,
                        SUM(CASE WHEN {column.name} IS NOT NULL AND {column.name} NOT IN (
                            SELECT DISTINCT {column.referenced_column} 
                            FROM {column.referenced_table} 
                            WHERE {column.referenced_column} IS NOT NULL
                        ) THEN 1 ELSE 0 END) as invalid_fk_count
                    FROM {table_name}
                    """,
                    python_code=f"""
def check_foreign_key_integrity(df, referenced_df):
    \"\"\"Check foreign key referential integrity\"\"\"
    from pyspark.sql.functions import col, when, isnan, isnull
    
    # Get valid foreign key values from referenced table
    valid_fk_values = referenced_df.select('{column.referenced_column}').distinct().rdd.map(lambda x: x[0]).collect()
    
    total_count = df.count()
    null_fk_count = df.filter(col('{column.name}').isNull()).count()
    invalid_fk_count = df.filter(
        col('{column.name}').isNotNull() & 
        ~col('{column.name}').isin(valid_fk_values)
    ).count()
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'consistency',
        'severity': 'critical',
        'total_rows': total_count,
        'null_fk_count': null_fk_count,
        'invalid_fk_count': invalid_fk_count
    }}
                    """,
                    column_name=column.name
                ))
    
    def _generate_timeliness_rules(self, table_meta: TableMetadata):
        """Generate timeliness rules for data freshness validation"""
        table_name = table_meta.name
        
        # Data freshness check
        if table_meta.last_updated and table_meta.expected_freshness_hours:
            rule_id = f"{table_name}_data_freshness"
            self.rules.append(DataQualityRule(
                rule_id=rule_id,
                rule_name=f"Data freshness check for {table_name}",
                dimension="timeliness",
                severity="critical",
                description=f"Check if data is fresh within {table_meta.expected_freshness_hours} hours",
                spark_sql=f"""
                SELECT 
                    '{rule_id}' as rule_id,
                    '{table_name}' as table_name,
                    'data_freshness' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '{table_meta.last_updated}') <= {table_meta.expected_freshness_hours} THEN 1 ELSE 0 END) as fresh_rows,
                    DATEDIFF(CURRENT_TIMESTAMP(), '{table_meta.last_updated}') as hours_since_update
                FROM {table_name}
                """,
                python_code=f"""
def check_data_freshness(df):
    \"\"\"Check data freshness\"\"\"
    from pyspark.sql.functions import current_timestamp, datediff, lit
    from datetime import datetime
    
    last_updated = datetime.strptime('{table_meta.last_updated}', '%Y-%m-%d %H:%M:%S')
    expected_freshness_hours = {table_meta.expected_freshness_hours}
    
    # Calculate hours since last update
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    is_fresh = hours_since_update <= expected_freshness_hours
    fresh_rows = total_count if is_fresh else 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': 'data_freshness',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'fresh_rows': fresh_rows,
        'hours_since_update': round(hours_since_update, 2),
        'is_fresh': is_fresh
    }}
                """
            ))
        
        # SLA compliance check
        if table_meta.sla_hours:
            rule_id = f"{table_name}_sla_compliance"
            self.rules.append(DataQualityRule(
                rule_id=rule_id,
                rule_name=f"SLA compliance check for {table_name}",
                dimension="timeliness",
                severity="critical",
                description=f"Check if data meets SLA of {table_meta.sla_hours} hours",
                spark_sql=f"""
                SELECT 
                    '{rule_id}' as rule_id,
                    '{table_name}' as table_name,
                    'sla_compliance' as column_name,
                    'timeliness' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    CASE WHEN DATEDIFF(CURRENT_TIMESTAMP(), '{table_meta.last_updated}') <= {table_meta.sla_hours} THEN 1 ELSE 0 END as sla_met
                FROM {table_name}
                """,
                python_code=f"""
def check_sla_compliance(df):
    \"\"\"Check SLA compliance\"\"\"
    from datetime import datetime
    
    last_updated = datetime.strptime('{table_meta.last_updated}', '%Y-%m-%d %H:%M:%S')
    sla_hours = {table_meta.sla_hours}
    
    current_time = datetime.now()
    hours_since_update = (current_time - last_updated).total_seconds() / 3600
    
    total_count = df.count()
    sla_met = hours_since_update <= sla_hours
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': 'sla_compliance',
        'dimension': 'timeliness',
        'severity': 'critical',
        'total_rows': total_count,
        'sla_met': sla_met,
        'hours_since_update': round(hours_since_update, 2)
    }}
                """
            ))
    
    def _generate_validity_rules(self, table_meta: TableMetadata):
        """Generate validity rules for business rule validation"""
        table_name = table_meta.name
        
        for column in table_meta.columns:
            # Data type validation
            rule_id = f"{table_name}_{column.name}_data_type_validation"
            self.rules.append(DataQualityRule(
                rule_id=rule_id,
                rule_name=f"Data type validation for {column.name}",
                dimension="validity",
                severity="critical",
                description=f"Validate {column.name} has correct data type",
                spark_sql=f"""
                SELECT 
                    '{rule_id}' as rule_id,
                    '{table_name}' as table_name,
                    '{column.name}' as column_name,
                    'validity' as dimension,
                    'critical' as severity,
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN {column.name} IS NULL OR CAST({column.name} AS {column.data_type}) IS NOT NULL THEN 1 ELSE 0 END) as valid_type_rows
                FROM {table_name}
                """,
                python_code=f"""
def validate_{column.name}_data_type(df):
    \"\"\"Validate {column.name} has correct data type\"\"\"
    from pyspark.sql.functions import col, when, isnan, isnull
    from pyspark.sql.types import {self._get_pyspark_type(column.data_type)}
    
    total_count = df.count()
    
    try:
        # Try to cast to expected type
        valid_df = df.filter(col('{column.name}').cast({self._get_pyspark_type(column.data_type)}).isNotNull())
        valid_type_rows = valid_df.count()
    except Exception:
        valid_type_rows = 0
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'validity',
        'severity': 'critical',
        'total_rows': total_count,
        'valid_type_rows': valid_type_rows
    }}
                """,
                column_name=column.name
            ))
            
            # Business rule validation
            if column.business_rules:
                for i, rule in enumerate(column.business_rules):
                    rule_id = f"{table_name}_{column.name}_business_rule_{i+1}"
                    self.rules.append(DataQualityRule(
                        rule_id=rule_id,
                        rule_name=f"Business rule validation for {column.name}",
                        dimension="validity",
                        severity="warning",
                        description=f"Validate business rule: {rule}",
                        spark_sql=f"""
                        SELECT 
                            '{rule_id}' as rule_id,
                            '{table_name}' as table_name,
                            '{column.name}' as column_name,
                            'validity' as dimension,
                            'warning' as severity,
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN {rule} THEN 1 ELSE 0 END) as valid_rule_rows
                        FROM {table_name}
                        """,
                        python_code=f"""
def validate_{column.name}_business_rule_{i+1}(df):
    \"\"\"Validate business rule for {column.name}: {rule}\"\"\"
    from pyspark.sql.functions import expr
    
    total_count = df.count()
    valid_rule_rows = df.filter(expr('{rule}')).count()
    
    return {{
        'rule_id': '{rule_id}',
        'table_name': '{table_name}',
        'column_name': '{column.name}',
        'dimension': 'validity',
        'severity': 'warning',
        'total_rows': total_count,
        'valid_rule_rows': valid_rule_rows,
        'business_rule': '{rule}'
    }}
                        """,
                        column_name=column.name
                    ))
    
    def _get_pyspark_type(self, sql_type: str) -> str:
        """Convert SQL data type to PySpark type"""
        type_mapping = {
            'int': 'IntegerType()',
            'integer': 'IntegerType()',
            'bigint': 'LongType()',
            'smallint': 'ShortType()',
            'tinyint': 'ByteType()',
            'float': 'FloatType()',
            'double': 'DoubleType()',
            'decimal': 'DecimalType()',
            'string': 'StringType()',
            'varchar': 'StringType()',
            'char': 'StringType()',
            'text': 'StringType()',
            'boolean': 'BooleanType()',
            'bool': 'BooleanType()',
            'date': 'DateType()',
            'timestamp': 'TimestampType()',
            'datetime': 'TimestampType()'
        }
        return type_mapping.get(sql_type.lower(), 'StringType()')
    
    def export_spark_sql_commands(self, output_file: str = None) -> str:
        """Export all rules as Spark SQL commands"""
        sql_commands = []
        
        for rule in self.rules:
            sql_commands.append(f"-- {rule.rule_name}")
            sql_commands.append(f"-- {rule.description}")
            sql_commands.append(rule.spark_sql.strip())
            sql_commands.append("")
        
        sql_content = "\n".join(sql_commands)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(sql_content)
        
        return sql_content
    
    def export_python_pipeline_code(self, output_file: str = None) -> str:
        """Export all rules as Python pipeline integration code"""
        python_code = '''
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
        
        report = "Data Quality Report\\n" + "="*50 + "\\n"
        
        for result in self.results:
            report += f"Rule ID: {result['rule_id']}\\n"
            report += f"Table: {result['table_name']}\\n"
            report += f"Column: {result.get('column_name', 'N/A')}\\n"
            report += f"Dimension: {result['dimension']}\\n"
            report += f"Severity: {result['severity']}\\n"
            report += f"Total Rows: {result['total_rows']}\\n"
            
            # Add dimension-specific metrics
            if result['dimension'] == 'accuracy':
                report += f"Valid Rows: {result.get('valid_rows', 'N/A')}\\n"
                report += f"Accuracy: {result.get('accuracy_percentage', 'N/A')}%\\n"
            elif result['dimension'] == 'completeness':
                report += f"Null Count: {result.get('null_count', 'N/A')}\\n"
                report += f"Null Percentage: {result.get('null_percentage', 'N/A')}%\\n"
            elif result['dimension'] == 'consistency':
                report += f"Unique Rows: {result.get('unique_rows', 'N/A')}\\n"
                report += f"Is Unique: {result.get('is_unique', 'N/A')}\\n"
            elif result['dimension'] == 'timeliness':
                report += f"Fresh Rows: {result.get('fresh_rows', 'N/A')}\\n"
                report += f"Hours Since Update: {result.get('hours_since_update', 'N/A')}\\n"
            elif result['dimension'] == 'validity':
                report += f"Valid Type Rows: {result.get('valid_type_rows', 'N/A')}\\n"
                report += f"Valid Rule Rows: {result.get('valid_rule_rows', 'N/A')}\\n"
            
            report += "\\n" + "-"*30 + "\\n"
        
        return report

# Data Quality Rule Functions
'''
        
        # Add all rule functions
        for rule in self.rules:
            python_code += f"\n{rule.python_code}\n"
        
        python_code += '''
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
'''
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(python_code)
        
        return python_code


def create_sample_metadata() -> Dict[str, TableMetadata]:
    """Create sample table metadata for demonstration"""
    
    # Sample users table
    users_columns = [
        ColumnMetadata(
            name="user_id",
            data_type="int",
            nullable=False,
            is_primary_key=True,
            min_value=1,
            max_value=1000000
        ),
        ColumnMetadata(
            name="email",
            data_type="string",
            nullable=False,
            pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        ),
        ColumnMetadata(
            name="age",
            data_type="int",
            nullable=True,
            min_value=0,
            max_value=120
        ),
        ColumnMetadata(
            name="created_at",
            data_type="timestamp",
            nullable=False
        ),
        ColumnMetadata(
            name="status",
            data_type="string",
            nullable=False,
            unique_values=["active", "inactive", "pending"]
        )
    ]
    
    users_table = TableMetadata(
        name="users",
        columns=users_columns,
        row_count=10000,
        last_updated=datetime.now() - timedelta(hours=2),
        expected_freshness_hours=24,
        sla_hours=6
    )
    
    # Sample orders table
    orders_columns = [
        ColumnMetadata(
            name="order_id",
            data_type="int",
            nullable=False,
            is_primary_key=True
        ),
        ColumnMetadata(
            name="user_id",
            data_type="int",
            nullable=False,
            is_foreign_key=True,
            referenced_table="users",
            referenced_column="user_id"
        ),
        ColumnMetadata(
            name="amount",
            data_type="decimal",
            nullable=False,
            min_value=0.01,
            max_value=10000.00
        ),
        ColumnMetadata(
            name="order_date",
            data_type="date",
            nullable=False
        ),
        ColumnMetadata(
            name="status",
            data_type="string",
            nullable=False,
            unique_values=["pending", "confirmed", "shipped", "delivered", "cancelled"]
        )
    ]
    
    orders_table = TableMetadata(
        name="orders",
        columns=orders_columns,
        row_count=50000,
        last_updated=datetime.now() - timedelta(hours=1),
        expected_freshness_hours=12,
        sla_hours=2
    )
    
    return {
        "users": users_table,
        "orders": orders_table
    }


if __name__ == "__main__":
    # Create sample metadata
    sample_metadata = create_sample_metadata()
    
    # Initialize rule generator
    dq_generator = DataQualityRuleGenerator()
    
    # Add table metadata
    for table_name, table_meta in sample_metadata.items():
        dq_generator.add_table_metadata(table_meta)
    
    # Generate all rules
    rules = dq_generator.generate_all_rules()
    
    print(f"Generated {len(rules)} data quality rules")
    
    # Export Spark SQL commands
    spark_sql = dq_generator.export_spark_sql_commands("data_quality_rules.sql")
    print("Spark SQL commands exported to data_quality_rules.sql")
    
    # Export Python pipeline code
    python_code = dq_generator.export_python_pipeline_code("data_quality_pipeline.py")
    print("Python pipeline code exported to data_quality_pipeline.py")
    
    # Print sample rules
    print("\nSample generated rules:")
    for rule in rules[:3]:  # Show first 3 rules
        print(f"- {rule.rule_name} ({rule.dimension})")