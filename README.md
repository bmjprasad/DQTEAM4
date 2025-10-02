# Auto Data Quality Rules Generator

An automated system for generating comprehensive data quality rules covering five key dimensions: **Accuracy**, **Completeness**, **Consistency**, **Timeliness**, and **Validity**. The system generates both Spark SQL commands and Python pipeline integration code.

## Features

### Data Quality Dimensions Covered

1. **Accuracy** - Data correctness and format validation
   - Format validation using regex patterns
   - Range validation for numeric values
   - Data type validation

2. **Completeness** - Missing data detection
   - Null value checks
   - Empty string detection
   - Required field validation

3. **Consistency** - Cross-table and referential integrity validation
   - Primary key uniqueness validation
   - Foreign key referential integrity
   - Cross-table consistency checks

4. **Timeliness** - Data freshness and SLA validation
   - Data freshness checks
   - SLA compliance validation
   - Update frequency monitoring

5. **Validity** - Business rule and data type validation
   - Business rule enforcement
   - Data type validation
   - Custom validation logic

### Key Components

- **DataQualityRuleGenerator**: Main class for generating rules from metadata
- **DataProfileAnalyzer**: Automatically analyzes sample data to extract metadata
- **ColumnMetadata**: Metadata structure for table columns
- **TableMetadata**: Metadata structure for tables
- **DataQualityRule**: Generated rule definitions

## Installation

```bash
pip install pandas numpy pyspark
```

## Quick Start

### 1. Using Sample Data (Automatic Analysis)

```python
from data_profile_analyzer import DataProfileAnalyzer, create_sample_data

# Create sample data
sample_data = create_sample_data()

# Initialize analyzer
analyzer = DataProfileAnalyzer()

# Analyze tables and generate rules
for table_name, df in sample_data.items():
    analyzer.analyze_dataframe(df, table_name)

# Generate data quality rules
spark_sql, python_code = analyzer.generate_rules_from_profiles()
```

### 2. Using Custom Metadata (Manual Definition)

```python
from data_quality_generator import DataQualityRuleGenerator, TableMetadata, ColumnMetadata
from datetime import datetime

# Define table metadata
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
    )
]

users_table = TableMetadata(
    name="users",
    columns=users_columns,
    row_count=10000,
    last_updated=datetime.now(),
    expected_freshness_hours=24,
    sla_hours=6
)

# Generate rules
dq_generator = DataQualityRuleGenerator()
dq_generator.add_table_metadata(users_table)
rules = dq_generator.generate_all_rules()

# Export rules
spark_sql = dq_generator.export_spark_sql_commands("my_dq_rules.sql")
python_code = dq_generator.export_python_pipeline_code("my_dq_pipeline.py")
```

## Generated Output

### Spark SQL Commands

The system generates comprehensive Spark SQL queries for each data quality rule:

```sql
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
```

### Python Pipeline Integration

The system generates Python functions that can be directly integrated into your data pipelines:

```python
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
```

## Usage in Data Pipelines

### PySpark Pipeline Integration

```python
from pyspark.sql import SparkSession
from data_quality_pipeline import DataQualityPipeline

# Initialize Spark
spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()

# Load your data
df = spark.read.table("your_table_name")

# Initialize data quality pipeline
dq_pipeline = DataQualityPipeline(spark)

# Execute data quality rules
result = dq_pipeline.execute_rule(df, validate_email_format)

# Generate report
report = dq_pipeline.generate_report()
print(report)
```

### Airflow Integration

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_quality_pipeline import DataQualityPipeline

def run_data_quality_checks(**context):
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    df = spark.read.table("your_table")
    
    dq_pipeline = DataQualityPipeline(spark)
    dq_pipeline.execute_rule(df, validate_email_format)
    
    # Send alerts if quality issues found
    results = dq_pipeline.get_all_results()
    for result in results:
        if result['accuracy_percentage'] < 95:
            send_alert(f"Data quality issue: {result['rule_id']}")

dag = DAG('data_quality_checks', schedule_interval='@daily')
quality_task = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)
```

## Configuration Options

### Column Metadata Options

```python
ColumnMetadata(
    name="column_name",
    data_type="string",  # int, string, decimal, timestamp, etc.
    nullable=True,       # Whether column allows nulls
    is_primary_key=False,
    is_foreign_key=False,
    referenced_table=None,
    referenced_column=None,
    min_value=None,      # Minimum value for range validation
    max_value=None,      # Maximum value for range validation
    unique_values=None,  # List of allowed values
    pattern=None,        # Regex pattern for format validation
    business_rules=None  # List of custom business rules
)
```

### Table Metadata Options

```python
TableMetadata(
    name="table_name",
    columns=[...],                    # List of ColumnMetadata
    row_count=1000,                  # Total row count
    last_updated=datetime.now(),     # Last update timestamp
    expected_freshness_hours=24,     # Expected data freshness
    sla_hours=6                      # SLA for data updates
)
```

## Advanced Features

### Custom Business Rules

```python
# Add custom business rules to columns
ColumnMetadata(
    name="age",
    data_type="int",
    business_rules=[
        "age >= 0",
        "age <= 120",
        "age IS NOT NULL"
    ]
)
```

### Pattern Detection

The system automatically detects common patterns:
- Email addresses
- Phone numbers
- UUIDs
- Dates
- Credit card numbers
- SSNs

### Foreign Key Detection

The system automatically detects foreign key relationships between tables based on:
- Column name matching
- Data type compatibility
- Primary key identification

## Monitoring and Alerting

### Quality Metrics

Each rule generates metrics including:
- Total row count
- Valid/invalid row counts
- Percentage calculations
- Severity levels (critical, warning, info)

### Integration with Monitoring Systems

```python
# Example: Integration with monitoring dashboard
def send_to_monitoring(results):
    for result in results:
        if result['severity'] == 'critical':
            send_critical_alert(result)
        elif result['severity'] == 'warning':
            send_warning_alert(result)
```

## Best Practices

1. **Regular Rule Updates**: Update rules as data schemas evolve
2. **Threshold Management**: Set appropriate thresholds for each rule
3. **Monitoring Integration**: Integrate with your monitoring and alerting systems
4. **Performance Optimization**: Use sampling for large datasets
5. **Documentation**: Document custom business rules and their rationale

## Examples

See the `examples/` directory for:
- Complete pipeline examples
- Airflow DAG examples
- Monitoring integration examples
- Custom rule examples

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.