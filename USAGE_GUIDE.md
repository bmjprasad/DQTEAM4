# Ataccama-like Data Quality Framework for Databricks Spark

## Overview

This comprehensive data quality framework provides Ataccama-like capabilities for Databricks Spark, including data profiling, validation, cleansing, monitoring, and automation.

## Quick Start

### 1. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Make automation script executable
chmod +x run_automation.py
```

### 2. Basic Usage

```python
from src.core.data_quality_engine import DataQualityEngine
from pyspark.sql import SparkSession

# Initialize
spark = SparkSession.builder.appName("DataQuality").getOrCreate()
dq_engine = DataQualityEngine(spark)

# Load your data
df = spark.read.table("your_table")

# Profile data
profile = dq_engine.profile_data(df)

# Validate data
validation_rules = {
    "not_null_rule": {
        "type": "not_null",
        "columns": ["id", "name", "email"]
    }
}
results = dq_engine.validate_data(df, validation_rules)

# Generate quality report
report = dq_engine.generate_quality_report(df, validation_rules)
```

### 3. Start Dashboard

```bash
python src/dashboard/app.py
# Open http://localhost:8050 in your browser
```

### 4. Run Automation

```bash
# Setup default configurations
python run_automation.py --setup-defaults

# Start automation system
python run_automation.py --mode start --config config/automation_config.json

# Check status
python run_automation.py --mode status

# Generate report
python run_automation.py --mode report
```

## Features

### Core Components

1. **Data Profiling**
   - Statistical analysis
   - Pattern detection
   - Data type analysis
   - Completeness metrics

2. **Data Validation**
   - Not null constraints
   - Range validations
   - Pattern matching
   - Custom SQL rules
   - Referential integrity
   - Business rules

3. **Data Cleansing**
   - Null value handling
   - Data type corrections
   - Standardization
   - Custom transformations

4. **Quality Metrics**
   - Completeness scoring
   - Consistency metrics
   - Accuracy assessment
   - Validity checks
   - Uniqueness analysis

5. **Anomaly Detection**
   - Statistical methods (IQR, Z-score)
   - Machine learning approaches
   - Pattern-based detection
   - Temporal analysis

6. **Interactive Dashboard**
   - Web-based UI
   - Real-time monitoring
   - Quality score visualization
   - Alert management

7. **Automation**
   - Scheduled quality checks
   - Multi-channel alerting
   - Configuration management
   - Monitoring and reporting

## Configuration

### Validation Rules

```json
{
  "rule_name": {
    "type": "not_null|unique|range|pattern|length|data_type|custom_sql|referential_integrity|business_rule",
    "column": "column_name",
    "columns": ["col1", "col2"],
    "min_value": 0,
    "max_value": 100,
    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
    "min_length": 3,
    "max_length": 100,
    "expected_type": "numeric|integer|date|string",
    "sql_condition": "custom SQL condition",
    "reference_table": "ref_table",
    "reference_column": "ref_column",
    "rule_expression": "business rule expression"
  }
}
```

### Cleansing Rules

```json
{
  "null_handling": {
    "column_name": {
      "strategy": "fill|drop",
      "method": "constant|mean|median|mode",
      "value": "default_value"
    }
  },
  "type_corrections": {
    "column_name": "target_type"
  },
  "standardization": {
    "column_name": {
      "case_standardization": "upper|lower|title",
      "date_format": "yyyy-MM-dd"
    }
  }
}
```

### Automation Configuration

```json
{
  "quality_checks": [
    {
      "name": "check_name",
      "dataset": "table_name",
      "rules_config": {...},
      "schedule": "daily|weekly|monthly",
      "schedule_time": "HH:MM",
      "enabled": true,
      "alert_threshold": 80.0
    }
  ],
  "alert_rules": [
    {
      "name": "rule_name",
      "condition": "quality_score < 70",
      "severity": "low|medium|high|critical",
      "channels": ["email", "slack", "webhook", "log"],
      "enabled": true,
      "cooldown_minutes": 60
    }
  ],
  "channels_config": {
    "email": {
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "username": "your-email@gmail.com",
      "password": "your-app-password",
      "from_email": "data-quality@yourcompany.com",
      "to_emails": ["admin@yourcompany.com"]
    },
    "slack": {
      "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    },
    "webhook": {
      "url": "https://your-monitoring-system.com/webhook"
    }
  }
}
```

## Examples

### Basic Data Profiling

```python
# Profile a dataset
profile = dq_engine.profile_data(df)

# Access profiling results
print(f"Overall quality score: {profile['data_quality_metrics']['overall_quality_score']['overall_score']}")
print(f"Grade: {profile['data_quality_metrics']['overall_quality_score']['grade']}")

# Column-specific information
for column, col_profile in profile['column_profiles'].items():
    print(f"Column {column}: {col_profile['null_percentage']:.1f}% null values")
```

### Advanced Validation

```python
# Complex validation rules
advanced_rules = {
    "premium_customer_minimum": {
        "type": "custom_sql",
        "sql_condition": """
            NOT EXISTS (
                SELECT 1 FROM customers c 
                WHERE c.customer_id = sales.customer_id 
                AND c.tier = 'Premium' 
                AND sales.amount < 100.00
            )
        """
    },
    "referential_integrity": {
        "type": "referential_integrity",
        "column": "customer_id",
        "reference_table": "customers",
        "reference_column": "customer_id"
    }
}

results = dq_engine.validate_data(df, advanced_rules)
```

### Automated Monitoring

```python
# Setup automation
automation_manager = AutomationManager(dq_engine)

# Add custom quality check
automation_manager.add_custom_quality_check(
    name="daily_sales_quality",
    dataset="sales",
    rules_config=validation_rules,
    schedule=ScheduleType.DAILY,
    schedule_time="09:00",
    alert_threshold=85.0
)

# Start automation
automation_manager.start_automation()
```

## Databricks Integration

### Using in Databricks Notebooks

```python
# In a Databricks notebook
%run /workspace/src/core/data_quality_engine

# Initialize with existing Spark session
dq_engine = DataQualityEngine(spark)

# Use with your data
df = spark.table("your_table")
profile = dq_engine.profile_data(df)
```

### Job Scheduling

```python
# Create a Databricks job that runs quality checks
# Use the run_automation.py script as the job entry point
# Configure with your automation_config.json
```

## Monitoring and Alerting

### Alert Channels

1. **Email**: SMTP-based email notifications
2. **Slack**: Webhook-based Slack messages
3. **Webhook**: HTTP POST to custom endpoints
4. **Log**: File-based logging

### Quality Score Thresholds

- **A (90-100)**: Excellent quality
- **B (80-89)**: Good quality
- **C (70-79)**: Fair quality
- **D (60-69)**: Poor quality
- **F (0-59)**: Critical quality issues

## Best Practices

1. **Start Small**: Begin with basic validation rules and gradually add complexity
2. **Monitor Continuously**: Use automation to run checks regularly
3. **Set Appropriate Thresholds**: Adjust alert thresholds based on business requirements
4. **Document Rules**: Keep clear documentation of validation rules and their purposes
5. **Regular Reviews**: Periodically review and update quality rules
6. **Performance Optimization**: Use sampling for large datasets during profiling
7. **Error Handling**: Implement proper error handling and logging

## Troubleshooting

### Common Issues

1. **Memory Issues**: Use sampling for large datasets
2. **Slow Performance**: Optimize Spark configurations and use appropriate partitioning
3. **Alert Spam**: Adjust cooldown periods and severity thresholds
4. **Configuration Errors**: Validate JSON configuration files

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with verbose output
python run_automation.py --mode status --verbose
```

## Support

For issues and questions:
1. Check the logs in `data_quality_automation.log`
2. Review configuration files for syntax errors
3. Test individual components before running full automation
4. Use the demo notebooks for reference implementations

## License

This framework is provided as-is for educational and development purposes.