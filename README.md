# Ataccama-like Data Quality Framework for Databricks Spark

A comprehensive data quality solution built on Apache Spark for Databricks, inspired by Ataccama's data quality platform.

## Features

- **Data Profiling**: Comprehensive statistical analysis of datasets
- **Rule-based Validation**: Customizable data quality rules and constraints
- **Anomaly Detection**: ML-powered outlier detection and pattern analysis
- **Data Cleansing**: Automated data correction and standardization
- **Quality Metrics**: Real-time data quality scoring and monitoring
- **Interactive Dashboard**: Web-based UI for data quality management
- **Automated Checks**: Scheduled data quality validation and alerting

## Architecture

```
├── src/
│   ├── core/           # Core data quality engine
│   ├── validators/     # Data validation rules
│   ├── profilers/      # Data profiling components
│   ├── metrics/        # Quality metrics calculation
│   ├── dashboard/      # Web dashboard
│   └── utils/          # Utility functions
├── notebooks/          # Databricks notebooks
├── config/            # Configuration files
└── tests/             # Unit tests
```

## Quick Start

1. Install dependencies: `pip install -r requirements.txt`
2. Run data profiling: `python src/core/profiler.py`
3. Start dashboard: `python src/dashboard/app.py`
4. Open browser to `http://localhost:8050`

## Usage

```python
from src.core.data_quality_engine import DataQualityEngine

# Initialize the engine
dq_engine = DataQualityEngine()

# Load and profile data
df = spark.read.table("your_table")
profile = dq_engine.profile_data(df)

# Run quality checks
results = dq_engine.validate_data(df, rules_config)
```