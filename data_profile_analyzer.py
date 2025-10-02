"""
Data Profile Analyzer

This module analyzes sample data to automatically extract metadata
that can be used for data quality rule generation.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from data_quality_generator import ColumnMetadata, TableMetadata, DataQualityRuleGenerator


class DataProfileAnalyzer:
    """Analyzes data profiles to extract metadata for rule generation"""
    
    def __init__(self):
        self.analyzed_metadata: Dict[str, TableMetadata] = {}
    
    def analyze_dataframe(self, df: pd.DataFrame, table_name: str, 
                         sample_size: int = 10000) -> TableMetadata:
        """Analyze a pandas DataFrame to extract metadata"""
        
        # Sample data if it's too large
        if len(df) > sample_size:
            df_sample = df.sample(n=sample_size, random_state=42)
        else:
            df_sample = df.copy()
        
        columns = []
        
        for col_name in df_sample.columns:
            column_meta = self._analyze_column(df_sample, col_name)
            columns.append(column_meta)
        
        # Determine primary keys (columns with unique values)
        pk_candidates = [col for col in columns if col.unique_values and len(col.unique_values) == len(df_sample)]
        if pk_candidates:
            pk_candidates[0].is_primary_key = True
        
        # Calculate table-level metadata
        table_metadata = TableMetadata(
            name=table_name,
            columns=columns,
            row_count=len(df),
            last_updated=datetime.now(),
            expected_freshness_hours=24,  # Default value
            sla_hours=6  # Default value
        )
        
        self.analyzed_metadata[table_name] = table_metadata
        return table_metadata
    
    def _analyze_column(self, df: pd.DataFrame, col_name: str) -> ColumnMetadata:
        """Analyze a single column to extract metadata"""
        
        series = df[col_name]
        
        # Basic metadata
        data_type = self._infer_data_type(series)
        nullable = series.isnull().any()
        null_count = series.isnull().sum()
        total_count = len(series)
        
        # Unique values analysis
        unique_values = None
        if series.nunique() < 100:  # Only store if reasonable number of unique values
            unique_values = series.dropna().unique().tolist()
        
        # Range analysis for numeric columns
        min_value = None
        max_value = None
        if pd.api.types.is_numeric_dtype(series):
            min_value = series.min()
            max_value = series.max()
        
        # Pattern analysis for string columns
        pattern = None
        if pd.api.types.is_string_dtype(series) or series.dtype == 'object':
            pattern = self._detect_pattern(series)
        
        # Business rules detection
        business_rules = self._detect_business_rules(series, col_name)
        
        return ColumnMetadata(
            name=col_name,
            data_type=data_type,
            nullable=nullable,
            is_primary_key=False,  # Will be determined later
            is_foreign_key=False,  # Will be determined later
            min_value=min_value,
            max_value=max_value,
            unique_values=unique_values,
            null_count=null_count,
            total_count=total_count,
            pattern=pattern,
            business_rules=business_rules
        )
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer SQL data type from pandas Series"""
        
        if pd.api.types.is_integer_dtype(series):
            if series.min() >= -128 and series.max() <= 127:
                return "tinyint"
            elif series.min() >= -32768 and series.max() <= 32767:
                return "smallint"
            elif series.min() >= -2147483648 and series.max() <= 2147483647:
                return "int"
            else:
                return "bigint"
        
        elif pd.api.types.is_float_dtype(series):
            return "double"
        
        elif pd.api.types.is_bool_dtype(series):
            return "boolean"
        
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "timestamp"
        
        elif pd.api.types.is_string_dtype(series) or series.dtype == 'object':
            return "string"
        
        else:
            return "string"  # Default fallback
    
    def _detect_pattern(self, series: pd.Series) -> Optional[str]:
        """Detect common patterns in string data"""
        
        # Remove null values for pattern detection
        non_null_series = series.dropna()
        if len(non_null_series) == 0:
            return None
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if non_null_series.str.match(email_pattern).sum() / len(non_null_series) > 0.8:
            return email_pattern
        
        # Phone number pattern
        phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        if non_null_series.str.match(phone_pattern).sum() / len(non_null_series) > 0.8:
            return phone_pattern
        
        # UUID pattern
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if non_null_series.str.match(uuid_pattern, case=False).sum() / len(non_null_series) > 0.8:
            return uuid_pattern
        
        # Date pattern (YYYY-MM-DD)
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        if non_null_series.str.match(date_pattern).sum() / len(non_null_series) > 0.8:
            return date_pattern
        
        # Credit card pattern
        cc_pattern = r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'
        if non_null_series.str.match(cc_pattern).sum() / len(non_null_series) > 0.8:
            return cc_pattern
        
        # SSN pattern
        ssn_pattern = r'^\d{3}-\d{2}-\d{4}$'
        if non_null_series.str.match(ssn_pattern).sum() / len(non_null_series) > 0.8:
            return ssn_pattern
        
        return None
    
    def _detect_business_rules(self, series: pd.Series, col_name: str) -> List[str]:
        """Detect potential business rules from data patterns"""
        
        business_rules = []
        
        # Non-negative values for numeric columns
        if pd.api.types.is_numeric_dtype(series):
            if series.min() >= 0:
                business_rules.append(f"{col_name} >= 0")
        
        # Length constraints for string columns
        if pd.api.types.is_string_dtype(series) or series.dtype == 'object':
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                min_length = non_null_series.str.len().min()
                max_length = non_null_series.str.len().max()
                
                if min_length > 0:
                    business_rules.append(f"LENGTH({col_name}) >= {min_length}")
                if max_length < 1000:  # Only add if reasonable
                    business_rules.append(f"LENGTH({col_name}) <= {max_length}")
        
        # Status/enum values
        if series.nunique() < 20 and series.nunique() > 1:
            unique_values = series.dropna().unique()
            if all(isinstance(val, str) for val in unique_values):
                values_str = "', '".join(unique_values)
                business_rules.append(f"{col_name} IN ('{values_str}')")
        
        return business_rules
    
    def detect_foreign_keys(self, tables: Dict[str, TableMetadata]) -> Dict[str, TableMetadata]:
        """Detect foreign key relationships between tables"""
        
        for table_name, table_meta in tables.items():
            for column in table_meta.columns:
                # Look for potential foreign key relationships
                for other_table_name, other_table_meta in tables.items():
                    if other_table_name == table_name:
                        continue
                    
                    # Check if this column name matches a primary key in another table
                    for other_column in other_table_meta.columns:
                        if (other_column.is_primary_key and 
                            column.name.lower() == other_column.name.lower() and
                            column.data_type == other_column.data_type):
                            
                            column.is_foreign_key = True
                            column.referenced_table = other_table_name
                            column.referenced_column = other_column.name
                            break
        
        return tables
    
    def generate_rules_from_profiles(self, output_sql: str = "generated_dq_rules.sql", 
                                   output_python: str = "generated_dq_pipeline.py") -> Tuple[str, str]:
        """Generate data quality rules from analyzed profiles"""
        
        # Detect foreign key relationships
        self.analyzed_metadata = self.detect_foreign_keys(self.analyzed_metadata)
        
        # Initialize rule generator
        dq_generator = DataQualityRuleGenerator()
        
        # Add all analyzed metadata
        for table_name, table_meta in self.analyzed_metadata.items():
            dq_generator.add_table_metadata(table_meta)
        
        # Generate all rules
        rules = dq_generator.generate_all_rules()
        
        # Export rules
        spark_sql = dq_generator.export_spark_sql_commands(output_sql)
        python_code = dq_generator.export_python_pipeline_code(output_python)
        
        return spark_sql, python_code


def create_sample_data() -> Dict[str, pd.DataFrame]:
    """Create sample data for demonstration"""
    
    # Sample users data
    np.random.seed(42)
    n_users = 1000
    
    users_data = {
        'user_id': range(1, n_users + 1),
        'email': [f'user{i}@example.com' for i in range(1, n_users + 1)],
        'age': np.random.randint(18, 80, n_users),
        'created_at': pd.date_range('2023-01-01', periods=n_users, freq='1h'),
        'status': np.random.choice(['active', 'inactive', 'pending'], n_users, p=[0.7, 0.2, 0.1]),
        'phone': [f'+1-{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}' for _ in range(n_users)]
    }
    
    users_df = pd.DataFrame(users_data)
    
    # Add some null values
    age_indices = np.random.choice(n_users, 50, replace=False)
    phone_indices = np.random.choice(n_users, 30, replace=False)
    
    users_df.loc[age_indices, 'age'] = None
    users_df.loc[phone_indices, 'phone'] = None
    
    # Sample orders data
    n_orders = 5000
    orders_data = {
        'order_id': range(1, n_orders + 1),
        'user_id': np.random.choice(users_data['user_id'], n_orders),
        'amount': np.round(np.random.uniform(10, 1000, n_orders), 2),
        'order_date': pd.date_range('2023-01-01', periods=n_orders, freq='30min'),
        'status': np.random.choice(['pending', 'confirmed', 'shipped', 'delivered', 'cancelled'], 
                                 n_orders, p=[0.1, 0.2, 0.3, 0.35, 0.05])
    }
    
    # Add some invalid foreign keys
    orders_data['user_id'][np.random.choice(n_orders, 20, replace=False)] = 99999
    
    orders_df = pd.DataFrame(orders_data)
    
    return {
        'users': users_df,
        'orders': orders_df
    }


if __name__ == "__main__":
    # Create sample data
    sample_data = create_sample_data()
    
    # Initialize analyzer
    analyzer = DataProfileAnalyzer()
    
    # Analyze each table
    for table_name, df in sample_data.items():
        print(f"Analyzing {table_name} table...")
        table_metadata = analyzer.analyze_dataframe(df, table_name)
        print(f"  - Found {len(table_metadata.columns)} columns")
        print(f"  - Row count: {table_metadata.row_count}")
        
        # Print column details
        for col in table_metadata.columns:
            print(f"    - {col.name}: {col.data_type}, nullable={col.nullable}")
            if col.pattern:
                print(f"      Pattern: {col.pattern}")
            if col.business_rules:
                print(f"      Business rules: {col.business_rules}")
    
    # Generate data quality rules
    print("\nGenerating data quality rules...")
    spark_sql, python_code = analyzer.generate_rules_from_profiles()
    
    print(f"Generated Spark SQL rules saved to: generated_dq_rules.sql")
    print(f"Generated Python pipeline code saved to: generated_dq_pipeline.py")
    
    # Print summary
    print(f"\nTotal rules generated: {len(analyzer.analyzed_metadata)}")
    for table_name, table_meta in analyzer.analyzed_metadata.items():
        print(f"  - {table_name}: {len(table_meta.columns)} columns analyzed")