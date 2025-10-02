"""
Main Data Quality Engine - Core orchestrator for all data quality operations
Inspired by Ataccama's data quality platform capabilities
"""

from typing import Dict, List, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, count, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import json
from datetime import datetime
import logging

from .profiler import DataProfiler
from .validator import DataValidator
from .metrics import QualityMetrics
from .anomaly_detector import AnomalyDetector

class DataQualityEngine:
    """
    Main Data Quality Engine that orchestrates all data quality operations
    Similar to Ataccama's data quality platform
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the Data Quality Engine
        
        Args:
            spark_session: Spark session instance (optional)
        """
        self.spark = spark_session or SparkSession.builder.appName("DataQualityEngine").getOrCreate()
        self.profiler = DataProfiler(self.spark)
        self.validator = DataValidator(self.spark)
        self.metrics = QualityMetrics(self.spark)
        self.anomaly_detector = AnomalyDetector(self.spark)
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def profile_data(self, df: DataFrame, sample_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Comprehensive data profiling similar to Ataccama's profiling capabilities
        
        Args:
            df: Input DataFrame
            sample_size: Optional sample size for large datasets
            
        Returns:
            Dictionary containing comprehensive data profile
        """
        self.logger.info("Starting data profiling...")
        
        # Sample data if needed
        if sample_size and df.count() > sample_size:
            df_sample = df.sample(fraction=sample_size/df.count(), seed=42)
        else:
            df_sample = df
            
        profile = {
            "timestamp": datetime.now().isoformat(),
            "dataset_info": self._get_dataset_info(df),
            "column_profiles": self.profiler.profile_columns(df_sample),
            "data_quality_metrics": self.metrics.calculate_basic_metrics(df_sample),
            "anomaly_analysis": self.anomaly_detector.detect_anomalies(df_sample),
            "recommendations": self._generate_recommendations(df_sample)
        }
        
        self.logger.info("Data profiling completed")
        return profile
    
    def validate_data(self, df: DataFrame, rules_config: Union[Dict, str]) -> Dict[str, Any]:
        """
        Validate data against quality rules
        
        Args:
            df: Input DataFrame
            rules_config: Rules configuration (dict or path to JSON file)
            
        Returns:
            Validation results with pass/fail status and details
        """
        self.logger.info("Starting data validation...")
        
        # Load rules configuration
        if isinstance(rules_config, str):
            with open(rules_config, 'r') as f:
                rules = json.load(f)
        else:
            rules = rules_config
            
        validation_results = self.validator.validate_dataframe(df, rules)
        
        self.logger.info(f"Data validation completed. {validation_results['summary']['total_rules']} rules processed")
        return validation_results
    
    def cleanse_data(self, df: DataFrame, cleansing_rules: Dict[str, Any]) -> DataFrame:
        """
        Cleanse data based on specified rules
        
        Args:
            df: Input DataFrame
            cleansing_rules: Data cleansing configuration
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Starting data cleansing...")
        
        cleaned_df = df
        
        # Apply null value handling
        if 'null_handling' in cleansing_rules:
            cleaned_df = self._handle_nulls(cleaned_df, cleansing_rules['null_handling'])
            
        # Apply data type corrections
        if 'type_corrections' in cleansing_rules:
            cleaned_df = self._correct_data_types(cleaned_df, cleansing_rules['type_corrections'])
            
        # Apply standardization rules
        if 'standardization' in cleansing_rules:
            cleaned_df = self._standardize_data(cleaned_df, cleansing_rules['standardization'])
            
        self.logger.info("Data cleansing completed")
        return cleaned_df
    
    def generate_quality_report(self, df: DataFrame, rules_config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report
        
        Args:
            df: Input DataFrame
            rules_config: Optional validation rules
            
        Returns:
            Comprehensive quality report
        """
        self.logger.info("Generating comprehensive quality report...")
        
        # Get data profile
        profile = self.profile_data(df)
        
        # Run validation if rules provided
        validation_results = None
        if rules_config:
            validation_results = self.validate_data(df, rules_config)
            
        # Calculate quality score
        quality_score = self.metrics.calculate_quality_score(df, validation_results)
        
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "dataset_info": profile["dataset_info"],
            "data_profile": profile,
            "validation_results": validation_results,
            "quality_score": quality_score,
            "recommendations": profile["recommendations"]
        }
        
        self.logger.info("Quality report generated")
        return report
    
    def _get_dataset_info(self, df: DataFrame) -> Dict[str, Any]:
        """Get basic dataset information"""
        return {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "schema": [{"name": field.name, "type": str(field.dataType)} for field in df.schema.fields]
        }
    
    def _generate_recommendations(self, df: DataFrame) -> List[str]:
        """Generate data quality recommendations"""
        recommendations = []
        
        # Check for high null percentage
        null_counts = df.select([count(when(isnull(c) | isnan(c), c)).alias(c) for c in df.columns]).collect()[0]
        total_rows = df.count()
        
        for col_name, null_count in zip(df.columns, null_counts):
            null_percentage = (null_count / total_rows) * 100
            if null_percentage > 50:
                recommendations.append(f"Column '{col_name}' has {null_percentage:.1f}% null values - consider data source investigation")
            elif null_percentage > 20:
                recommendations.append(f"Column '{col_name}' has {null_percentage:.1f}% null values - consider null handling strategy")
                
        # Check for duplicate rows
        duplicate_count = df.count() - df.dropDuplicates().count()
        if duplicate_count > 0:
            recommendations.append(f"Found {duplicate_count} duplicate rows - consider deduplication")
            
        return recommendations
    
    def _handle_nulls(self, df: DataFrame, null_rules: Dict[str, Any]) -> DataFrame:
        """Handle null values based on rules"""
        cleaned_df = df
        
        for column, rule in null_rules.items():
            if rule['strategy'] == 'fill':
                if rule['method'] == 'constant':
                    cleaned_df = cleaned_df.fillna({column: rule['value']})
                elif rule['method'] == 'mean':
                    mean_val = df.select(col(column)).filter(col(column).isNotNull()).agg({column: 'mean'}).collect()[0][0]
                    cleaned_df = cleaned_df.fillna({column: mean_val})
            elif rule['strategy'] == 'drop':
                cleaned_df = cleaned_df.filter(col(column).isNotNull())
                
        return cleaned_df
    
    def _correct_data_types(self, df: DataFrame, type_rules: Dict[str, str]) -> DataFrame:
        """Correct data types based on rules"""
        cleaned_df = df
        
        for column, target_type in type_rules.items():
            if target_type == 'string':
                cleaned_df = cleaned_df.withColumn(column, col(column).cast(StringType()))
            elif target_type == 'integer':
                cleaned_df = cleaned_df.withColumn(column, col(column).cast(IntegerType()))
            elif target_type == 'double':
                cleaned_df = cleaned_df.withColumn(column, col(column).cast(DoubleType()))
                
        return cleaned_df
    
    def _standardize_data(self, df: DataFrame, std_rules: Dict[str, Any]) -> DataFrame:
        """Standardize data based on rules"""
        cleaned_df = df
        
        for column, rules in std_rules.items():
            if 'case_standardization' in rules:
                if rules['case_standardization'] == 'upper':
                    cleaned_df = cleaned_df.withColumn(column, col(column).upper())
                elif rules['case_standardization'] == 'lower':
                    cleaned_df = cleaned_df.withColumn(column, col(column).lower())
                    
        return cleaned_df