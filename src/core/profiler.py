"""
Data Profiler - Comprehensive data profiling capabilities
Similar to Ataccama's data profiling features
"""

from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, min as spark_min, max as spark_max, 
    mean, stddev, variance, sum as spark_sum, avg, 
    when, isnan, isnull, regexp_replace, length, 
    trim, upper, lower, regexp_extract
)
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType
import statistics
import json

class DataProfiler:
    """
    Comprehensive data profiler for statistical analysis and pattern detection
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def profile_columns(self, df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Profile all columns in the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with column profiles
        """
        column_profiles = {}
        
        for field in df.schema.fields:
            column_name = field.name
            column_type = field.dataType
            
            self.spark.catalog.clearCache()  # Clear cache for accurate profiling
            
            if isinstance(column_type, NumericType):
                column_profiles[column_name] = self._profile_numeric_column(df, column_name)
            elif isinstance(column_type, StringType):
                column_profiles[column_name] = self._profile_string_column(df, column_name)
            elif isinstance(column_type, (DateType, TimestampType)):
                column_profiles[column_name] = self._profile_datetime_column(df, column_name)
            else:
                column_profiles[column_name] = self._profile_generic_column(df, column_name)
                
        return column_profiles
    
    def _profile_numeric_column(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Profile numeric columns with statistical analysis"""
        stats = df.select(
            count(col(column_name)).alias("count"),
            countDistinct(col(column_name)).alias("distinct_count"),
            count(when(isnull(col(column_name)) | isnan(col(column_name)), col(column_name))).alias("null_count"),
            spark_min(col(column_name)).alias("min"),
            spark_max(col(column_name)).alias("max"),
            mean(col(column_name)).alias("mean"),
            stddev(col(column_name)).alias("stddev"),
            variance(col(column_name)).alias("variance"),
            spark_sum(col(column_name)).alias("sum")
        ).collect()[0]
        
        total_count = df.count()
        null_count = stats["null_count"]
        non_null_count = stats["count"]
        
        # Calculate percentiles
        percentiles = self._calculate_percentiles(df, column_name)
        
        # Detect outliers using IQR method
        outliers = self._detect_numeric_outliers(df, column_name, percentiles)
        
        return {
            "data_type": "numeric",
            "total_count": total_count,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "null_percentage": (null_count / total_count) * 100 if total_count > 0 else 0,
            "distinct_count": stats["distinct_count"],
            "distinct_percentage": (stats["distinct_count"] / non_null_count) * 100 if non_null_count > 0 else 0,
            "min_value": stats["min"],
            "max_value": stats["max"],
            "mean": stats["mean"],
            "stddev": stats["stddev"],
            "variance": stats["variance"],
            "sum": stats["sum"],
            "percentiles": percentiles,
            "outliers": outliers,
            "data_quality_issues": self._identify_numeric_issues(df, column_name, stats, percentiles)
        }
    
    def _profile_string_column(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Profile string columns with pattern analysis"""
        # Basic statistics
        stats = df.select(
            count(col(column_name)).alias("count"),
            countDistinct(col(column_name)).alias("distinct_count"),
            count(when(isnull(col(column_name)), col(column_name))).alias("null_count"),
            spark_min(col(column_name)).alias("min"),
            spark_max(col(column_name)).alias("max")
        ).collect()[0]
        
        total_count = df.count()
        null_count = stats["null_count"]
        non_null_count = stats["count"]
        
        # String length analysis
        length_stats = df.select(
            length(col(column_name)).alias("length")
        ).filter(col(column_name).isNotNull()).agg(
            mean("length").alias("avg_length"),
            spark_min("length").alias("min_length"),
            spark_max("length").alias("max_length"),
            stddev("length").alias("stddev_length")
        ).collect()[0]
        
        # Pattern analysis
        patterns = self._analyze_string_patterns(df, column_name)
        
        # Top values analysis
        top_values = self._get_top_values(df, column_name, limit=10)
        
        return {
            "data_type": "string",
            "total_count": total_count,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "null_percentage": (null_count / total_count) * 100 if total_count > 0 else 0,
            "distinct_count": stats["distinct_count"],
            "distinct_percentage": (stats["distinct_count"] / non_null_count) * 100 if non_null_count > 0 else 0,
            "min_value": stats["min"],
            "max_value": stats["max"],
            "avg_length": length_stats["avg_length"],
            "min_length": length_stats["min_length"],
            "max_length": length_stats["max_length"],
            "stddev_length": length_stats["stddev_length"],
            "patterns": patterns,
            "top_values": top_values,
            "data_quality_issues": self._identify_string_issues(df, column_name, patterns)
        }
    
    def _profile_datetime_column(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Profile datetime columns"""
        stats = df.select(
            count(col(column_name)).alias("count"),
            countDistinct(col(column_name)).alias("distinct_count"),
            count(when(isnull(col(column_name)), col(column_name))).alias("null_count"),
            spark_min(col(column_name)).alias("min"),
            spark_max(col(column_name)).alias("max")
        ).collect()[0]
        
        total_count = df.count()
        null_count = stats["null_count"]
        
        return {
            "data_type": "datetime",
            "total_count": total_count,
            "non_null_count": stats["count"],
            "null_count": null_count,
            "null_percentage": (null_count / total_count) * 100 if total_count > 0 else 0,
            "distinct_count": stats["distinct_count"],
            "min_value": str(stats["min"]) if stats["min"] else None,
            "max_value": str(stats["max"]) if stats["max"] else None,
            "data_quality_issues": self._identify_datetime_issues(df, column_name)
        }
    
    def _profile_generic_column(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Profile generic columns"""
        stats = df.select(
            count(col(column_name)).alias("count"),
            countDistinct(col(column_name)).alias("distinct_count"),
            count(when(isnull(col(column_name)), col(column_name))).alias("null_count")
        ).collect()[0]
        
        total_count = df.count()
        null_count = stats["null_count"]
        
        return {
            "data_type": "generic",
            "total_count": total_count,
            "non_null_count": stats["count"],
            "null_count": null_count,
            "null_percentage": (null_count / total_count) * 100 if total_count > 0 else 0,
            "distinct_count": stats["distinct_count"]
        }
    
    def _calculate_percentiles(self, df: DataFrame, column_name: str) -> Dict[str, float]:
        """Calculate percentiles for numeric columns"""
        try:
            # Sample data for percentile calculation (to avoid memory issues)
            sample_df = df.select(column_name).filter(col(column_name).isNotNull()).sample(0.1)
            values = [row[0] for row in sample_df.collect() if row[0] is not None]
            
            if not values:
                return {}
                
            values.sort()
            n = len(values)
            
            percentiles = {}
            for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
                idx = int((p / 100) * (n - 1))
                percentiles[f"p{p}"] = values[idx] if idx < n else values[-1]
                
            return percentiles
        except Exception:
            return {}
    
    def _detect_numeric_outliers(self, df: DataFrame, column_name: str, percentiles: Dict[str, float]) -> Dict[str, Any]:
        """Detect outliers using IQR method"""
        try:
            q1 = percentiles.get("p25", 0)
            q3 = percentiles.get("p75", 0)
            iqr = q3 - q1
            
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outlier_count = df.filter(
                (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
            ).count()
            
            return {
                "method": "IQR",
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "outlier_count": outlier_count,
                "outlier_percentage": (outlier_count / df.count()) * 100 if df.count() > 0 else 0
            }
        except Exception:
            return {"method": "IQR", "error": "Unable to calculate outliers"}
    
    def _analyze_string_patterns(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Analyze string patterns and formats"""
        try:
            # Sample data for pattern analysis
            sample_df = df.select(column_name).filter(col(column_name).isNotNull()).sample(0.1)
            sample_values = [row[0] for row in sample_df.collect() if row[0]]
            
            if not sample_values:
                return {}
            
            patterns = {
                "email_pattern": 0,
                "phone_pattern": 0,
                "numeric_pattern": 0,
                "alphanumeric_pattern": 0,
                "special_chars_pattern": 0,
                "whitespace_issues": 0
            }
            
            for value in sample_values:
                value_str = str(value)
                
                # Email pattern
                if "@" in value_str and "." in value_str:
                    patterns["email_pattern"] += 1
                
                # Phone pattern (basic)
                if any(char.isdigit() for char in value_str) and len(value_str) >= 10:
                    patterns["phone_pattern"] += 1
                
                # Numeric pattern
                if value_str.isdigit():
                    patterns["numeric_pattern"] += 1
                
                # Alphanumeric pattern
                if value_str.isalnum():
                    patterns["alphanumeric_pattern"] += 1
                
                # Special characters
                if any(not char.isalnum() and not char.isspace() for char in value_str):
                    patterns["special_chars_pattern"] += 1
                
                # Whitespace issues
                if value_str != value_str.strip():
                    patterns["whitespace_issues"] += 1
            
            # Convert to percentages
            total = len(sample_values)
            for key in patterns:
                patterns[key] = (patterns[key] / total) * 100 if total > 0 else 0
                
            return patterns
        except Exception:
            return {}
    
    def _get_top_values(self, df: DataFrame, column_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top values and their frequencies"""
        try:
            top_values = df.groupBy(column_name).count().orderBy(col("count").desc()).limit(limit)
            return [{"value": row[0], "count": row[1]} for row in top_values.collect()]
        except Exception:
            return []
    
    def _identify_numeric_issues(self, df: DataFrame, column_name: str, stats: Any, percentiles: Dict[str, float]) -> List[str]:
        """Identify data quality issues in numeric columns"""
        issues = []
        
        # High null percentage
        null_percentage = (stats["null_count"] / df.count()) * 100
        if null_percentage > 50:
            issues.append(f"High null percentage: {null_percentage:.1f}%")
        elif null_percentage > 20:
            issues.append(f"Moderate null percentage: {null_percentage:.1f}%")
        
        # Zero variance (constant values)
        if stats["variance"] == 0:
            issues.append("Zero variance - all values are identical")
        
        # High outlier percentage
        outliers = self._detect_numeric_outliers(df, column_name, percentiles)
        if outliers.get("outlier_percentage", 0) > 10:
            issues.append(f"High outlier percentage: {outliers.get('outlier_percentage', 0):.1f}%")
        
        return issues
    
    def _identify_string_issues(self, df: DataFrame, column_name: str, patterns: Dict[str, Any]) -> List[str]:
        """Identify data quality issues in string columns"""
        issues = []
        
        # Inconsistent patterns
        if patterns.get("email_pattern", 0) > 0 and patterns.get("email_pattern", 0) < 100:
            issues.append("Mixed data types - contains both email and non-email values")
        
        # Whitespace issues
        if patterns.get("whitespace_issues", 0) > 10:
            issues.append("Significant whitespace issues detected")
        
        # High special character usage
        if patterns.get("special_chars_pattern", 0) > 80:
            issues.append("High special character usage - may need standardization")
        
        return issues
    
    def _identify_datetime_issues(self, df: DataFrame, column_name: str) -> List[str]:
        """Identify data quality issues in datetime columns"""
        issues = []
        
        # Check for null values
        null_count = df.filter(col(column_name).isNull()).count()
        null_percentage = (null_count / df.count()) * 100
        
        if null_percentage > 20:
            issues.append(f"High null percentage in datetime column: {null_percentage:.1f}%")
        
        return issues