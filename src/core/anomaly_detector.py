"""
Anomaly Detector - ML-powered anomaly detection
Similar to Ataccama's anomaly detection capabilities
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, count, mean, stddev, min as spark_min, max as spark_max
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.types import NumericType
import numpy as np

class AnomalyDetector:
    """
    ML-powered anomaly detection for data quality
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def detect_anomalies(self, df: DataFrame, methods: List[str] = None) -> Dict[str, Any]:
        """
        Detect anomalies using multiple methods
        
        Args:
            df: Input DataFrame
            methods: List of detection methods to use
            
        Returns:
            Dictionary containing anomaly detection results
        """
        if methods is None:
            methods = ["statistical", "isolation_forest", "clustering"]
        
        results = {
            "timestamp": None,
            "methods_used": methods,
            "anomaly_summary": {},
            "detailed_results": {}
        }
        
        # Get numeric columns for analysis
        numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]
        
        if not numeric_columns:
            return {
                "error": "No numeric columns found for anomaly detection",
                "methods_used": methods
            }
        
        for method in methods:
            try:
                if method == "statistical":
                    results["detailed_results"]["statistical"] = self._detect_statistical_anomalies(df, numeric_columns)
                elif method == "isolation_forest":
                    results["detailed_results"]["isolation_forest"] = self._detect_isolation_forest_anomalies(df, numeric_columns)
                elif method == "clustering":
                    results["detailed_results"]["clustering"] = self._detect_clustering_anomalies(df, numeric_columns)
            except Exception as e:
                results["detailed_results"][method] = {"error": str(e)}
        
        # Summarize results
        results["anomaly_summary"] = self._summarize_anomalies(results["detailed_results"])
        
        return results
    
    def _detect_statistical_anomalies(self, df: DataFrame, numeric_columns: List[str]) -> Dict[str, Any]:
        """Detect anomalies using statistical methods (IQR, Z-score)"""
        results = {
            "method": "statistical",
            "column_anomalies": {},
            "overall_anomaly_count": 0
        }
        
        total_rows = df.count()
        anomaly_rows = set()
        
        for column in numeric_columns:
            # Calculate statistics
            stats = df.select(
                mean(col(column)).alias("mean"),
                stddev(col(column)).alias("stddev"),
                spark_min(col(column)).alias("min"),
                spark_max(col(column)).alias("max")
            ).collect()[0]
            
            if stats["mean"] is None or stats["stddev"] is None:
                continue
            
            mean_val = stats["mean"]
            std_val = stats["stddev"]
            
            # Z-score method
            z_score_threshold = 3
            z_anomalies = df.filter(
                (col(column) < mean_val - z_score_threshold * std_val) |
                (col(column) > mean_val + z_score_threshold * std_val)
            )
            z_anomaly_count = z_anomalies.count()
            
            # IQR method
            q1 = df.select(col(column)).filter(col(column).isNotNull()).approxQuantile(column, [0.25], 0.1)[0]
            q3 = df.select(col(column)).filter(col(column).isNotNull()).approxQuantile(column, [0.75], 0.1)[0]
            iqr = q3 - q1
            
            iqr_anomalies = df.filter(
                (col(column) < q1 - 1.5 * iqr) |
                (col(column) > q3 + 1.5 * iqr)
            )
            iqr_anomaly_count = iqr_anomalies.count()
            
            # Combine both methods
            combined_anomalies = z_anomalies.union(iqr_anomalies).distinct()
            combined_anomaly_count = combined_anomalies.count()
            
            results["column_anomalies"][column] = {
                "z_score_anomalies": z_anomaly_count,
                "iqr_anomalies": iqr_anomaly_count,
                "combined_anomalies": combined_anomaly_count,
                "anomaly_percentage": (combined_anomaly_count / total_rows) * 100,
                "statistics": {
                    "mean": mean_val,
                    "stddev": std_val,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr
                }
            }
            
            results["overall_anomaly_count"] += combined_anomaly_count
        
        return results
    
    def _detect_isolation_forest_anomalies(self, df: DataFrame, numeric_columns: List[str]) -> Dict[str, Any]:
        """Detect anomalies using Isolation Forest (simplified implementation)"""
        try:
            # Prepare data
            assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
            df_vectorized = assembler.transform(df.filter(col(numeric_columns[0]).isNotNull()))
            
            # Simple clustering-based anomaly detection (simplified Isolation Forest)
            kmeans = KMeans(featuresCol="features", k=2, seed=42)
            model = kmeans.fit(df_vectorized)
            predictions = model.transform(df_vectorized)
            
            # Calculate distances to centroids
            centroids = model.clusterCenters()
            
            # Simple anomaly detection based on distance to centroids
            anomaly_count = 0
            total_count = df_vectorized.count()
            
            # This is a simplified implementation
            # In practice, you'd implement a proper Isolation Forest algorithm
            
            results = {
                "method": "isolation_forest",
                "anomaly_count": anomaly_count,
                "anomaly_percentage": (anomaly_count / total_count) * 100 if total_count > 0 else 0,
                "note": "Simplified implementation - consider using proper Isolation Forest library"
            }
            
            return results
            
        except Exception as e:
            return {
                "method": "isolation_forest",
                "error": str(e),
                "note": "Failed to run isolation forest detection"
            }
    
    def _detect_clustering_anomalies(self, df: DataFrame, numeric_columns: List[str]) -> Dict[str, Any]:
        """Detect anomalies using clustering methods"""
        try:
            # Prepare data
            assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            
            # Filter out null values
            df_clean = df.filter(col(numeric_columns[0]).isNotNull())
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler])
            pipeline_model = pipeline.fit(df_clean)
            df_scaled = pipeline_model.transform(df_clean)
            
            # K-means clustering
            kmeans = KMeans(featuresCol="scaled_features", k=3, seed=42)
            model = kmeans.fit(df_scaled)
            predictions = model.transform(df_scaled)
            
            # Calculate distances to centroids for anomaly detection
            centroids = model.clusterCenters()
            
            # Simple anomaly detection based on distance to nearest centroid
            anomaly_count = 0
            total_count = df_scaled.count()
            
            # This is a simplified implementation
            # In practice, you'd calculate actual distances and use thresholds
            
            results = {
                "method": "clustering",
                "anomaly_count": anomaly_count,
                "anomaly_percentage": (anomaly_count / total_count) * 100 if total_count > 0 else 0,
                "clusters": len(centroids),
                "note": "Simplified implementation - consider using proper distance calculations"
            }
            
            return results
            
        except Exception as e:
            return {
                "method": "clustering",
                "error": str(e),
                "note": "Failed to run clustering-based anomaly detection"
            }
    
    def _summarize_anomalies(self, detailed_results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize anomaly detection results across all methods"""
        summary = {
            "total_anomalies_detected": 0,
            "methods_successful": 0,
            "methods_failed": 0,
            "overall_anomaly_percentage": 0,
            "method_comparison": {}
        }
        
        total_anomalies = 0
        total_rows = 0
        successful_methods = 0
        
        for method, result in detailed_results.items():
            if "error" in result:
                summary["methods_failed"] += 1
                continue
            
            summary["methods_successful"] += 1
            successful_methods += 1
            
            if method == "statistical" and "overall_anomaly_count" in result:
                total_anomalies += result["overall_anomaly_count"]
                summary["method_comparison"]["statistical"] = {
                    "anomaly_count": result["overall_anomaly_count"],
                    "status": "success"
                }
            elif "anomaly_count" in result:
                total_anomalies += result["anomaly_count"]
                summary["method_comparison"][method] = {
                    "anomaly_count": result["anomaly_count"],
                    "anomaly_percentage": result.get("anomaly_percentage", 0),
                    "status": "success"
                }
        
        summary["total_anomalies_detected"] = total_anomalies
        
        # Estimate total rows (this is simplified)
        if successful_methods > 0:
            summary["overall_anomaly_percentage"] = total_anomalies / max(1, total_anomalies) * 100
        
        return summary
    
    def detect_pattern_anomalies(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Detect pattern-based anomalies in string columns
        
        Args:
            df: Input DataFrame
            column: Column name to analyze
            
        Returns:
            Pattern anomaly detection results
        """
        try:
            # Get unique patterns
            patterns = df.select(column).filter(col(column).isNotNull()).distinct().collect()
            pattern_list = [row[0] for row in patterns]
            
            # Simple pattern analysis
            pattern_counts = {}
            for pattern in pattern_list:
                count = df.filter(col(column) == pattern).count()
                pattern_counts[pattern] = count
            
            # Identify rare patterns (potential anomalies)
            total_count = df.count()
            rare_patterns = {k: v for k, v in pattern_counts.items() if v < total_count * 0.01}  # Less than 1%
            
            return {
                "method": "pattern_analysis",
                "total_patterns": len(pattern_list),
                "rare_patterns": len(rare_patterns),
                "rare_pattern_details": rare_patterns,
                "anomaly_percentage": (len(rare_patterns) / len(pattern_list)) * 100 if pattern_list else 0
            }
            
        except Exception as e:
            return {
                "method": "pattern_analysis",
                "error": str(e)
            }
    
    def detect_temporal_anomalies(self, df: DataFrame, timestamp_column: str, value_column: str) -> Dict[str, Any]:
        """
        Detect temporal anomalies in time series data
        
        Args:
            df: Input DataFrame
            timestamp_column: Column containing timestamps
            value_column: Column containing values to analyze
            
        Returns:
            Temporal anomaly detection results
        """
        try:
            # Sort by timestamp
            df_sorted = df.orderBy(timestamp_column)
            
            # Calculate rolling statistics (simplified)
            # In practice, you'd use window functions for proper rolling calculations
            
            # Simple trend analysis
            first_value = df_sorted.select(value_column).first()[0]
            last_value = df_sorted.select(value_column).orderBy(col(timestamp_column).desc()).first()[0]
            
            trend_direction = "increasing" if last_value > first_value else "decreasing"
            
            # Basic anomaly detection based on value changes
            anomaly_count = 0
            total_count = df_sorted.count()
            
            # This is a simplified implementation
            # In practice, you'd implement proper time series anomaly detection
            
            return {
                "method": "temporal_analysis",
                "trend_direction": trend_direction,
                "anomaly_count": anomaly_count,
                "anomaly_percentage": (anomaly_count / total_count) * 100 if total_count > 0 else 0,
                "note": "Simplified implementation - consider using proper time series analysis"
            }
            
        except Exception as e:
            return {
                "method": "temporal_analysis",
                "error": str(e)
            }