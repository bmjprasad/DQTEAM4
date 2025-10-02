"""
Quality Metrics - Data quality scoring and metrics calculation
Similar to Ataccama's quality metrics capabilities
"""

from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, isnan, isnull, 
    sum as spark_sum, mean, stddev, min as spark_min, max as spark_max
)
import math

class QualityMetrics:
    """
    Data quality metrics calculation and scoring system
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def calculate_basic_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Calculate basic data quality metrics
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary containing basic quality metrics
        """
        total_rows = df.count()
        total_columns = len(df.columns)
        
        # Calculate completeness metrics
        completeness_metrics = self._calculate_completeness(df, total_rows)
        
        # Calculate consistency metrics
        consistency_metrics = self._calculate_consistency(df)
        
        # Calculate accuracy metrics
        accuracy_metrics = self._calculate_accuracy(df)
        
        # Calculate validity metrics
        validity_metrics = self._calculate_validity(df)
        
        # Calculate uniqueness metrics
        uniqueness_metrics = self._calculate_uniqueness(df)
        
        return {
            "dataset_size": {
                "total_rows": total_rows,
                "total_columns": total_columns
            },
            "completeness": completeness_metrics,
            "consistency": consistency_metrics,
            "accuracy": accuracy_metrics,
            "validity": validity_metrics,
            "uniqueness": uniqueness_metrics,
            "overall_quality_score": self._calculate_overall_score(
                completeness_metrics, consistency_metrics, 
                accuracy_metrics, validity_metrics, uniqueness_metrics
            )
        }
    
    def calculate_quality_score(self, df: DataFrame, validation_results: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Calculate comprehensive quality score
        
        Args:
            df: Input DataFrame
            validation_results: Optional validation results
            
        Returns:
            Quality score breakdown
        """
        basic_metrics = self.calculate_basic_metrics(df)
        
        # Add validation-based metrics if available
        if validation_results:
            validation_score = self._calculate_validation_score(validation_results)
            basic_metrics["validation_score"] = validation_score
            
            # Recalculate overall score with validation
            basic_metrics["overall_quality_score"] = self._calculate_overall_score_with_validation(
                basic_metrics, validation_score
            )
        
        return basic_metrics
    
    def _calculate_completeness(self, df: DataFrame, total_rows: int) -> Dict[str, Any]:
        """Calculate completeness metrics"""
        if total_rows == 0:
            return {"score": 0, "details": {}}
        
        completeness_details = {}
        total_null_cells = 0
        total_cells = total_rows * len(df.columns)
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_percentage = (null_count / total_rows) * 100
            completeness_percentage = 100 - null_percentage
            
            completeness_details[column] = {
                "null_count": null_count,
                "null_percentage": null_percentage,
                "completeness_percentage": completeness_percentage
            }
            
            total_null_cells += null_count
        
        overall_completeness = ((total_cells - total_null_cells) / total_cells) * 100
        
        return {
            "score": overall_completeness,
            "details": completeness_details,
            "total_null_cells": total_null_cells,
            "total_cells": total_cells
        }
    
    def _calculate_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate consistency metrics"""
        consistency_details = {}
        total_inconsistencies = 0
        
        for column in df.columns:
            field = df.schema[column]
            
            # Check data type consistency
            type_inconsistencies = 0
            if str(field.dataType) == "StringType":
                # Check for mixed data types in string columns
                try:
                    numeric_count = df.filter(col(column).rlike("^[0-9]+$")).count()
                    total_count = df.filter(col(column).isNotNull()).count()
                    if total_count > 0:
                        type_inconsistencies = abs(numeric_count - total_count) / total_count
                except:
                    type_inconsistencies = 0
            
            # Check format consistency (basic)
            format_inconsistencies = 0
            if str(field.dataType) == "StringType":
                # Check for consistent case
                try:
                    upper_count = df.filter(col(column) == col(column).upper()).count()
                    lower_count = df.filter(col(column) == col(column).lower()).count()
                    total_count = df.filter(col(column).isNotNull()).count()
                    if total_count > 0:
                        case_consistency = max(upper_count, lower_count) / total_count
                        format_inconsistencies = 1 - case_consistency
                except:
                    format_inconsistencies = 0
            
            column_inconsistencies = (type_inconsistencies + format_inconsistencies) / 2
            total_inconsistencies += column_inconsistencies
            
            consistency_details[column] = {
                "type_consistency": 1 - type_inconsistencies,
                "format_consistency": 1 - format_inconsistencies,
                "overall_consistency": 1 - column_inconsistencies
            }
        
        overall_consistency = 1 - (total_inconsistencies / len(df.columns)) if df.columns else 1
        overall_consistency = max(0, min(100, overall_consistency * 100))
        
        return {
            "score": overall_consistency,
            "details": consistency_details
        }
    
    def _calculate_accuracy(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate accuracy metrics (simplified)"""
        accuracy_details = {}
        total_accuracy = 0
        
        for column in df.columns:
            field = df.schema[column]
            
            # Basic accuracy checks
            accuracy_score = 100  # Start with perfect score
            
            # Check for obviously incorrect values
            if str(field.dataType) in ["IntegerType", "LongType", "DoubleType", "FloatType"]:
                # Check for negative values where they shouldn't exist
                negative_count = df.filter(col(column) < 0).count()
                total_count = df.filter(col(column).isNotNull()).count()
                if total_count > 0:
                    negative_penalty = (negative_count / total_count) * 50  # Penalty for negative values
                    accuracy_score -= negative_penalty
            
            # Check for extreme outliers (simplified)
            if str(field.dataType) in ["DoubleType", "FloatType"]:
                try:
                    stats = df.select(
                        mean(col(column)).alias("mean"),
                        stddev(col(column)).alias("stddev")
                    ).collect()[0]
                    
                    if stats["mean"] is not None and stats["stddev"] is not None:
                        # Count values beyond 3 standard deviations
                        outlier_count = df.filter(
                            (col(column) < stats["mean"] - 3 * stats["stddev"]) |
                            (col(column) > stats["mean"] + 3 * stats["stddev"])
                        ).count()
                        
                        if total_count > 0:
                            outlier_penalty = (outlier_count / total_count) * 30
                            accuracy_score -= outlier_penalty
                except:
                    pass
            
            accuracy_score = max(0, min(100, accuracy_score))
            total_accuracy += accuracy_score
            
            accuracy_details[column] = {
                "score": accuracy_score
            }
        
        overall_accuracy = total_accuracy / len(df.columns) if df.columns else 100
        
        return {
            "score": overall_accuracy,
            "details": accuracy_details
        }
    
    def _calculate_validity(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate validity metrics"""
        validity_details = {}
        total_validity = 0
        
        for column in df.columns:
            field = df.schema[column]
            total_count = df.filter(col(column).isNotNull()).count()
            
            if total_count == 0:
                validity_details[column] = {"score": 100}
                total_validity += 100
                continue
            
            # Basic validity checks
            validity_score = 100
            
            # Check for invalid data types
            try:
                if str(field.dataType) == "StringType":
                    # Check for empty strings
                    empty_count = df.filter(col(column) == "").count()
                    empty_penalty = (empty_count / total_count) * 20
                    validity_score -= empty_penalty
                    
                    # Check for whitespace-only strings
                    whitespace_count = df.filter(col(column).rlike("^\\s+$")).count()
                    whitespace_penalty = (whitespace_count / total_count) * 10
                    validity_score -= whitespace_penalty
                    
            except:
                pass
            
            validity_score = max(0, min(100, validity_score))
            total_validity += validity_score
            
            validity_details[column] = {
                "score": validity_score
            }
        
        overall_validity = total_validity / len(df.columns) if df.columns else 100
        
        return {
            "score": overall_validity,
            "details": validity_details
        }
    
    def _calculate_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate uniqueness metrics"""
        uniqueness_details = {}
        total_uniqueness = 0
        
        for column in df.columns:
            total_count = df.count()
            distinct_count = df.select(column).distinct().count()
            
            if total_count == 0:
                uniqueness_score = 100
            else:
                uniqueness_score = (distinct_count / total_count) * 100
            
            total_uniqueness += uniqueness_score
            
            uniqueness_details[column] = {
                "distinct_count": distinct_count,
                "total_count": total_count,
                "uniqueness_percentage": uniqueness_score
            }
        
        overall_uniqueness = total_uniqueness / len(df.columns) if df.columns else 100
        
        return {
            "score": overall_uniqueness,
            "details": uniqueness_details
        }
    
    def _calculate_validation_score(self, validation_results: Dict) -> Dict[str, Any]:
        """Calculate score based on validation results"""
        if not validation_results or "summary" not in validation_results:
            return {"score": 100, "details": {}}
        
        summary = validation_results["summary"]
        total_rules = summary.get("total_rules", 0)
        passed_rules = summary.get("passed_rules", 0)
        
        if total_rules == 0:
            return {"score": 100, "details": {}}
        
        validation_score = (passed_rules / total_rules) * 100
        
        return {
            "score": validation_score,
            "details": {
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": summary.get("failed_rules", 0)
            }
        }
    
    def _calculate_overall_score(self, completeness: Dict, consistency: Dict, 
                               accuracy: Dict, validity: Dict, uniqueness: Dict) -> Dict[str, Any]:
        """Calculate overall quality score"""
        scores = {
            "completeness": completeness["score"],
            "consistency": consistency["score"],
            "accuracy": accuracy["score"],
            "validity": validity["score"],
            "uniqueness": uniqueness["score"]
        }
        
        # Weighted average (can be customized)
        weights = {
            "completeness": 0.25,
            "consistency": 0.20,
            "accuracy": 0.25,
            "validity": 0.20,
            "uniqueness": 0.10
        }
        
        weighted_score = sum(scores[metric] * weights[metric] for metric in scores)
        
        # Determine quality grade
        if weighted_score >= 90:
            grade = "A"
        elif weighted_score >= 80:
            grade = "B"
        elif weighted_score >= 70:
            grade = "C"
        elif weighted_score >= 60:
            grade = "D"
        else:
            grade = "F"
        
        return {
            "overall_score": weighted_score,
            "grade": grade,
            "component_scores": scores,
            "weights": weights
        }
    
    def _calculate_overall_score_with_validation(self, basic_metrics: Dict, validation_score: Dict) -> Dict[str, Any]:
        """Calculate overall score including validation results"""
        scores = {
            "completeness": basic_metrics["completeness"]["score"],
            "consistency": basic_metrics["consistency"]["score"],
            "accuracy": basic_metrics["accuracy"]["score"],
            "validity": basic_metrics["validity"]["score"],
            "uniqueness": basic_metrics["uniqueness"]["score"],
            "validation": validation_score["score"]
        }
        
        # Updated weights including validation
        weights = {
            "completeness": 0.20,
            "consistency": 0.15,
            "accuracy": 0.20,
            "validity": 0.15,
            "uniqueness": 0.10,
            "validation": 0.20
        }
        
        weighted_score = sum(scores[metric] * weights[metric] for metric in scores)
        
        # Determine quality grade
        if weighted_score >= 90:
            grade = "A"
        elif weighted_score >= 80:
            grade = "B"
        elif weighted_score >= 70:
            grade = "C"
        elif weighted_score >= 60:
            grade = "D"
        else:
            grade = "F"
        
        return {
            "overall_score": weighted_score,
            "grade": grade,
            "component_scores": scores,
            "weights": weights
        }