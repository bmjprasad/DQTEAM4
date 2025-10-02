"""
Data Validator - Rule-based data validation engine
Similar to Ataccama's validation capabilities
"""

from typing import Dict, List, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, length, 
    trim, upper, lower, regexp_extract, count, sum as spark_sum,
    min as spark_min, max as spark_max, mean, stddev
)
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType
import re
import json
from datetime import datetime

class DataValidator:
    """
    Rule-based data validator with comprehensive validation capabilities
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def validate_dataframe(self, df: DataFrame, rules_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate DataFrame against a set of rules
        
        Args:
            df: Input DataFrame
            rules_config: Validation rules configuration
            
        Returns:
            Validation results with pass/fail status and details
        """
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "dataset_info": {
                "row_count": df.count(),
                "column_count": len(df.columns)
            },
            "rules": [],
            "summary": {
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "overall_status": "PASS"
            }
        }
        
        # Process each rule
        for rule_name, rule_config in rules_config.items():
            rule_result = self._validate_rule(df, rule_name, rule_config)
            validation_results["rules"].append(rule_result)
            
            if rule_result["status"] == "PASS":
                validation_results["summary"]["passed_rules"] += 1
            else:
                validation_results["summary"]["failed_rules"] += 1
                
            validation_results["summary"]["total_rules"] += 1
        
        # Determine overall status
        if validation_results["summary"]["failed_rules"] > 0:
            validation_results["summary"]["overall_status"] = "FAIL"
            
        return validation_results
    
    def _validate_rule(self, df: DataFrame, rule_name: str, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single rule
        
        Args:
            df: Input DataFrame
            rule_name: Name of the rule
            rule_config: Rule configuration
            
        Returns:
            Rule validation result
        """
        rule_type = rule_config.get("type", "unknown")
        
        try:
            if rule_type == "not_null":
                result = self._validate_not_null(df, rule_config)
            elif rule_type == "unique":
                result = self._validate_unique(df, rule_config)
            elif rule_type == "range":
                result = self._validate_range(df, rule_config)
            elif rule_type == "pattern":
                result = self._validate_pattern(df, rule_config)
            elif rule_type == "length":
                result = self._validate_length(df, rule_config)
            elif rule_type == "data_type":
                result = self._validate_data_type(df, rule_config)
            elif rule_type == "custom_sql":
                result = self._validate_custom_sql(df, rule_config)
            elif rule_type == "referential_integrity":
                result = self._validate_referential_integrity(df, rule_config)
            elif rule_type == "business_rule":
                result = self._validate_business_rule(df, rule_config)
            else:
                result = {
                    "status": "ERROR",
                    "message": f"Unknown rule type: {rule_type}",
                    "violation_count": 0
                }
                
            result["rule_name"] = rule_name
            result["rule_type"] = rule_type
            result["timestamp"] = datetime.now().isoformat()
            
            return result
            
        except Exception as e:
            return {
                "rule_name": rule_name,
                "rule_type": rule_type,
                "status": "ERROR",
                "message": f"Validation error: {str(e)}",
                "violation_count": 0,
                "timestamp": datetime.now().isoformat()
            }
    
    def _validate_not_null(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate not null constraint"""
        columns = rule_config.get("columns", [])
        violation_count = 0
        violations = []
        
        for column in columns:
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                violation_count += null_count
                
                if null_count > 0:
                    violations.append({
                        "column": column,
                        "null_count": null_count,
                        "null_percentage": (null_count / df.count()) * 100
                    })
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} null values in specified columns"
        }
    
    def _validate_unique(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate uniqueness constraint"""
        columns = rule_config.get("columns", [])
        violation_count = 0
        violations = []
        
        if columns:
            # Check for duplicate combinations
            total_rows = df.count()
            unique_rows = df.select(*columns).dropDuplicates().count()
            duplicate_count = total_rows - unique_rows
            violation_count = duplicate_count
            
            if duplicate_count > 0:
                violations.append({
                    "columns": columns,
                    "duplicate_count": duplicate_count,
                    "duplicate_percentage": (duplicate_count / total_rows) * 100
                })
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} duplicate rows"
        }
    
    def _validate_range(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate range constraints"""
        column = rule_config.get("column")
        min_value = rule_config.get("min_value")
        max_value = rule_config.get("max_value")
        violation_count = 0
        violations = []
        
        if column and column in df.columns:
            # Build range condition
            condition = col(column).isNotNull()
            
            if min_value is not None:
                condition = condition & (col(column) >= min_value)
            if max_value is not None:
                condition = condition & (col(column) <= max_value)
            
            # Count violations
            violation_count = df.filter(~condition).count()
            
            if violation_count > 0:
                violations.append({
                    "column": column,
                    "min_value": min_value,
                    "max_value": max_value,
                    "violation_count": violation_count,
                    "violation_percentage": (violation_count / df.count()) * 100
                })
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} values outside range [{min_value}, {max_value}]"
        }
    
    def _validate_pattern(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate pattern/regex constraints"""
        column = rule_config.get("column")
        pattern = rule_config.get("pattern")
        violation_count = 0
        violations = []
        
        if column and column in df.columns and pattern:
            try:
                # Count non-matching values
                violation_count = df.filter(
                    col(column).isNotNull() & 
                    ~col(column).rlike(pattern)
                ).count()
                
                if violation_count > 0:
                    violations.append({
                        "column": column,
                        "pattern": pattern,
                        "violation_count": violation_count,
                        "violation_percentage": (violation_count / df.count()) * 100
                    })
            except Exception as e:
                return {
                    "status": "ERROR",
                    "violation_count": 0,
                    "message": f"Pattern validation error: {str(e)}"
                }
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} values not matching pattern: {pattern}"
        }
    
    def _validate_length(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate string length constraints"""
        column = rule_config.get("column")
        min_length = rule_config.get("min_length")
        max_length = rule_config.get("max_length")
        violation_count = 0
        violations = []
        
        if column and column in df.columns:
            # Build length condition
            condition = col(column).isNotNull()
            
            if min_length is not None:
                condition = condition & (length(col(column)) >= min_length)
            if max_length is not None:
                condition = condition & (length(col(column)) <= max_length)
            
            # Count violations
            violation_count = df.filter(~condition).count()
            
            if violation_count > 0:
                violations.append({
                    "column": column,
                    "min_length": min_length,
                    "max_length": max_length,
                    "violation_count": violation_count,
                    "violation_percentage": (violation_count / df.count()) * 100
                })
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} values with invalid length"
        }
    
    def _validate_data_type(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data type constraints"""
        column = rule_config.get("column")
        expected_type = rule_config.get("expected_type")
        violation_count = 0
        violations = []
        
        if column and column in df.columns and expected_type:
            try:
                # Try to cast to expected type and count failures
                if expected_type == "numeric":
                    violation_count = df.filter(
                        col(column).isNotNull() & 
                        ~col(column).cast("double").isNotNull()
                    ).count()
                elif expected_type == "integer":
                    violation_count = df.filter(
                        col(column).isNotNull() & 
                        ~col(column).cast("int").isNotNull()
                    ).count()
                elif expected_type == "date":
                    violation_count = df.filter(
                        col(column).isNotNull() & 
                        ~col(column).cast("date").isNotNull()
                    ).count()
                
                if violation_count > 0:
                    violations.append({
                        "column": column,
                        "expected_type": expected_type,
                        "violation_count": violation_count,
                        "violation_percentage": (violation_count / df.count()) * 100
                    })
            except Exception as e:
                return {
                    "status": "ERROR",
                    "violation_count": 0,
                    "message": f"Data type validation error: {str(e)}"
                }
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} values with invalid data type"
        }
    
    def _validate_custom_sql(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate custom SQL conditions"""
        sql_condition = rule_config.get("sql_condition")
        violation_count = 0
        violations = []
        
        if sql_condition:
            try:
                # Create temporary view
                df.createOrReplaceTempView("validation_table")
                
                # Execute custom SQL
                violation_count = self.spark.sql(f"SELECT COUNT(*) FROM validation_table WHERE NOT ({sql_condition})").collect()[0][0]
                
                if violation_count > 0:
                    violations.append({
                        "sql_condition": sql_condition,
                        "violation_count": violation_count,
                        "violation_percentage": (violation_count / df.count()) * 100
                    })
            except Exception as e:
                return {
                    "status": "ERROR",
                    "violation_count": 0,
                    "message": f"Custom SQL validation error: {str(e)}"
                }
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} rows violating custom SQL condition"
        }
    
    def _validate_referential_integrity(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate referential integrity constraints"""
        column = rule_config.get("column")
        reference_table = rule_config.get("reference_table")
        reference_column = rule_config.get("reference_column")
        violation_count = 0
        violations = []
        
        if column and reference_table and reference_column:
            try:
                # Get reference values
                ref_df = self.spark.table(reference_table)
                reference_values = set([row[0] for row in ref_df.select(reference_column).distinct().collect()])
                
                # Check for foreign key violations
                df_values = set([row[0] for row in df.select(column).distinct().collect() if row[0] is not None])
                invalid_values = df_values - reference_values
                
                if invalid_values:
                    violation_count = df.filter(col(column).isin(list(invalid_values))).count()
                    violations.append({
                        "column": column,
                        "reference_table": reference_table,
                        "reference_column": reference_column,
                        "invalid_values": list(invalid_values),
                        "violation_count": violation_count,
                        "violation_percentage": (violation_count / df.count()) * 100
                    })
            except Exception as e:
                return {
                    "status": "ERROR",
                    "violation_count": 0,
                    "message": f"Referential integrity validation error: {str(e)}"
                }
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} referential integrity violations"
        }
    
    def _validate_business_rule(self, df: DataFrame, rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate custom business rules"""
        rule_expression = rule_config.get("rule_expression")
        violation_count = 0
        violations = []
        
        if rule_expression:
            try:
                # Parse and execute business rule
                # This is a simplified implementation - in practice, you'd want a more robust rule engine
                violation_count = df.filter(~eval(rule_expression)).count()
                
                if violation_count > 0:
                    violations.append({
                        "rule_expression": rule_expression,
                        "violation_count": violation_count,
                        "violation_percentage": (violation_count / df.count()) * 100
                    })
            except Exception as e:
                return {
                    "status": "ERROR",
                    "violation_count": 0,
                    "message": f"Business rule validation error: {str(e)}"
                }
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "violation_count": violation_count,
            "violations": violations,
            "message": f"Found {violation_count} business rule violations"
        }