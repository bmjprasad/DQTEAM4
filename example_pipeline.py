
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from data_quality_pipeline import DataQualityPipeline

def run_data_quality_checks():
    """Example pipeline integration"""
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()
    
    try:
        # Load data
        print("Loading data...")
        users_df = spark.read.table("users")
        orders_df = spark.read.table("orders")
        
        # Initialize data quality pipeline
        dq_pipeline = DataQualityPipeline(spark)
        
        # Execute data quality rules
        print("Executing data quality rules...")
        
        # Example: Check email format validation
        email_result = dq_pipeline.execute_rule(users_df, validate_email_format)
        if email_result and email_result['accuracy_percentage'] < 95:
            print(f"WARNING: Email format accuracy is {email_result['accuracy_percentage']}%")
        
        # Example: Check null values in critical fields
        age_result = dq_pipeline.execute_rule(users_df, check_age_completeness)
        if age_result and age_result['null_percentage'] > 10:
            print(f"WARNING: Age field has {age_result['null_percentage']}% null values")
        
        # Example: Check foreign key integrity
        fk_result = dq_pipeline.execute_rule(orders_df, check_user_id_foreign_key_integrity, users_df)
        if fk_result and fk_result['invalid_fk_count'] > 0:
            print(f"ERROR: Found {fk_result['invalid_fk_count']} invalid foreign key references")
        
        # Generate comprehensive report
        report = dq_pipeline.generate_report()
        print("\nData Quality Report:")
        print(report)
        
        # Save results to monitoring system
        results_df = spark.createDataFrame(dq_pipeline.get_all_results())
        results_df.write.mode("overwrite").saveAsTable("data_quality_results")
        
        print("\nData quality check completed successfully!")
        
    except Exception as e:
        print(f"Error in data quality pipeline: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    run_data_quality_checks()
    