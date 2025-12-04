import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

try:
    logger.info("starting etl job for delays_all table refresh")
    
    # Read delays_initial table
    try:
        logger.info("reading delays_initial table from glue catalog")
        df_initial = glueContext.create_dynamic_frame.from_catalog(
            database="mav_delays_weather", 
            table_name="delays_initial"
        ).toDF()
        logger.info(f"successfully loaded delays_initial: {df_initial.count()} rows")
    except Exception as e:
        logger.error(f"failed to read delays_initial table: {str(e)}")
        raise
    
    # Read delays_last_week table
    try:
        logger.info("reading delays_last_week table from glue catalog")
        df_weekly = glueContext.create_dynamic_frame.from_catalog(
            database="mav_delays_weather", 
            table_name="delays_last_week"
        ).toDF()
        logger.info(f"successfully loaded delays_last_week: {df_weekly.count()} rows")
    except Exception as e:
        logger.error(f"failed to read delays_last_week table: {str(e)}")
        raise
    
    # Union tables
    try:
        logger.info("unioning delays_initial and delays_last_week tables")
        df_all = df_initial.union(df_weekly)
        logger.info(f"successfully created union: {df_all.count()} total rows")
    except Exception as e:
        logger.error(f"failed to union tables: {str(e)}")
        raise
    
    # Define schema and cast
    try:
        logger.info("applying athena-compatible schema and casting columns")
        df_final = df_all.select(
            col("date").cast("date").alias("date"),
            col("station_name").cast("string").alias("station_name"),
            col("total_delay_minutes").cast("double").alias("total_delay_minutes"),
            col("train_count").cast("int").alias("train_count"),
            col("avg_delay_minutes").cast("double").alias("avg_delay_minutes")
        ).repartition(1)
        logger.info("schema casting completed successfully")
    except Exception as e:
        logger.error(f"failed to apply schema and cast columns: {str(e)}")
        raise
    
    # Write to S3
    try:
        logger.info("writing parquet data to s3://mav-delays-weather-slucrx/processed-data/delays_all/")
        df_final.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("s3://mav-delays-weather-slucrx/processed-data/delays_all/")
        logger.info("successfully wrote parquet files to s3")
    except Exception as e:
        logger.error(f"failed to write parquet data to s3: {str(e)}")
        raise
    
    # Commit job
    try:
        logger.info("committing glue job")
        job.commit()
        logger.info("etl job completed successfully - run 'msck repair table delays_all' in athena to refresh metadata")
    except Exception as e:
        logger.error(f"failed to commit job: {str(e)}")
        raise

except Exception as e:
    logger.error(f"etl job failed with error: {str(e)}")
    job.commit()
    sys.exit(1)