import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

athena_client = boto3.client('athena', region_name='us-east-1')

try:
    print("starting etl job for weather_all table refresh")
    
    # Read weather_initial table
    try:
        print("reading weather_initial table from glue catalog")
        df_initial = glueContext.create_dynamic_frame.from_catalog(
            database="mav_delays_weather", 
            table_name="weather_initial"
        ).toDF()
        print(f"successfully loaded weather_initial: {df_initial.count()} rows")
    except Exception as e:
        print(f"failed to read weather_initial table: {str(e)}")
        raise
    
    # Read weather_last_week table
    try:
        print("reading weather_last_week table from glue catalog")
        df_last_week = glueContext.create_dynamic_frame.from_catalog(
            database="mav_delays_weather", 
            table_name="weather_last_week"
        ).toDF()
        print(f"successfully loaded weather_last_week: {df_last_week.count()} rows")
    except Exception as e:
        print(f"failed to read weather_last_week table: {str(e)}")
        raise
    
    # Union tables
    try:
        print("unioning weather_initial and weather_last_week tables")
        df_all = df_initial.union(df_last_week)
        print(f"successfully created union: {df_all.count()} total rows")
    except Exception as e:
        print(f"failed to union tables: {str(e)}")
        raise
    
    # Define schema and cast
    try:
        print("applying athena-compatible schema and casting columns")
        df_final = df_all.select(
            col("date").cast("date").alias("date"),
            col("station_name").cast("string").alias("station_name"),
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
            col("temperature_mean_c").cast("double").alias("temperature_mean_c"),
            col("wind_gust_max_kmh").cast("double").alias("wind_gust_max_kmh"),
            col("precipitation_sum_mm").cast("double").alias("precipitation_sum_mm"),
            col("extreme_temperature").cast("int").alias("extreme_temperature"),
            col("extreme_wind").cast("int").alias("extreme_wind"),
            col("extreme_precipitation").cast("int").alias("extreme_precipitation")
        ).coalesce(1)
        print("schema casting completed successfully")
    except Exception as e:
        print(f"failed to apply schema and cast columns: {str(e)}")
        raise
    
    # Write to S3 with Athena-compatible settings
    try:
        print("writing parquet data to s3://mav-delays-weather-slucrx/processed-data/weather_all/")
        df_final.write \
            .mode("overwrite") \
            .option("parquet.enable.dictionary", "false") \
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .parquet("s3://mav-delays-weather-slucrx/processed-data/weather_all/")
        print("successfully wrote parquet files to s3")
    except Exception as e:
        print(f"failed to write parquet data to s3: {str(e)}")
        raise
    
    # Run MSCK REPAIR to refresh Athena metadata
    try:
        print("running msck repair table to refresh athena metadata")
        repair_query = "MSCK REPAIR TABLE weather_all"
        response = athena_client.start_query_execution(
            QueryString=repair_query,
            QueryExecutionContext={'Database': 'mav_delays_weather'},
            ResultConfiguration={'OutputLocation': 's3://mav-delays-weather-slucrx/query-results/'}
        )
        print(f"msck repair query started: {response['QueryExecutionId']}")
    except Exception as e:
        print(f"failed to run msck repair: {str(e)}")
    
    # ===== NEW SECTION: CREATE/REPLACE UNIFIED_ALL VIEW =====
    
    print("\n=== starting unified_all view refresh ===")
    
    try:
        print("creating or replacing unified_all view")
        unified_view_query = """
        CREATE OR REPLACE VIEW unified_all AS
        SELECT 
            'delay' as data_type,
            date,
            station_name,
            NULL as extreme_temperature,
            NULL as extreme_wind,
            NULL as extreme_precipitation,
            total_delay_minutes,
            train_count,
            avg_delay_minutes
        FROM delays_all
        UNION ALL
        SELECT 
            'weather' as data_type,
            date,
            station_name,
            extreme_temperature,
            extreme_wind,
            extreme_precipitation,
            NULL as total_delay_minutes,
            NULL as train_count,
            NULL as avg_delay_minutes
        FROM weather_all
        """
        
        response = athena_client.start_query_execution(
            QueryString=unified_view_query,
            QueryExecutionContext={'Database': 'mav_delays_weather'},
            ResultConfiguration={'OutputLocation': 's3://mav-delays-weather-slucrx/query-results/'}
        )
        print(f"unified_all view creation query started: {response['QueryExecutionId']}")
    except Exception as e:
        print(f"failed to create unified_all view: {str(e)}")
    
    # Commit job
    try:
        print("committing glue job")
        job.commit()
        print("etl job completed successfully")
    except Exception as e:
        print(f"failed to commit job: {str(e)}")
        raise

except Exception as e:
    print(f"etl job failed with error: {str(e)}")
    job.commit()
    sys.exit(1)