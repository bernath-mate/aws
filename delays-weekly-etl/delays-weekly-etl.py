import sys
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

# Read Athena tables
df_initial = glueContext.create_dynamic_frame.from_catalog(
    database="mav_delays_weather", 
    table_name="delays_initial"
).toDF()

df_weekly = glueContext.create_dynamic_frame.from_catalog(
    database="mav_delays_weather", 
    table_name="delays_last_week"  # Your recreated CSV table
).toDF()

# Union and write (OVERWRITE mode)
df_all = df_initial.union(df_weekly)

# Write to Athena-compatible Parquet location
df_all.write \
    .mode("overwrite") \
    .parquet("s3://mav-delays-weather-slucrx/processed-data/delays_all/")

# Update Glue catalog (optional - MSCK REPAIR via Athena)
glueContext.get_logger().info("ETL completed - run MSCK REPAIR TABLE delays_all in Athena")

job.commit()