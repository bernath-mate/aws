import sys
import boto3
import json
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

args = {}
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("initial-weather-load", {})

BUCKET = 'mav-delays-weather-slucrx'
API_BASE = 'https://archive-api.open-meteo.com/v1/archive'
START_DATE = '2025-06-01'
END_DATE = '2025-11-23'

s3 = boto3.client('s3')

try:
    print("starting initial weather load")
    print(f"date range: {START_DATE} to {END_DATE}")
    
    # STEP 1: download and read station coordinates JSON from S3
    try:
        print("downloading station coordinates from s3")
        response = s3.get_object(
            Bucket=BUCKET,
            Key='raw-data/weather/station_coordinates.json'
        )
        body = response['Body'].read().decode('utf-8')
        stations = json.loads(body)
        print(f"loaded {len(stations)} stations")
        stations_batch_1 = stations[0:900]
        print(f"batch 1: processing stations 0-899: ({len(stations_batch_1)} stations)")
    
    except Exception as e:
        print(f"error downloading station coordinates: {str(e)}")
        raise
    
    # STEP 2: fetch weather for each station
    all_weather = []
    success_count = 0
    error_count = 0
    
    for idx, station in enumerate(stations_batch_1):
        station_name = station['station_name']
        lat = station['lat']
        lon = station['lon']
        
        if (idx + 1) % 100 == 0:
            print(f"processing station {idx + 1}/{len(stations_batch_1)}")
        
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': START_DATE,
                'end_date': END_DATE,
                'daily': 'wind_gusts_10m_max,temperature_2m_mean,precipitation_sum',
                'timezone': 'auto'
            }
            
            response = requests.get(API_BASE, params=params, timeout=420)
            response.raise_for_status()
            data = response.json()
            
            # parse all days in response
            times = data['daily']['time']
            temps = data['daily']['temperature_2m_mean']
            winds = data['daily']['wind_gusts_10m_max']
            precips = data['daily']['precipitation_sum']
            
            for i, date_str in enumerate(times):
                temp = temps[i]
                wind = winds[i]
                precip = precips[i]
                
                # hungarian thresholds
                extreme_temperature = 1 if (temp < -15 or temp > 35) else 0
                extreme_wind = 1 if wind > 60 else 0
                extreme_precipitation = 1 if precip > 20 else 0
                
                record = {
                    'date': date_str,
                    'station_name': station_name,
                    'latitude': float(lat),
                    'longitude': float(lon),
                    'temperature_mean_c': float(temp),
                    'wind_gust_max_kmh': float(wind),
                    'precipitation_sum_mm': float(precip),
                    'extreme_temperature': int(extreme_temperature),
                    'extreme_wind': int(extreme_wind),
                    'extreme_precipitation': int(extreme_precipitation)
                }
                all_weather.append(record)
            
            success_count += 1
        
        except requests.exceptions.RequestException as e:
            print(f"api request failed for {station_name}: {str(e)}")
            error_count += 1
        except Exception as e:
            print(f"error processing station {station_name}: {str(e)}")
            error_count += 1
    
    print(f"api fetch complete: {success_count} successful, {error_count} failed")
    print(f"total records collected: {len(all_weather)}")
    
    if not all_weather:
        raise Exception("no weather data fetched, aborting")
    
    # STEP 3: define schema and create DataFrame with it
    try:
        print("defining schema and creating dataframe")
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("temperature_mean_c", FloatType(), True),
            StructField("wind_gust_max_kmh", FloatType(), True),
            StructField("precipitation_sum_mm", FloatType(), True),
            StructField("extreme_temperature", IntegerType(), True),
            StructField("extreme_wind", IntegerType(), True),
            StructField("extreme_precipitation", IntegerType(), True)
        ])
        
        df_weather = spark.createDataFrame(all_weather, schema=schema)
        print(f"dataframe created with {df_weather.count()} records")
    
    except Exception as e:
        print(f"error creating dataframe: {str(e)}")
        raise
    
    # STEP 4: write to S3 as CSV
    try:
        output_path = "s3://mav-delays-weather-slucrx/raw-data/weather/initial-load/batch-1"
        print(f"writing csv to s3: {output_path}")
        
        df_weather.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"csv write successful: {output_path}")
    
    except Exception as e:
        print(f"error writing csv to s3: {str(e)}")
        raise
    
    # summary statistics
    try:
        print("calculating summary statistics")
        stats = df_weather.groupBy().agg(
            countDistinct("date").alias("unique_dates"),
            countDistinct("station_name").alias("unique_stations"),
            sum("extreme_temperature").alias("extreme_temp_count"),
            sum("extreme_wind").alias("extreme_wind_count"),
            sum("extreme_precipitation").alias("extreme_precip_count")
        ).collect()[0]
        
        print(f"summary: total_records={len(all_weather)}, unique_dates={stats[0]}, unique_stations={stats[1]}, extreme_temps={stats[2]}, extreme_winds={stats[3]}, extreme_precips={stats[4]}")
    
    except Exception as e:
        print(f"error calculating statistics: {str(e)}")
    
    print("initial weather load completed successfully")

except Exception as e:
    print(f"fatal error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()