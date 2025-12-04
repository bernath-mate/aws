import sys
import boto3
import json
from datetime import datetime
import csv
import io
import urllib.request
import urllib.parse
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
import time

args = {}
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("daily-weather-update", {})

s3 = boto3.client('s3')
athena_client = boto3.client('athena', region_name='us-east-1')
BUCKET = 'mav-delays-weather-slucrx'
API_BASE = 'https://api.open-meteo.com/v1/forecast'

try:
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"starting daily weather fetch for {today}")
    
    # step 1: download station coordinates
    try:
        print("downloading station coordinates from s3")
        response = s3.get_object(
            Bucket=BUCKET,
            Key='raw-data/weather/station_coordinates.json'
        )
        body = response['Body'].read().decode('utf-8')
        stations = json.loads(body)
        print(f"loaded {len(stations)} stations")
    
    except Exception as e:
        print(f"error downloading station coordinates: {str(e)}")
        raise
    
    # step 2: fetch weather for today
    all_weather = []
    success_count = 0
    error_count = 0
    
    for idx, station in enumerate(stations):
        station_name = station['station_name']
        lat = station['lat']
        lon = station['lon']
        
        if (idx + 1) % 500 == 0:
            print(f"processing station {idx + 1}/{len(stations)}")
        
        # retry logic (3 attempts per station)
        max_retries = 3
        data = None
        for attempt in range(max_retries):
            try:
                params = {
                    'latitude': lat,
                    'longitude': lon,
                    'daily': 'wind_gusts_10m_max,precipitation_sum,temperature_2m_mean',
                    'forecast_days': 1,
                    'timezone': 'auto'
                }
                
                query_string = urllib.parse.urlencode(params)
                url = f'{API_BASE}?{query_string}'
                
                req = urllib.request.Request(url)
                req.add_header('User-Agent', 'mav-weather-etl')
                
                with urllib.request.urlopen(req, timeout=180) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                break  # success, exit retry loop
            
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"retry {attempt+1}/3 for {station_name}")
                    time.sleep(5)
                else:
                    print(f"failed after 3 retries for {station_name}: {str(e)}")
                    error_count += 1
        
        if data is None:
            # skip this station's parsing, as fetching data was unsuccessful
            continue
        
        # parse today's data
        times = data['daily']['time']
        temps = data['daily']['temperature_2m_mean']
        winds = data['daily']['wind_gusts_10m_max']
        precips = data['daily']['precipitation_sum']
        
        if times:
            temp = temps[0]
            wind = winds[0]
            precip = precips[0]
            
            # thresholds
            extreme_temperature = 1 if (temp < -15 or temp > 35) else 0
            extreme_wind = 1 if wind > 60 else 0
            extreme_precipitation = 1 if precip > 20 else 0
            
            record = {
                'date': today,
                'station_name': station_name,
                'latitude': float(lat),
                'longitude': float(lon),
                'temperature_mean_c': float(temp),
                'wind_gust_max_kmh': float(wind),
                'precipitation_sum_mm': float(precip),
                'extreme_temperature': extreme_temperature,
                'extreme_wind': extreme_wind,
                'extreme_precipitation': extreme_precipitation
            }
            all_weather.append(record)
            success_count += 1
    
    print(f"fetch complete: {success_count} success, {error_count} errors")
    
    if not all_weather:
        print("no weather data fetched, aborting")
        raise Exception("no data")
    
    # step 3: convert to csv
    print(f"converting {len(all_weather)} records to csv")
    
    try:
        csv_buffer = io.StringIO()
        fieldnames = [
            'date', 'station_name', 'latitude', 'longitude',
            'temperature_mean_c', 'wind_gust_max_kmh', 'precipitation_sum_mm',
            'extreme_temperature', 'extreme_wind', 'extreme_precipitation'
        ]
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_weather)
        
        csv_content = csv_buffer.getvalue()
        print(f"csv buffer size: {len(csv_content)} bytes")
        
        # step 4: upload to s3
        s3_key = f'raw-data/weather/daily-updates/{today}/weather_{today}.csv'
        print(f"uploading to s3: {s3_key}")
        
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=csv_content
        )
        
        print(f"upload successful: {s3_key}")
        
        # summary
        extreme_temps = sum(1 for r in all_weather if r['extreme_temperature'] == 1)
        extreme_winds = sum(1 for r in all_weather if r['extreme_wind'] == 1)
        extreme_precips = sum(1 for r in all_weather if r['extreme_precipitation'] == 1)
        
        summary = {
            'date': today,
            'records_fetched': len(all_weather),
            'extreme_temperature_count': extreme_temps,
            'extreme_wind_count': extreme_winds,
            'extreme_precipitation_count': extreme_precips
        }
        
        print(f"summary: {json.dumps(summary)}")
        
    except Exception as e:
        print(f"csv conversion error: {str(e)}")
        raise
    
    # ===== NEW SECTION: ETL TO WEATHER_ALL =====
    
    print("\n=== starting weather_all table refresh ===")
    
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
    
    # Read weather_yesterday table
    try:
        print("reading weather_yesterday table from glue catalog")
        df_yesterday = glueContext.create_dynamic_frame.from_catalog(
            database="mav_delays_weather", 
            table_name="weather_yesterday"
        ).toDF()
        print(f"successfully loaded weather_yesterday: {df_yesterday.count()} rows")
    except Exception as e:
        print(f"failed to read weather_yesterday table: {str(e)}")
        raise
    
    # Union tables
    try:
        print("unioning weather_initial and weather_yesterday tables")
        df_all = df_initial.union(df_yesterday)
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

except Exception as e:
    print(f"fatal error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()