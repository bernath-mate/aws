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

args = {}
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("daily-weather-update", {})

s3 = boto3.client('s3')
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
        
        try:
            # build url with urllib
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
            
            with urllib.request.urlopen(req, timeout=120) as response:
                data = json.loads(response.read().decode('utf-8'))
            
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
        
        except urllib.error.URLError as e:
            print(f"api error for {station_name}: {str(e)}")
            error_count += 1
        except Exception as e:
            print(f"error processing {station_name}: {str(e)}")
            error_count += 1
    
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

except Exception as e:
    print(f"fatal error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()