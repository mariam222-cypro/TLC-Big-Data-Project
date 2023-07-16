import json
import base64
from datetime import datetime
from json import JSONEncoder
import boto3
import csv
from io import StringIO 
import random


s3_client = boto3.client('s3', 'us-east-1')

bucket_name = 'staging-data-loc'
file_name = 'consumer_output.csv'


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def lambda_handler(event, context):
    print(json.dumps(event))
    print('-----------')
    valid_records = []
    for record in event['Records']:
        raw_data = json.loads(base64.b64decode(record['kinesis']['data']))
        # print(raw_data)
        fare_amount = raw_data.get('fare_amount')
        
        if fare_amount is not None and fare_amount < 0:
            continue  
        
        # print(raw_data)
        key=record['kinesis']['partitionKey']
        
        transformed_data = green_transformation(key, raw_data)
        valid_records.append(transformed_data)
    print(valid_records)
    
    # json_data = json.dumps(valid_records, cls=DateTimeEncoder).encode('utf-8')
    # s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=file_name)
    
    csv_data = convert_records_to_csv(valid_records)
    s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=file_name)


def green_transformation(key, data):
    data['type'] = key
    data['lpep_pickup_datetime'] = datetime.fromtimestamp(data['lpep_pickup_datetime'] / 1000)
    data['lpep_dropoff_datetime'] = datetime.fromtimestamp(data['lpep_dropoff_datetime'] / 1000)
    
    payment_type = data.get('payment_type') 
    
    payment_type_mapping = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'
    }
    
    if payment_type is not None:
        payment_type = str(int(payment_type)) 
        data['payment_type'] = payment_type_mapping.get(payment_type, 'Unknown')
    else:
        data['payment_type'] = 'Unknown'  
    
    unique_key = generate_unique_key(data)
    data['trip_id'] = unique_key
    
    trip_distance_miles = data.get('trip_distance')
    if trip_distance_miles is not None:
        trip_distance_km = round(trip_distance_miles * 1.60934, 2)
        data['trip_distance'] = trip_distance_km
    
    rate_code = data.get('RatecodeID')
    if rate_code is not None:
        rate_mapping = {
            1: 'Standard rate',
            2: 'JFK',
            3: 'Newark',
            4: 'Nassau or Westchester',
            5: 'Negotiated fare',
            6: 'Group ride'
        }
        data['ratecode'] = rate_mapping.get(rate_code, 'Unknown')
    del data['RatecodeID']
    
    return data



def yellow_transformation(key, data):
    data['type'] = key
    data['tpep_pickup_datetime'] = datetime.fromtimestamp(data['tpep_pickup_datetime'] / 1000)
    data['tpep_dropoff_datetime'] = datetime.fromtimestamp(data['tpep_dropoff_datetime'] / 1000)
    payment_type = str(int(data['payment_type']))
    data['payment_type'] = payment_type_mapping.get(payment_type, 'Unknown')
    
    return data

def convert_records_to_csv(records):
    fieldnames = [
        'trip_id', 'type', 'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
        'DOLocationID', 'PULocationID', 'trip_type', 'store_and_fwd_flag',
        'trip_distance', 'passenger_count', 'payment_type', 'ratecode',
        'extra', 'congestion_surcharge', 'mta_tax', 'fare_amount', 'ehail_fee',
        'tolls_amount', 'tip_amount', 'improvement_surcharge', 'total_amount'
    ]
    csv_data = ""
    with StringIO() as csv_buffer:
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        csv_data = csv_buffer.getvalue()
    return csv_data

    
def generate_unique_key(data):
    key = data['type']
    vendor_id = data['VendorID']
    pickup_datetime = data['lpep_pickup_datetime']
    
    year = pickup_datetime.year
    month = pickup_datetime.month
    day = pickup_datetime.day
    
    period_of_trip = (data['lpep_dropoff_datetime'] - pickup_datetime).seconds
    
    random_numbers = ''.join(random.choices('0123456789', k=3))
    
    unique_key = f"{key}{vendor_id}{year}{month:02d}{day:02d}{period_of_trip}{random_numbers}"
    return unique_key
