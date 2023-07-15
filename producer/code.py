import boto3
import random
import io
import pandas as pd
import hashlib
import csv
import json

s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

bucket_name = 'tlc-data-2022'
processed_bucket_name = 'processed-data-loc'

hashes_file = 'hashed_keys.txt'

kinesis_stream_name = 'Kinesis-queue'

objects = s3_client.list_objects_v2(Bucket=bucket_name)


def md5(data):
    """Calculates the MD5 hash of the given data."""
    hash_digest = hashlib.md5()
    hash_digest.update(data.encode('utf-8'))
    return hash_digest.hexdigest()


def lambda_handler(event, context):
    # read hashed keys
    file_obj = s3_client.get_object(Bucket=processed_bucket_name, Key=hashes_file)
    existing_keys_raw = file_obj['Body'].read().decode('utf-8')
    existing_keys = existing_keys_raw.split(",")

    
    for obj in objects['Contents']:
        filename = obj['Key']
        identifier = filename[:filename.index("_")]

        if filename.endswith('.parquet') and filename.startswith('green'):
            response = s3_client.get_object(Bucket=bucket_name, Key=filename)
            parquet_file = response['Body'].read()
            raw_data = pd.read_parquet(io.BytesIO(parquet_file))

            num_records = random.randint(1, 10)
            data = raw_data.sample(n=num_records)
            print('num of records ' + str(num_records))

            for index, record in data.iterrows():
                unique_key = ','.join(
                    [str(record['VendorID']), str(record['lpep_pickup_datetime']), str(record['lpep_dropoff_datetime']),
                     str(record['fare_amount'])])
                hashed_key = md5(unique_key)

                if hashed_key in existing_keys:
                    continue
                else:
                    existing_keys.append(hashed_key)
                    print(record.to_json())
                    # encoded = record.to_json().encode('utf-8')
                    # kinesis_client.put_record(
                    #     StreamName=kinesis_stream_name,
                    #     Data=encoded,
                    #     PartitionKey=identifier)

            updated_data = ",".join(existing_keys)

            
            s3_client.put_object(Body=updated_data, Bucket=processed_bucket_name, Key=hashes_file)

    return {
        'statusCode': 200,
        'body': 'Records sent to Kinesis'
    }
