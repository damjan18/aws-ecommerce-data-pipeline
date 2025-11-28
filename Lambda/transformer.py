import json
import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Read JSON
    obj = s3.get_object(Bucket=bucket)
    data = json.loads(obj['Body'].read())
    
    # Transform to DataFrame
    df = pd.DataFrame([data])
    df['total_amount'] = df['quantity'] * df['price']
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Write Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    
    new_key = key.replace('raw/', 'processed/').replace('.json', '.parquet')
    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=buffer.getvalue()
    )
    
    return {'statusCode': 200}