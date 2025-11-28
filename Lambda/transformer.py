import json
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Get S3 event details
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"processing s3://{bucket}/{key}")
        
        # Read JSON
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Transform to DataFrame
        df = pd.DataFrame([data])
        
        # Add time-based columns
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Add derived metrics
        df['discount'] = 0.0
        df['final_amount'] = df['total_amount'] - df['discount']
        
        #Data validation
        if df['quantity'].iloc[0] <= 0:
            raise ValueError("Invalid quantity")
        if df['price'].iloc[0] <= 0:
            raise ValueError("Invalid price")
        
        # Write Parquet
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        
        #Generate output key
        new_key = key.replace('raw/', 'processed/').replace('.json', '.parquet')
        
        # Write to S3
        s3.put_object(
            Bucket=bucket,
            Key=new_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"Successfully transformed to: s3://{bucket}/{new_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation complete',
                'output_key': new_key
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return{
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }