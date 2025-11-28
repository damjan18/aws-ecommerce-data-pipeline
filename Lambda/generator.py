import json
import boto3
import random
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'ecommerce-data-pipeline-damjan'

def lambda_handler(event, context):
    # Generate transaction
    transaction = {
        'transaction_id': f'TXN{random.randint(10000, 99999)}',
        'timestamp': datetime.now().isoformat(),
        'customer_id': f'CUST{random.randint(1000, 9999)}',
        'product_id': f'PROD{random.randint(100, 999)}',
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 500), 2),
        'category': random.choice(['Electronics', 'Clothing', 'Books'])
    }
    
    # Write to S3
    key = f"raw/date={datetime.now().strftime('%Y-%m-%d')}/transactions_{datetime.now().timestamp()}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(transaction)
    )
    
    return {'statusCode': 200}
