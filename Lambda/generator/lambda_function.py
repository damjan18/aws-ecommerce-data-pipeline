import json
import boto3
import random
from datetime import datetime
import os

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET_NAME', 'ecommerce-data-pipeline-yourname')

PRODUCTS = [
    {'id': 'PROD101', 'name': 'Laptop', 'category': 'Electronics', 'base_price': 999.99},
    {'id': 'PROD102', 'name': 'Wireless Mouse', 'category': 'Electronics', 'base_price': 29.99},
    {'id': 'PROD103', 'name': 'USB-C Cable', 'category': 'Electronics', 'base_price': 12.99},
    {'id': 'PROD104', 'name': 'T-Shirt', 'category': 'Clothing', 'base_price': 19.99},
    {'id': 'PROD105', 'name': 'Jeans', 'category': 'Clothing', 'base_price': 49.99},
    {'id': 'PROD106', 'name': 'Python Book', 'category': 'Books', 'base_price': 39.99},
    {'id': 'PROD107', 'name': 'AWS Book', 'category': 'Books', 'base_price': 44.99},
]

def lambda_handler(event, context):
    
    try:
        # Select random product
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        price = product['base_price']
        
        # Generate transaction
        transaction = {
            'transaction_id': f'TXN{random.randint(10000, 99999)}',
            'timestamp': datetime.now().isoformat(),
            'customer_id': f'CUST{random.randint(1000, 9999)}',
            'product_id': f'PROD{random.randint(100, 999)}',
            'product_name': product['name'],
            'category': product['category'],
            'quantity': quantity,
            'price': price,
            'total_amount': round(quantity * price, 2)
        }
        
        # Write to S3
        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp_str = int(datetime.now().timestamp())
        key = f"raw/date={date_str}/transactions_{timestamp_str}.json"
        
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(transaction),
            ContentType='application/json'
        )
        print(f"Successfully wrote transaction {transaction['transaction_id']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transaction generated',
                'transaction_id': transaction['transaction_id']
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
