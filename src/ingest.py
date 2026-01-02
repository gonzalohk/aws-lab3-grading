import json
import boto3
import os

sqs = boto3.client('sqs')
QUEUE_URL = os.environ['QUEUE_URL']
ALLOWED_EXTENSIONS = {'.jpg', '.jpeg', '.png'}

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        etag = record['s3']['object']['eTag']
        
        # only images (validator)
        if not any(key.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
            print(f"Skipping {key}: Not a supported image.")
            continue
            
        message_body = {
            "bucket": bucket,
            "key": key,
            "etag": etag
        }
        
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        print(f"Enqueued: {key}")
        
    return {"statusCode": 200}