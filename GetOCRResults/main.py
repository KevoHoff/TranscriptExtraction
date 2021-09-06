import boto3
import json
import sys
"""
Get OCR job results for key-value pairs and raw text from DynamoDB job Ids to store in folder structure
in S3 bucket. In S3, each transcript document gets a folder named after it. In each folder are two JSON
files. One for key_value pair OCR results and one for raw_text OCR results.
"""
# Establish AWS client connections
textract = boto3.client('textract', region_name='us-east-2')
db = boto3.resource('dynamodb', region_name='us-east-2')
s3 = boto3.client('s3', region_name='us-east-2')

# Connect to DynamoDB table
table = db.Table('TestTable251')

# Get job ids and ARN from database
scan = table.scan()
for transcript in scan['Items']:
    arn = transcript['ARN']
    kv_job = transcript['kv_job']
    raw_job = transcript['raw_job']
    
    # Get OCR results for key-value pairs and raw text
    kv_results = str(json.dumps(textract.get_document_analysis(JobId=kv_job)))
    raw_results = str(json.dumps(textract.get_document_text_detection(JobId=raw_job)))
    
    # Store key-value pair OCR results in kv.json file
    s3.put_object(
        Body=kv_results,
        Bucket='results251',
        Key=f'{arn[28:]}/kv.json'
    )
    
    # Store raw text OCR results in raw.json file
    s3.put_object(
        Body=raw_results,
        Bucket='results251',
        Key=f'{arn[28:]}/raw.json'
    )
