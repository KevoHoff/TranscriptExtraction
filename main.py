import boto3
import json
import sys

textract = boto3.client('textract', region_name='us-east-2')
db = boto3.resource('dynamodb', region_name='us-east-2')
s3 = boto3.client('s3', region_name='us-east-2')

table = db.Table('TestTable251')

scan = table.scan()
for transcript in scan['Items']:
    arn = transcript['ARN']
    kv_job = transcript['kv_job']
    raw_job = transcript['raw_job']

    kv_results = str(json.dumps(textract.get_document_analysis(JobId=kv_job)))
    raw_results = str(json.dumps(textract.get_document_text_detection(JobId=raw_job)))

    s3.put_object(
        Body=kv_results,
        Bucket='results251',
        Key=f'{arn[28:]}/kv.json'
    )
    s3.put_object(
        Body=raw_results,
        Bucket='results251',
        Key=f'{arn[28:]}/raw.json'
    )