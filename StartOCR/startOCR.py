import boto3

"""
Retrieves PDF transcript files from S3 bucket and starts OCR jobs on them.
Job ids are stored in DynamoDB on AWS. Textract OCR jobs include raw text
and key-value pair extraction.

Parameters
----------
bucket: (string) the text representation of the name of the S3 bucket

table: (string) the text representation of the name of the DynamoDB table
"""
def lambda_handler(bucket, table):
    db = boto3.client('dynamodb', region_name='us-east-2')  # Initialize AWS database client
    s3 = boto3.client('s3', region_name='us-east-2')
    textract = boto3.client('textract', region_name='us-east-2')

    bucket = bucket

    objects = s3.list_objects(Bucket=bucket)['Contents']
    
    for object in objects:
        file = object['Key']
        
        # key-value pair OCR extraction AWS job id
        kv_job = textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}},
            FeatureTypes=['FORMS'])['JobId']
        
        # raw text OCR extraction AWS job id
        raw_job = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}})['JobId']

        db_items = {
            'ARN': {'S': file},
            'kv_job': {'S': kv_job},
            'raw_job': {'S': raw_job}
        }

        db.put_item(
            TableName=table,
            Item=db_items
        )
