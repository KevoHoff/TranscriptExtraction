import json
import boto3
import urllib.parse
from dateutil import parser
# pip install cupy
# python -m spacy download en_core_web_trf
import spacy
import time
import numpy as np
from EntityDetector import EntityDetector
from RawEntDetector import RawEntDetector

def lambda_handler(event, context):
    db = boto3.client('dynamodb',
                      aws_access_key_id='AKIA2JQARBGZ7EXISAEW',
                      aws_secret_access_key='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',
                      region_name='us-east-2')  # Initialize AWS database client

    bucket = event['Records'][0]['s3']['bucket']['name']  # Get the s3 bucket name
    file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')  # Get the file name

    META = loadData('meta.data')  # Define metadata for classifying words during extraction process

    form = main(bucket, file, META)  # Get the metadata

    # Store the information in proper format for DB
    # db_items = {}
    # for key in META['kv'].keys():
    #     db_items.update({key: {'S': form[key]}})
    # db_items.update({'Img': {'S': f'{event["Records"][0]["s3"]["bucket"]["arn"]}/{file}'}})

    # Put an item in the database
    # data = db.put_item(
    #     TableName='Test-Table251',
    #     Item=db_items
    # )
    # return data

"""
Start an asychronous job using Textract's API on a transcript located in bucket/file.
Capable of analyzing for key-value pairs, or detecting raw text
"""
def startJob(client, bucket, file, type):
    response = None
    if type == 'Analyze':  # Finds key-value pairs
        response = client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}},
            FeatureTypes=['FORMS'])
    elif type == 'Detect':  # Finds raw text
        response = client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}})
    return response['JobId']

"""
Checks if the Textract job is completed or not.
"""
def isJobComplete(client, jobId, type):
    status = 'IN_PROGRESS'
    if type == 'Analyze':
        print('Key-Value Extraction Phase')
    if type == 'Detect':
        print('Raw Text Extraction Phase')
    while status == 'IN_PROGRESS':
        time.sleep(2)
        if type == 'Analyze':
            response = client.get_document_analysis(JobId=jobId)
        elif type == 'Detect':
            response = client.get_document_text_detection(JobId=jobId)
        status = response['JobStatus']
        print(f'Job Status: {status}')
    if status == 'SUCCEEDED':
        isComplete = True
    else:
        isComplete = False
    print(f'Job Status: {status}')
    return isComplete

"""
When the job is complete, extract the block information.
We can harness this to extract key-value pairs and raw text (Depending on our job type).
"""
def getJobResults(client, jobId, type):
    if isJobComplete(client, jobId, type):
        if type == 'Analyze':
            response = client.get_document_analysis(JobId=jobId)
        elif type == 'Detect':
            response = client.get_document_text_detection(JobId=jobId)
        return response['Blocks']

"""
Takes in raw output from textract.analyze_document(...)
and sorts through data to return a mapping for all blocks
through their respective ID and a mapping for all key blocks
through their respective ID
"""
def extractIds(tokens):
    token_map = {}
    key_ids = []

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token

        if token['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in token['EntityTypes']:
            key_ids.append(token_id)

    return token_map, key_ids

"""
Given a key block, extract its values
"""
def getValueIds(token):
    for relation in token['Relationships']:
        if relation['Type'] == 'VALUE':
            value_ids = relation['Ids']
    return value_ids

"""
By inputting a block, this function will extract the text that pertains to it.
"""
def getText(token, blocks_map):
    words = []
    if 'Relationships' in token:
        for relationship in token['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        words.append(word['Text'])
                    elif word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            words.append('X')
    return ' '.join(words)

"""
Phase 1. Extract the key-value pairs from a transcript and use it to identify metadata
"""
def getKeyValues(tokens, META):
    # Gets the mapping for IDs of blocks to the blocks themselves
    token_map, key_ids = extractIds(tokens)

    # Gets key-value mapping
    mappings = getMapping(token_map, key_ids)

    # Initialize EntityDetector
    ed = EntityDetector(META)

    # Get the metadata
    form = ed.detectEntityKV(mappings)

    return form


def getRawText(tokens):
    words = {}
    token_map = {}
    line_ids = []

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token
        if token['BlockType'] == 'LINE':
            line_ids.append(token['Id'])

    for line_id in line_ids:
        line = token_map[line_id]
        text = getText(line, token_map)
        y = token['Geometry']['BoundingBox']['Top']
        x = token['Geometry']['BoundingBox']['Left']
        words[text] = {'x': x, 'y': y}

    return words

def getLocation(token):
    pass

"""
Phase 2 extraction. If we cannot extract sufficient information from the Phase 1 extraction of key-value pairs,
then we will move to extraction using raw text in hopes of finding the remaining information
"""
def getRemainder(tokens, NA, META):
    text = getRawText(tokens)

    red = RawEntDetector(META)  # Instantiating class

    form = red.detectEntity(NA, text)

    return form


"""
Inputs the raw output from textract.analyze_document() and
outputs a dictionary formatted key-value mapping of words in the transcript
"""
def getMapping(token_map, key_ids):
    mappings = {}

    for key_id in key_ids:
        key = token_map[key_id]
        value_ids = getValueIds(key)
        values = []
        for value_id in value_ids:
            value = token_map[value_id]
            values.append(getText(value, token_map))
        value_text = ' '.join(values)
        key_text = getText(token_map[key_id], token_map)
        y = key['Geometry']['BoundingBox']['Top']
        mappings[key_text] = {'Value': value_text, 'Top': y}

    return mappings

"""
Load in a file
"""
def loadData(file):
    with open(file) as fp:
        data = json.load(fp)
    return data

def main(bucket, file, META):
    client = boto3.client('textract',
                          aws_access_key_id='AKIA2JQARBGZ7EXISAEW',
                          aws_secret_access_key='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',
                          region_name='us-east-2'
                          )

    form = {'First': 'NA',
            'Last': 'NA',
            'School': 'NA',
            'Grad': 'NA'}

    # Identify form objects
    jobId = startJob(client, bucket, file, 'Analyze')
    key_value = getJobResults(client, jobId, 'Analyze')

    # Extract metadata from key-value blocks
    key_value_dict = getKeyValues(key_value, META)
    form.update(key_value_dict)

    print(form)

    # Identify what information is still missing
    nas = []
    for k, v in form.items():
        if v == 'NA':
            nas.append(k)  # Append the missing keys

    if 'NA' in form.values():
        print('Incomplete extraction...')
        # Identify raw text objects
        jobId = startJob(client, bucket, file, 'Detect')
        raw = getJobResults(client, jobId, 'Detect')

        # Use the raw text blocks to find remaining metadata
        rem_dict = getRemainder(raw, nas, META)
        form.update(rem_dict)
    else:
        print('Complete extraction...')

    print(form)
    return form


"""
Simulating event and context event triggers in AWS
"""
if __name__ == '__main__':
    event = loadData('event.txt')
    context = None
    response = lambda_handler(event, context)
