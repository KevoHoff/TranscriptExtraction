import json
import boto3
import urllib.parse
# python -m spacy download en_core_web_trf
import spacy
import time
from EntityDetector import EntityDetector
from RawEntDetector import RawEntDetector
import ProgressReport as pr

def getFileContent(bucket, folder):
    s3res = boto3.resource('s3', region_name='us-east-2')
    kvObj = s3res.Object(bucket, f'{folder}/kv.json')
    rawObj = s3res.Object(bucket, f'{folder}/raw.json')

    # Retrieve the key-value pair contents
    kv_content = kvObj.get()['Body'].read().decode('utf-8')
    kv_json = json.loads(kv_content)

    # Retrieve the raw text contents
    raw_content = rawObj.get()['Body'].read().decode('utf-8')
    raw_json = json.loads(raw_content)

    return kv_json, raw_json

def lambda_handler(event, context):
    db = boto3.client('dynamodb', region_name='us-east-2')  # Initialize AWS database client
    s3 = boto3.client('s3', region_name='us-east-2')

    bucket = 'results251'

    META = loadData('meta.data')

    objects = s3.list_objects(Bucket=bucket)['Contents']
    files = {}
    for object in objects:
        partitioned = object['Key'].split('/')  # Partitioned out file directory
        desired = '/'.join(partitioned[0:-1])  # Directory specification
        if not desired in files.keys():
            files[desired] = {}

    output = []
    for k in files.keys():
        kv_json, raw_json = getFileContent(bucket, k)
        files[k]['kv'] = kv_json
        files[k]['raw'] = raw_json
        print(k)

        form = main(kv_json, raw_json, META)
        form['ARN'] = k
        print(form)

        # Store the information in proper format for DB
        db_items = {
            'First': {'S': form['First']},
            'Last': {'S': form['Last']},
            'Grad': {'S': form['Grad']},
            'ARN': {'S': form['ARN']}
        }
        output.append(form)

        # Put an item in the database
        data = db.put_item(
            TableName='Final251',
            Item=db_items
        )


"""
Takes in raw output from textract.analyze_document(...)
and sorts through data to return a mapping for all blocks
through their respective ID and a mapping for all key blocks
through their respective ID
"""
def extractIds(tokens):
    token_map = {}
    key_ids = []
    conf_list = []
    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token
        if 'Confidence' in token:
            conf_list.append(token['Confidence'])
        if token['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in token['EntityTypes']:
            key_ids.append(token_id)
    confidence = sum(conf_list) / len(tokens)
    return token_map, key_ids, confidence

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
                    if child_id in blocks_map.keys():
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
    token_map, key_ids, confidence = extractIds(tokens)

    # Gets key-value mapping
    mappings = getMapping(token_map, key_ids)


    # Initialize EntityDetector
    ed = EntityDetector(META)

    # Get the metadata
    form = ed.detectEntityKV(mappings)

    # Report confidence of readings
    form['Confidence'] = confidence

    return form


def getRawText(tokens):
    words = {}
    token_map = {}
    line_ids = []
    conf_list = []

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token
        if 'Confidence' in token:
            conf_list.append(token['Confidence'])
        if token['BlockType'] == 'LINE':
            line_ids.append(token['Id'])

    for line_id in line_ids:
        line = token_map[line_id]
        text = getText(line, token_map)
        y = line['Geometry']['BoundingBox']['Top']
        x = line['Geometry']['BoundingBox']['Left']
        #words[text] = {'x': x, 'y': y}
        words[text] = y

    confidence = sum(conf_list) / len(tokens)

    return words, confidence

def getLocation(token):
    pass

"""
Phase 2 extraction. If we cannot extract sufficient information from the Phase 1 extraction of key-value pairs,
then we will move to extraction using raw text in hopes of finding the remaining information
"""
def getRemainder(tokens, NA, META):
    nlp = spacy.load('en_core_web_trf')
    thirty_percent = []

    text, confidence = getRawText(tokens)

    for t in text:

        if text[t] <= 0.3:
            thirty_percent.append(t)

    red = RawEntDetector(META)  # Instantiating class

    form = red.detectEntity(NA, thirty_percent)

    form['Confidence'] = confidence

    return form


"""
Inputs the raw output from textract.analyze_document() and
outputs a dictionary formatted key-value mapping of words in the transcript
"""
def getMapping(token_map, key_ids):
    mappings = {}
    for key_id in key_ids:
        if key_id in token_map.keys():
            key = token_map[key_id]
            value_ids = getValueIds(key)
            values = []
            for value_id in value_ids:
                if value_id in token_map.keys():
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

def main(kv_json, raw_json, META):
    form = {'First': 'NA',
            'Last': 'NA',
            'Grad': 'NA'}

    # Extract metadata from key-value blocks
    key_value_dict = getKeyValues(kv_json['Blocks'], META)
    form.update(key_value_dict)

    print(form)

    # Identify what information is still missing
    nas = []
    for k, v in form.items():
        if v == 'NA':
            nas.append(k)  # Append the missing keys

    if 'NA' in form.values():
        print('Incomplete extraction...')

        # Use the raw text blocks to find remaining metadata
        rem_dict = getRemainder(raw_json['Blocks'], nas, META)
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
