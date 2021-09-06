import json
import boto3
import urllib.parse
# python -m spacy download en_core_web_trf
import spacy
import time
from EntityDetector import EntityDetector
from RawEntDetector import RawEntDetector
import ProgressReport as pr
"""
Retrieves key-value pair and raw text OCR extraction from JSON files in an S3 bucket.

Parameters
----------
bucket: (string) the string name of the S3 bucket on an AWS account
folder: (string) the folder name that is named after a given transcript PDF. Ends with .pdf

Returns
-------
kv_json: (dictionary) unaltered output from Textract's get_document_analysis(...) API
                      and contains OCR extraction information on key-value pairs
raw_json: (dictionary) unaltered output from Textract's get_document_text_detection(...)
                       API and contains OCR extraction information on text blocks
"""
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

"""
Takes OCR results from the key-value pairs and raw text of a document, extracts the
first name, last name, graduation year and stored these three pieces of metadata--
alongiside the ARN--into DynamoDB on AWS.

Priotity work to be done: algorithm to get school name from folder naming conventions 
and full ARN (instead of the current transcript file name).
"""
def lambda_handler(bucket):
    db = boto3.client('dynamodb', region_name='us-east-2')  # Initialize AWS database client
    s3 = boto3.client('s3', region_name='us-east-2')

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
Takes in raw output from get_document_analysis(...)
and sorts through data to return a mapping for all blocks
through their respective ID and a list for all key blocks
through their respective ID, and the average confidence of
OCR extraction

Parameters
----------
tokens: (list) each element is a dictionary that represents a single block of information
               identified from the OCR extraction ('Id', 'EntityType', 'BlockType', 'Text',
               'Relationships', 'Confidence', etc.)

Returns
-------
token_map: (dictionary) a mapping such that keys are the Id's of blocks found from OCR and
                        the values are the same dictionaries stores in the list tokens
                        
key_ids: (list) contains all blocks that are identified such that 'BlockType' == 'KEY_VALUE_SET'
         and 'EntityType' == 'KEY'. Allows to find value and text information from this
         parent block
                         
confidence: (float) represents the average confidence of OCR readings from every block found
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
Finds all value block ids for a given key block id

Parameters
----------
token: (dictionary) represents a single block of information identified from the OCR 
                    extraction ('Id', 'EntityType', 'BlockType', 'Text', 'Relationships',
                    'Confidence', etc.)

Returns
-------                   
value_ids: (list) contains all block ids that are identified such that 'BlockType' == 'KEY_VALUE_SET'
                  and 'EntityType' == 'VALUE' specifically as a descendent from a key block
"""
def getValueIds(token):
    for relation in token['Relationships']:
        if relation['Type'] == 'VALUE':
            value_ids = relation['Ids']
    return value_ids

"""
By inputting a block, this function will extract the text that pertains to it.

Parameters
----------
token: (dictionary) represents a single block of information identified from the OCR 
                    extraction ('Id', 'EntityType', 'BlockType', 'Text', 'Relationships',
                    'Confidence', etc.)

token_map: (dictionary) a mapping such that keys are the Id's of blocks found from OCR and
                        the values are the same dictionaries stores in the list tokens
                        
Returns
-------                   
phrase: (string) the text extracted from either a key or value block
"""
def getText(token, token_map):
    words = []
    if 'Relationships' in token:
        for relationship in token['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    if child_id in token_map.keys():
                        word = token_map[child_id]
                        if word['BlockType'] == 'WORD':
                            words.append(word['Text'])
                        elif word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                words.append('X')
    phrase = ' '.join(words)
    return phrase

"""
Phase 1. Extract the key-value pairs from a transcript and use it to identify metadata

Parameters
----------
tokens: (list) each element is a dictionary that represents a single block of information
               identified from the OCR extraction ('Id', 'EntityType', 'BlockType', 'Text',
               'Relationships', 'Confidence', etc.)
               
META: (dictionary) a mapping of different of key words (aliases) and words that indicate wrong 
                   selection (antialiases) to our desired metadata (first, last, and graduation 
                   date). Currently there only exists aliases and antaliases for key blocks of
                   the metadata

Returns
-------
form: (dictionary) a mapping of metadata names to extracted information. In other words, the
                   desired metadata are the keys and the selected information from fields in
                   a transcript are the values (e.g. what the first name is)
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

"""
Extracts all the lines of text found in a transcript document.

Parameters
----------
tokens: (list) each element is a dictionary that represents a single block of information
               identified from the OCR extraction ('Id', 'EntityType', 'BlockType', 'Text',
               'Relationships', 'Confidence', etc.)
                        
Returns
-------                   
words: (dictionary) the keys are a single line of text and it mapped to the y position of
                    the text found on the page. The position is represented in a percentage
                    from the top of the page. 0.1 is 10% from the top of the page
                    
confidence: (float) represents the average confidence of OCR readings from every block found
"""
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

Parameters
----------
tokens: (list) each element is a dictionary that represents a single block of information
               identified from the OCR extraction ('Id', 'EntityType', 'BlockType', 'Text',
               'Relationships', 'Confidence', etc.)
               
NA: (list) a collection of strings, where strings are the names of any piece of missing metadata
           (e.g., 'GRAD')
               
META: (dictionary) a mapping of different of key words (aliases) and words that indicate wrong 
                   selection (antialiases) to our desired metadata (first, last, and graduation 
                   date). Currently there only exists aliases and antaliases for key blocks of
                   the metadata

Returns
-------
form: (dictionary) a mapping of metadata names to extracted information. In other words, the
                   desired metadata are the keys and the selected information from fields in
                   a transcript are the values (e.g. what the first name is)
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
Gets the key-value pair mappings and location from a structured token_map

Parameters
----------
token_map: (dictionary) a mapping such that keys are the Id's of blocks found from OCR and
                        the values are the same dictionaries stores in the list tokens
                        
key_ids: (list) contains all blocks that are identified such that 'BlockType' == 'KEY_VALUE_SET'
         and 'EntityType' == 'KEY'. Allows to find value and text information from this
         parent block

Returns
-------
mappings: (dictionary) key is the field name found in a document, and is mapped to another
                       dictionary with two items. This dictionary contains 'Value' which
                       maps to the value text found in the field and 'Top' which represents
                       the relative percentage distance this key-value pair was from the top
                       of the document
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
Load in a file in JSON format to a dictionary

Parameters
----------
file: (string) file destination of JSON information

Returns
-------
data: (dictionary) any mapping output from a JSON file
"""
def loadData(file):
    with open(file) as fp:
        data = json.load(fp)
    return data

"""
Extracts key metadata information from Textract key-value pair and raw text OCR extraction

Parameters
----------
kv_json: (dictionary) unaltered output from Textract's get_document_analysis(...) API
                      and contains OCR extraction information on key-value pairs
                      
raw_json: (dictionary) unaltered output from Textract's get_document_text_detection(...)
                       API and contains OCR extraction information on text blocks
                       
META: (dictionary) a mapping of different of key words (aliases) and words that indicate wrong 
                   selection (antialiases) to our desired metadata (first, last, and graduation 
                   date). Currently there only exists aliases and antaliases for key blocks of
                   the metadata

Returns
-------
form: (dictionary) a mapping of metadata names to extracted information. In other words, the
                   desired metadata are the keys and the selected information from fields in
                   a transcript are the values (e.g. what the first name is)
"""
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
    response = lambda_handler(bucket='Bucket')
