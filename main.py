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

def lambda_handler(event, context):
    db = boto3.client('dynamodb',
                      aws_access_key_id='AKIA2JQARBGZ7EXISAEW',
                      aws_secret_access_key='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',
                      region_name='us-east-2')  # Initialize AWS database client

    bucket = event['Records'][0]['s3']['bucket']['name']  # Get the s3 bucket name
    file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')  # Get the file name

    META = loadData('meta.data')  # Define metadata for classifying words during extraction process

    form = main(bucket, file, META)

    db_items = {}
    for key in META['kv'].keys():
        db_items.update({key: {'S': form[key]}})
    db_items.update({'Img': {'S': f'{event["Records"][0]["s3"]["bucket"]["arn"]}/{file}'}})

    print(db_items)
    data = db.put_item(
        TableName='Test-Table251',
        Item=db_items
    )
    return data

def startJob(client, bucket, file, type):
    response = None
    if type == 'Analyze':
        response = client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}},
            FeatureTypes=['FORMS'])
    elif type == 'Detect':
        response = client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': file}})
    return response['JobId']

def isJobComplete(client, jobId, type):
    status = 'IN_PROGRESS'
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

def getJobResults(client, jobId, type):
    if isJobComplete(client, jobId, type):
        if type == 'Analyze':
            response = client.get_document_analysis(JobId=jobId)
        elif type == 'Detect':
            response = client.get_document_text_detection(JobId=jobId)
        return response['Blocks']

# Takes in raw output from textract.analyze_document(...)
# and sorts through data to return a mapping for all blocks
# through their respective ID and a mapping for all key blocks
# through their respective ID
def extractIds(tokens):
    token_map = {}
    key_ids = []

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token

        if token['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in token['EntityTypes']:
            key_ids.append(token_id)

    return token_map, key_ids

def getValueIds(token):
    for relation in token['Relationships']:
        if relation['Type'] == 'VALUE':
            value_ids = relation['Ids']
    return value_ids

# Tentative method of extracting the text as I
# received from Pankaj's example
#
# Inputs the a single block and the block mapping
# and returns the text that either a key or value
# contains
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

def getRemainder(tokens, nas, nlp):
    global META
    token_map = {}
    line_ids = []

    trans = {'ORG': 'School',
             'PERSON': 'Name',
             'CARDINAL': 'GPA'}

    storage = {'School': [],
               'GPA': []}

    form_processed = {}

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token

        if token['BlockType'] == 'LINE':
            line_ids.append(token_id)

    for line in line_ids:
        block = token_map[line]
        text = block['Text']
        doc = nlp(text)

        labels = ['ORG']
        if len(doc.ents) == 1:
            label = doc.ents[0].label_
            if label in labels:
                counter = 0
                for item in META[trans[label]]:
                    if item in block['Text'].lower():
                        counter += 1
                perc_similar = counter / len(META[trans[label]])
                storage[trans[label]].append([perc_similar, text])
            for k, v in storage.items():
                mX_value = ''
                mX = 0
                for item in v:
                    if item[0] > mX:
                        mX = item[0]
                        mX_value = item[1]
                if mX > 0:
                    if k in nas and label == 'ORG':
                        form_processed.update({'School': mX_value})

    return form_processed

# Inputs the raw output from textract.analyze_document and
# outputs a json formatted key-value mapping
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
        mappings.update([(key_text, value_text)])
    return mappings

def getKeyValues(tokens, META):
    score_list = {}

    # Gets the mapping for IDs of blocks to the blocks themselves
    # token_map, key_ids = extractIds(tokens)
    #
    # mappings = getMapping(token_map, key_ids)
    mappings = {'Course:': 'Nursing', 'Date of Application:': '2/1/95',
                'Last': 'Fowler', 'First':'Schlonda', 'Middle': 'R',
                'Nickname': 'Shon', 'Date of Birth': '7/16/75', 'Age': '19',
                'Place of Birth': 'PlailA', 'Maiden Name': '',
                'Address': '1322 NARCAGAMIH St', 'City': 'Phila',
                'State': 'PA', 'Zip Code': '19138', 'Area Code': '215',
                'Telephone': '549-7301', 'V.A. No.': '', 'Branch of Service': '',
                'Soc. Security No.': '191-54-3459', 'YES': '', 'NO': '',
                'Last Employer: (Name)': 'Filenes BASEMENT',
                'Employed As:': 'SAles clerk', 'Address (Street)': '(City) (Zip) ST DAVIS LAnCAStoR AUE.',
                'Dates of Employment': '10/93-12/93', 'In Case of Emergency Notify': 'JoAnn Guy',
                'Relationship:': 'Aunt', 'Address': '7274 OGOnZ AVE,', 'Area Code': '215',
                'Telephone': '927-7009', 'Yes': 'NOT_SELECTED', 'No': 'NOT_SELECTED',
                'Explain': '', 'NAME': 'NAncy HANEY', 'ADDRESS': '2015 N, 20thst',
                'OCCUPATION': 'Teller', 'TELEPHONE': '924-6896', 'NAME': 'JoAnn Guy',
                'HOW DID YOU HEAR OF ADVANCED CAREER TRAINING?': 'Friend',
                'PREVIOUS TRAINING OR EXPERIENCE RELATED TO THIS PROGRAM': 'NO',
                'HOW LONG:': '', 'HAVE YOU APPLIED STUDENT LOAN': 'NO.',
                'STATE GRANT?': 'NO', 'PELL GRANT?': 'NO',
                'HAVE YOU PREVIOUSLY RECEIVED FINANCIAL AID FROM A FEDERAL OR STATE AGENCY?': 'NO',
                'Signature': 'Jahlondic fowler'
                }
    ed = EntityDetector(META)

    form = ed.detectEntity(mappings)
    print(form)

    return form

def loadData(file):
    with open(file) as fp:
        data = json.load(fp)
    return data

def findStudentInfo(tokens):
    token_map = {}
    line_ids = []
    info_words = ['student', 'info']
    stu_section = None
    in_range = []

    for token in tokens:
        token_id = token['Id']
        token_map[token_id] = token
        if token['BlockType'] == 'LINE' and 'Geometry' in token:
            if all(word in token['Text'] for word in info_words):
                stu_section = token
            else:
                line_ids.append(token_id)
    if stu_section != None:
        min, max = getMinMax(stu_section)
        for line_id in line_ids:
            line = token_map[line_id]
            l_min, l_max = getMinMax(line)
            if (l_max > min and l_max < max) or (l_min < max and l_max > min):
                in_range.append(line['Text'])

    print(in_range)
    return in_range

def getMinMax(token):
    dimensions = token['Geometry']
    x_array = []
    for x, y in dimensions['Polygon']:
        x_array.append(x)
    max = np.max(x_array)
    min = np.min(x_array)
    return min, max

def main(bucket, file, META):
    client = boto3.client('textract',
                          aws_access_key_id='AKIA2JQARBGZ7EXISAEW',
                          aws_secret_access_key='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',
                          region_name='us-east-2'
                          )

    form = {}
    for k in META['kv'].keys():
        form.update({k: 'NA'})

    # identify form objects
    jobId = startJob(client, bucket, file, 'Analyze')
    key_value = getJobResults(client, jobId, 'Analyze')

    key_value_dict = getKeyValues(key_value, META)

    form.update(key_value_dict)

    # nas = []
    # for k, v in form.items():
    #     if v == 'NA' and (k == 'School' or k == 'Name'):
    #         nas.append(k)
    #         jobId = startJob(client, bucket, file, 'Detect')
    #         raw = getJobResults(client, jobId, 'Detect')
    #         rem_dict = getRemainder(raw['Blocks'], nas, nlp)
    #         form.update(rem_dict)



    print("========Process Successful========")

    return form
if __name__ == '__main__':
    event = loadData('event.txt')
    context = None
    response = lambda_handler(event, context)