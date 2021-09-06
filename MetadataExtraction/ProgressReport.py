import matplotlib.pyplot as plt
import json
import numpy as np

"""
Plots a histogram based on a sample of confidences from OCR extractions

Parameters
----------
conf: (list) a collection of floats each representing the average confidence of OCR
             extraction on a document
"""
def plotConf(conf):
    fig = plt.figure(figsize=(10, 5))
    plt.style.use('seaborn-whitegrid')
    plt.xlim((0, 100))
    plt.hist(conf, bins=[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100], facecolor='#2ab0ff', edgecolor='#169acf')
    plt.xlabel('Confidence %')
    plt.ylabel('Number of Observations')
    plt.title('Histogram of 500 Transcript Confidence Readings')
    plt.show()

"""
Plots out a historgram of confidences and writes a file of all transcripts' extraction
results as well as some distribution details on the confidences.

Parameters
----------
forms: (list) a collection of dictionaries where the key is a string of the transcript
              PDF name and is mapped to a dictionary. This dictionary maps the metadata
              to what the algorithm was able to select for each
"""
def report(forms):
    conf = []
    for form in forms:
        conf.append(form['Confidence'])
    output = open('results.txt', 'w')
    output.write('Transcript Extraction Report\n'
                 '500 transcripts analyzed\n\n'

                 'Confidence scores:\n'
                 f'     Mean: {np.mean(conf)}\n'
                 f'     SD: {np.std(conf)}\n\n'
                 )
    output.write(json.dumps(forms, indent=4, sort_keys=True))
    output.close()

    plotConf(conf)
