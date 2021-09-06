import spacy
from dateutil import parser

"""
Handles analysis of transcript key-value pairs
"""
class RawEntDetector():
    nlp = spacy.load('en_core_web_trf')  # NLP Class Variable

    def __init__(self, META):
        self.META = META
        self.form = {}
    
    """
    Extracts the first name, last name, and graduation year if the information is found in a transcript
    
    Parameters
    ----------
    NA: (list) a collection of strings, where strings are the names of any piece of missing metadata
               (e.g., 'GRAD')
   
    text: (list) a collection of strings representing text lines found in a document
    
    Returns
    -------
    form: (dictionary) a mapping of metadata names to extracted information. In other words, the
                       desired metadata are the keys and the selected information from fields in
                       a transcript are the values (e.g. what the first name is)
    """
    def detectEntity(self, NA, text):
        if 'First' in NA:
            first, last = self.__getName__(text)
            self.form.update({'First': first,
                             'Last': last})

        # elif 'Grad' in NA:
        #     grad = self.__getGrad__(text)

        return self.form
    
    """
    Finds the first and last name in a list of possible names from the transcript. Tries
    to take in various formats (e.g., First Last; Last, First; First Mi Last; Last, First Mi)
    
    Parameters
    ----------   
    text: (list) a collection of strings representing text lines found in a document
    
    Returns
    -------
    first: (string) the text representation of a person's first name in the document
    
    last: (string) the text representation of a person's first name in the document
    
    Priority Work
    -------------
    This algorithm needs improvements as it gets confused when there is a middle name.
    Likely can be optimized with more concise code
    """
    def __getName__(self, text):
        names = []

        for line in text:
            doc = self.nlp(line)
            for ent in doc.ents:
                if ent.label_ == 'PERSON':
                    names.append(line)

        if len(names) == 1:
            name = names[0]
            if ',' in name:
                last, remainder = name.split(sep=',')
                last = last.strip(' ').title()
                if len(remainder.split()) > 1:
                    first = remainder.split()[0]
                else:
                    first = remainder
                first = first.strip(' ').title()
            else:
                part_name = name.split()
                first = part_name[0].title()
                last = part_name[-1].title()
        else:
            first = 'NA'
            last = 'NA'
        return first, last
