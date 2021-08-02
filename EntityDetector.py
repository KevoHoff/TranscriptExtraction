import spacy
from dateutil import parser

"""
Handles analysis of transcript key-value pairs
"""
class EntityDetector():
    nlp = spacy.load('en_core_web_trf')  # NLP Class Variable

    # Defines the association between metadata we wish to find and the named entity that spacy detects it as
    translator = {'School': 'ORG',
                  'Name': 'PERSON',
                  'DOB': 'DATE',
                  'Grad': 'DATE'}

    def __init__(self, META):
        self.META = META
        self.form = {
            'First': 'NA',
            'Last': 'NA',
            'Grad': 'NA',
            'School': 'NA'
        }

    """
    Identify if a key-value pair is metadata and (if so) what piece of matadata is it
    """
    def classify(self, k, v):
        local_scores = {}
        local_scores['Name'] = self.__getScore__(k, v, 'Name')
        local_scores['Grad'] = self.__getScore__(k, v, 'Grad')
        local_scores['School'] = self.__getScore__(k, v, 'School')
        local_max = max(local_scores, key=local_scores.get)
        score = local_scores[local_max]
        print(f'{k}: {v} \n      {local_scores}')
        return local_max, score

    """
    Extracts the first name, last name, graduation year, and school name if the information is found in a transcript
    """
    def detectEntity(self, mapping):
        scores = {
            'Name': {},
            'Grad': {},
            'School': {}
        }
        for k, v in mapping.items():
            # local_max is a string that represents the classified metadata, e.g., 'School'
            # score is the awarded points for similarity to the classified metadata
            local_max, score = self.classify(k, v)
            if score > 1:
                scores[local_max] = {
                    k: {
                         'Value': v,
                         'Score': score
                         }
                     }

        # Get first and last name
        first, last = self.__getName__(scores['Name'])
        self.form['First'] = first
        self.form['Last'] = last

        # Get the grad date
        grad = self.__getMax__(scores['Grad'])
        grad = self.processDate(grad)
        self.form['Grad'] = grad

        # Get the school name
        school = self.__getMax__(scores['School'])
        self.form['School'] = school

        return self.form

    """
    Finds the highest score for a given piece of metadata
    """
    def __getMax__(self, list):
        score = 0
        mX = 'NA'
        for k, v in list.items():
            if v['Score'] > score:
                mX = v['Value']
                score = v['Score']
        return mX

    """
    Finds the first and last name in a list of possible names from the transcript
    """
    def __getName__(self, names):
        existsFirst = False
        existsLast = False
        for k, v in names.items():
            k_lower = k.lower()
            if 'first' in k_lower:
                existsFirst = True
                first = v['Value']
            elif 'last' in k_lower:
                existsLast = True
                last = v['Value']
            if existsFirst and existsLast:
                print('break')
                break
        if not existsFirst or not existsLast:
            name = self.__getMax__(names)
            if ',' in name:
                last, remainder = name.split(sep=',')
                last = last.strip(' ')
                if len(remainder.split()) > 1:
                    first = remainder.split()[0]
                else:
                    first = remainder
                first = first.strip(' ')
            else:
                names = name.split()
                first = names[0]
                last = names[-1]
        return first.title(), last.title()

    """
    Gives a key-value block a score for how well it relates to metadata
    """
    def __getScore__(self, key, value, name):
        score = 0
        ner = self.translator[name]

        aliases = self.META['kv'][name]['alias']
        anti_aliases = self.META['kv'][name]['antialias']

        temp = key.lower()
        for alias in aliases:  # reward if the key text contains aliases
            if alias in temp:
                score += 1
        for anti_alias in anti_aliases:  # reduce score if the key text contains anti aliases
            if anti_alias in temp:
                score -= 1

        doc = self.nlp(value)  # if the NLP recognizes an appropriate metadata, increment score
        for ent in doc.ents:
            if ent.label_ == ner:
                score += 2

        return score

    """
    Converts a date to the YYYY/MM/DD format
    """
    def processDate(self, date):
        try:
            dt = parser.parse(date)
            dt_proc = dt.strftime("%Y/%m/%d")
        except ValueError:
            print('Warning. \'Date\' key does not map to a Date value.')
            dt_proc = 'NA'
        return dt_proc
