import spacy
from dateutil import parser

"""
Handles analysis of transcript key-value pairs
"""
class EntityDetector():
    nlp = spacy.load('en_core_web_trf')  # NLP Class Variable

    # Defines the association between metadata we wish to find and the named entity that spacy detects it as
    translator = {'Name': 'PERSON',
                  'DOB': 'DATE',
                  'Grad': 'DATE'}

    def __init__(self, META):
        self.META = META
        self.form = {
            'First': 'NA',
            'Last': 'NA',
            'Grad': 'NA',
        }

    """
    Identify if a key-value pair is metadata and (if so) what piece of matadata is it
    """
    def classify(self, k, v):
        k = k.title()
        v = v.title()
        local_scores = {}
        local_scores['Name'] = self.__getScore__(k, v, 'Name')
        local_scores['Grad'] = self.__getScore__(k, v, 'Grad')
        local_max = max(local_scores, key=local_scores.get)
        score = local_scores[local_max]
        return local_max, score

    """
    Extracts the first name, last name, and graduation year if the information is found in a transcript
    """
    def detectEntityKV(self, mapping):
        scores = {
            'Name': {},
            'Grad': {},
        }
        for k, v in mapping.items():
            # local_max is a string that represents the classified metadata, e.g., 'First'
            # score is the awarded points for similarity to the classified metadata
            if len(v['Value']) > 0:
                local_max, score = self.classify(k, v['Value'])
                score += (1-v['Top'])

                if score > 2:
                    scores[local_max].update({
                        k: {
                             'Value': v['Value'],
                             'Score': score
                             }
                    })

        # Get first and last name
        first, last = self.__getName__(scores['Name'])
        self.form['First'] = first
        self.form['Last'] = last

        # Get the grad date
        grad = self.__getMax__(scores['Grad'])
        grad = self.processDate(grad)
        self.form['Grad'] = grad

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
        if len(names) > 0:
            for k, v in names.items():
                k_lower = k.lower()
                if 'first' in k_lower and v != '':
                    existsFirst = True
                    first = v['Value']
                elif 'last' in k_lower and v != '':
                    existsLast = True
                    last = v['Value']
                if existsFirst and existsLast:
                    break
            if not existsFirst or not existsLast:
                name = self.__getMax__(names)
                print(name)
                if ',' in name:
                    splitted_name = name.split(sep=',')
                    splitted_name[0] = splitted_name[0].strip(' ')
                    last = splitted_name[0].title()
                    if len(splitted_name[1].split()) > 1:
                        first = splitted_name[1].split()[0]
                        first = first.title()
                    else:
                        first = splitted_name[1]
                        first = first.title()
                    first = first.strip(' ')
                else:
                    names = name.split()
                    first = names[0].title()
                    last = names[-1].title()
        else:
            first = 'NA'
            last = 'NA'
        return first, last

    """
    Gives a key-value block a score for how well it relates to metadata
    """
    def __getScore__(self, key, value, name):
        score = 0
        ner = self.translator[name]

        key_aliases = self.META['key'][name]['alias']
        key_anti_aliases = self.META['key'][name]['antialias']
        value_aliases = self.META['value'][name]['alias']
        value_anti_aliases = self.META['value'][name]['antialias']

        temp_key = key.lower()
        temp_value = value.lower()

        for key_alias in key_aliases:  # reward if the key text contains aliases
            if key_alias in temp_key:
                score += 1
        for key_anti_alias in key_anti_aliases:  # reduce score if the key text contains anti aliases
            if key_anti_alias in temp_key:
                score -= 10
        for value_alias in value_aliases:
            if value_alias in temp_value:
                score += 2
        for value_anti_alias in value_anti_aliases:
            if value_anti_alias in temp_value:
                score -= 10

        doc = self.nlp(value)  # if the NLP recognizes an appropriate metadata, increment score
        for ent in doc.ents:
            if ent.label_ == ner:
                print(f'Person: {ent.text}')
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
