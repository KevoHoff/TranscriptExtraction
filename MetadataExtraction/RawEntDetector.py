import spacy
from dateutil import parser

class RawEntDetector():
    nlp = spacy.load('en_core_web_trf')  # NLP Class Variable

    def __init__(self, META):
        self.META = META
        self.form = {}

    def detectEntity(self, NA, text):
        if 'First' in NA:
            first, last = self.__getName__(text)
            self.form.update({'First': first,
                             'Last': last})

        # elif 'Grad' in NA:
        #     grad = self.__getGrad__(text)

        return self.form

    def __getName__(self, text):
        names = []

        for line in text:
            doc = self.nlp(line)
            for ent in doc.ents:
                if ent.label_ == 'PERSON':
                    names.append(line)

        print(names)

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