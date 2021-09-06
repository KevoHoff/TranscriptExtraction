# TranscriptExtraction
A multi-step algorithm to extract key information from transcript documents in the state of PA.

main.py is a script file that handles the bulk algorithms to extract blocks of key-value mappings or raw text from transcripts. It pulls transcripts from an S3 bucket and pushes extracted information to DynamoDB.

EntityDetector.py is a script file that defines the class that analyzes key-value pairs and extracts metadata from the mappings.

RawEntDetector.py is a script file that defines the class that analyzes raw text on transcript and extracts metadata from the collection.

meta.data is a structured JSON file that contains aliases and antialiases for certain metadata. Aliases are works that metadata may appear as and antialiases are words that indicate it is not metadata we are looking for.

event.txt is a text file that simulates an event trigger from S3.

Priority Work:
- Improve RawEntDetector to look for graduation dates
- Use regex to detect different ways of getting graduation dates (e.g., ranges from intial to final date)
- Improve name formatting so that names are always stored in Last, First correctly
- Tweak aliases and scoring so that less extraneous information is selected as metadata

