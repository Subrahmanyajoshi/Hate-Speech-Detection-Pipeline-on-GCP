import json
import re
from typing import Dict

import apache_beam as beam
import numpy as np
from apache_beam import pvalue
from apache_beam.ml.gcp import naturallanguageml as nlp
from google.cloud import language


# Separate Results into Hate comment or Normal comment
class ResultsFilter(beam.DoFn):
    OUTPUT_TAG_HATE = 'Hate comments'
    OUTPUT_TAG_NORM = 'Normal comments'

    def process(self, result):
        sentiment = result['sentiment']

        if sentiment == 'hate':
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_HATE, result)
        else:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_NORM, result)


class PipelineComponents(object):

    @staticmethod
    def load_comment(message: bytes):
        message = json.loads(message.decode("utf-8"))
        return message

    @staticmethod
    def preprocess_comment(message: Dict):
        line = message['text']

        # Remove extra spaces, hastags and new line characters
        line = line.strip()
        line = line.replace('\n', '')
        line = line.replace('\\', '')
        line = line.replace('#', '')
        line = line.replace('&', ' ')
        line = ' '.join(line.split())

        # Href strings in comments
        re.sub("<[^>]+>", "", line)

        # Remove @ mentions and URLs
        line = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", line)

        # Remove extra spaces
        line = " ".join(line.split())

        # Remove special characters
        line = re.sub('[-+.^:,!]', '', line)

        # Remove Numbers
        line = ' '.join(c for c in line.split() if not c.isdigit())

        # Remove Emojies
        emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
        line = re.sub(emoj, '', line)

        line = line.lower().strip()

        # Expanding short forms
        contraction_dict = {"ain't": "are not", "'s": " is", "i'm": "i am", "aren't": "are not", "don't": "do not",
                            "didn't": "did not", "won't": "will not",
                            "can't": "cannot", "wouldn't": "would not", "hv": "have", "ik": "i know", "fr": "for real"}

        words = line.split()
        for i in range(len(words)):
            if words[i] in contraction_dict:
                words[i] = contraction_dict[words[i]]
        line = ' '.join(words)

        message['preprocessed'] = nlp.Document(line, type='PLAIN_TEXT')
        return message

    @staticmethod
    def detect_sentiments(message: Dict):
        client = language.LanguageServiceClient()
        line = message['preprocessed']

        try:
            message['response'] = client.analyze_sentiment(document={'content': line.content, 'type': line.type})
        except Exception:
            message['response'] = None

        return message

    @staticmethod
    def prepare_results(message):
        response = message['response']

        if response:
            message['score'] = response.document_sentiment.score
            message['sentiment'] = 'hate' if message['score'] <= -0.6 else 'normal'
        else:
            message['score'] = np.nan
            message['sentiment'] = 'NA'

        del message['preprocessed']
        del message['response']

        return message

    @staticmethod
    def convert_to_bytes(result):
        return json.dumps(result).encode("utf-8")
