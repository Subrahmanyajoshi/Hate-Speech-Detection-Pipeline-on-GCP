import json
import re
from typing import Dict

import apache_beam as beam
import numpy as np
from apache_beam import pvalue
from apache_beam.ml.gcp import naturallanguageml as nlp
from google.cloud import language


# Separate Results into Hate speech or Normal speech

class ResultsFilter(beam.DoFn):
    OUTPUT_TAG_HATE = 'Hate speech'
    OUTPUT_TAG_NORM = 'Normal speech'

    def process(self, result):
        sentiment = result['sentiment']

        if sentiment == 'hate':
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_HATE, result)
        else:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_NORM, result)


class PipelineComponents(object):

    @staticmethod
    def load_tweet(message: bytes):
        message = json.loads(message.decode("utf-8"))
        return message

    @staticmethod
    def preprocess_tweet(message: Dict):

        line = message['content']

        # Remove extra spaces, hastags and new line characters
        line = line.strip()
        line = line.replace('\n', '')
        line = line.replace('\\', '')
        line = line.replace('#', '')
        line = ' '.join(line.split())

        # Remove @ mentions and URLs
        line = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", line)
        line = " ".join(line.split())

        # Expanding short forms
        contraction_dict = {"ain't": "are not", "'s": " is", "aren't": "are not", "don't": "do not",
                            "didn't": "did not", "won't": "will not",
                            "can't": "cannot"}

        words = line.split()
        for i in range(len(words)):
            if words[i] in contraction_dict:
                words[i] = contraction_dict[words[i]]
        line = ' '.join(words)

        # Remove special characters
        line = re.sub('[-+.^:,]', '', line)

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
            message['sentiment'] = 'hate' if message['score'] < -0.5 else 'normal'
        else:
            message['score'] = np.nan
            message['sentiment'] = 'NA'

        del message['preprocessed']
        del message['response']

        return message

    @staticmethod
    def convert_to_bytes(result):
        import json
        return json.dumps(result).encode("utf-8")
