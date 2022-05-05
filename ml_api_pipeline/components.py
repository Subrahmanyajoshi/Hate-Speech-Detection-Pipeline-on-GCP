class PipelineComponents(object):

    @staticmethod
    def get_review(message: bytes):
        import json

        return json.loads(message.decode("utf-8"))['review']

    @staticmethod
    def strip_lines(line: str):
        line = line.strip()
        line = ' '.join(line.split())
        return line

    @staticmethod
    def remove_emojis(line: str):
        import re

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
        return re.sub(emoj, '', line)

    @staticmethod
    def convert_to_doc(line: str):
        from apache_beam.ml.gcp import naturallanguageml as nlp

        return nlp.Document(line, type='PLAIN_TEXT')

    @staticmethod
    def parse_response(response):
        import json

        final_sentence = ''

        for sentence in response.sentences:
            final_sentence += sentence.text.content

        message = json.dumps({'review': final_sentence, 'sentiment': response.document_sentiment.score})
        return message.encode("utf-8")
