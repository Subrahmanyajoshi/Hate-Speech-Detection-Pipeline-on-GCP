import argparse
from argparse import Namespace
from datetime import datetime

import apache_beam as beam
import google
from apache_beam import io
from apache_beam.ml.gcp import naturallanguageml as nlp
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions


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


class DataFlowSubmitter(object):

    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.region = args.region
        self.input_topic = args.input_topic
        self.output_topic = args.output_topic

        # Setting up the Apache Beam pipeline options.
        self.options = pipeline_options.PipelineOptions(streaming=True, save_main_session=True)

        if args.direct_runner and args.dataflow_runner:
            raise ValueError('Please specify only one of the options. either direct runner or dataflow runner')

        self.runner = 'DirectRunner'
        if args.dataflow_runner:
            self.runner = 'DataFlowRunner'

    def set_pipeline_options(self):

        # Sets the project to the default project in your current Google Cloud environment.
        _, self.options.view_as(GoogleCloudOptions).project = google.auth.default()

        # Sets the Google Cloud Region in which Cloud Dataflow runs.
        self.options.view_as(GoogleCloudOptions).region = self.region

        self.options.view_as(GoogleCloudOptions).job_name = f'sa-{datetime.now().strftime("%Y%m%d-%H%M%S")}'

        dataflow_gcs_location = f'gs://{self.project}/{self.options.view_as(GoogleCloudOptions).job_name}'

        # Dataflow Staging Location. This location is used to stage the Dataflow Pipeline and SDK binary.
        self.options.view_as(GoogleCloudOptions).staging_location = f"{dataflow_gcs_location}/staging"

        # Dataflow Temp Location. This location is used to store temporary files or intermediate results before
        # finally outputting to the sink.
        self.options.view_as(GoogleCloudOptions).temp_location = f"{dataflow_gcs_location}/temp"

    def build_and_run(self):

        features = nlp.types.AnnotateTextRequest.Features(extract_document_sentiment=True)

        argv = [
            f'--project={self.project}',
            f'--job_name=text-parsing-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            '--save_main_session',
            f'--streaming',
            f'--staging_location=gs://{self.bucket}/text_parsing/staging/',
            f'--temp_location=gs://{self.bucket}/text_parsing/temp/',
            '--region=us-central1',
            f'--runner={self.runner}'
        ]

        pipeline = beam.Pipeline(options=argv)

        (
                pipeline
                | 'Consume messages' >> io.gcp.pubsub.ReadFromPubSub(
                        subscription='projects/text-analysis-323506/subscriptions/reviews-texts2-sub')
                | 'get review' >> beam.Map(PipelineComponents.get_review)
                | 'Strip lines' >> beam.Map(PipelineComponents.strip_lines)
                | 'Remove emojis' >> beam.Map(PipelineComponents.remove_emojis)
                | 'convert to doc' >> beam.Map(PipelineComponents.convert_to_doc)
                | 'Call gcloud nlp api' >> nlp.AnnotateText(features)
                | 'process response' >> beam.Map(PipelineComponents.parse_response)
                | 'To result topic' >> beam.io.WriteToPubSub(topic='projects/text-analysis-323506/topics/sa-results')
        )

        pipeline.run()


def main():
    parser = argparse.ArgumentParser(description='Running Apache Beam pipelines on Dataflow')
    parser.add_argument('--project', type=str, required=True, help='Project id')
    parser.add_argument('--region', type=str, required=True, help='Region to run dataflow')
    parser.add_argument('--bucket', type=str, required=True, help='Name of the bucket to host dataflow components')
    parser.add_argument('--input-topic', type=str, required=True, help='input pubsub topic')
    parser.add_argument('--output-topic', type=str, required=True, help='output pubsub topic')
    parser.add_argument('--direct-runner', required=False, action='store_true')
    parser.add_argument('--dataflow-runner', required=False, action='store_true')
    args = parser.parse_args()

    runner = DataFlowSubmitter(args=args)
    runner.build_and_run()


if __name__ == '__main__':
    main()
