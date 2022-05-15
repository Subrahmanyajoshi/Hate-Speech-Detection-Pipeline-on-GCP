from argparse import Namespace
from datetime import datetime

import apache_beam as beam
import google
from apache_beam import io
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DirectRunner, DataflowRunner
from google.cloud import bigquery

from pipeline.components import PipelineComponents, ResultsFilter


class PipelineBuilder(object):

    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.region = args.region
        self.input_topic = args.input_topic
        self.output_topic = args.output_topic

        self.schema = [
            bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("id", "BIGNUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("score", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("sentiment", "STRING", mode="REQUIRED")
        ]

        # Setting up the Apache Beam pipeline options.
        self.options = pipeline_options.PipelineOptions(streaming=True, save_main_session=True)
        self.set_pipeline_options()

        argv = []
        if args.setup_file:
            argv.append(f'--setup_file={args.setup_file}')

        # Creating apache beam pipeline object
        self.pipeline = beam.Pipeline(argv=argv, options=self.options)

        if args.direct_runner and args.dataflow_runner:
            raise ValueError('Please specify only one of the options. either direct runner or dataflow runner')

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

    def build(self):

        # Get tweet sentiments
        results = (
                self.pipeline
                | 'From PubSub' >> io.gcp.pubsub.ReadFromPubSub(topic='projects/text-analysis-323506/topics/tweets')
                | 'Load Tweets' >> beam.Map(PipelineComponents.load_tweet)
                | 'Preprocess Tweets' >> beam.Map(PipelineComponents.preprocess_tweet)
                | 'Detect Sentiments' >> beam.Map(PipelineComponents.detect_sentiments)
                | 'Prepare Results' >> beam.Map(PipelineComponents.prepare_results)
        )

        separated_results = (results | 'Divide Results' >> beam.ParDo(ResultsFilter()).with_outputs('Hate speech',
                                                                                                    'Normal speech'))

        # Hate speech results to PubSub topic
        hate_speech_pubsub = (
                separated_results['Hate speech']
                | 'Bytes Conversion' >> beam.Map(PipelineComponents.convert_to_bytes)
                | 'PS Hate Speech' >> beam.io.WriteToPubSub(topic='projects/text-analysis-323506/topics/sa-results')
        )

        hate_speech_bq = (
                separated_results['Hate speech']
                | 'BQ Hate Speech' >> beam.io.WriteToBigQuery(table='hate_speeches', dataset='tweets_analysis',
                                                              project='text-analysis-323506')
        )

        normal_speech_bq = (
                separated_results['Normal speech']
                | 'BQ Norm Speech' >> beam.io.WriteToBigQuery(table='normal_speeches', dataset='tweets_analysis',
                                                              project='text-analysis-323506')
        )

    def run(self, runner: str):
        if runner == "direct":
            _ = DirectRunner().run_pipeline(self.pipeline, options=self.options).wait_until_finish()
        else:
            _ = DataflowRunner().run_pipeline(self.pipeline, options=self.options)
