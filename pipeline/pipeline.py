from argparse import Namespace
from datetime import datetime

import apache_beam as beam
from apache_beam import io
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

        if args.direct_runner and args.dataflow_runner:
            raise ValueError('Please specify only one of the options. either direct runner or dataflow runner')

        self.runner = 'DirectRunner'
        if args.dataflow_runner:
            self.runner = 'DataFlowRunner'

        # Setting up the Apache Beam pipeline options.
        self.options = []
        self.set_options()

        if args.setup_file:
            self.options.append(f'--setup_file={args.setup_file}')

        # Creating apache beam pipeline object
        self.pipeline = beam.Pipeline(argv=self.options)

    def set_options(self):

        job_name = f'sa-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
        dataflow_gcs_location = f'gs://{self.project}/{job_name}'

        self.options += [
            f'--project={self.project}',
            f'--job_name={job_name}',
            '--save_main_session',
            '--streaming',
            f'--staging_location={dataflow_gcs_location}/staging',
            f'--temp_location={dataflow_gcs_location}/temp',
            '--region=us-central1',
            f'--runner={self.runner}'
        ]

    def build(self):

        # Get tweet sentiments
        results = (
                self.pipeline
                | 'From PubSub' >>
                io.gcp.pubsub.ReadFromPubSub(topic=f'projects/text-analysis-323506/topics/{self.input_topic}')
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
                | 'PS Hate Speech' >>
                beam.io.WriteToPubSub(topic=f'projects/text-analysis-323506/topics/{self.output_topic}')
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
            self.pipeline.run().wait_until_finish()
        else:
            self.pipeline.run()
