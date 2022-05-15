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
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_profile", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("score", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE")
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

        job_name = f'hate-speech-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
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
                io.gcp.pubsub.ReadFromPubSub(topic=f'projects/{self.project}/topics/{self.input_topic}')
                | 'Load Comments' >> beam.Map(PipelineComponents.load_comment)
                | 'Preprocess Comments' >> beam.Map(PipelineComponents.preprocess_comment)
                | 'Detect Sentiments' >> beam.Map(PipelineComponents.detect_sentiments)
                | 'Prepare Results' >> beam.Map(PipelineComponents.prepare_results)
        )

        separated_results = (results | 'Divide Results' >> beam.ParDo(ResultsFilter()).with_outputs('Hate comments',
                                                                                                    'Normal comments'))

        # Hate speech results to PubSub topic
        hate_comments_pubsub = (
                separated_results['Hate comments']
                | 'Bytes Conversion' >> beam.Map(PipelineComponents.convert_to_bytes)
                | 'PS Hate Comments' >>
                beam.io.WriteToPubSub(topic=f'projects/{self.project}/topics/{self.output_topic}')
        )

        hate_comments_bq = (
                separated_results['Hate comments']
                | 'BQ Hate Comments' >> beam.io.WriteToBigQuery(table='hate_comments', dataset='yt_comments_analysis',
                                                                project=self.project)
        )

        normal_comments_bq = (
                separated_results['Normal comments']
                | 'BQ Norm Comments' >> beam.io.WriteToBigQuery(table='normal_comments', dataset='yt_comments_analysis',
                                                                project=self.project)
        )

    def run(self, runner: str):
        if runner == "direct":
            self.pipeline.run().wait_until_finish()
        else:
            self.pipeline.run()
