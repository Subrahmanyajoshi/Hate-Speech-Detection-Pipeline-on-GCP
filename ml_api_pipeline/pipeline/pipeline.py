import argparse
from argparse import Namespace
from datetime import datetime

import apache_beam as beam
import google
from apache_beam import io
from apache_beam.ml.gcp import naturallanguageml as nlp
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DirectRunner, DataflowRunner

from ml_api_pipeline.pipeline.components import PipelineComponents


class PipelineBuilder(object):

    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.region = args.region
        self.input_topic = args.input_topic
        self.output_topic = args.output_topic

        # Setting up the Apache Beam pipeline options.
        self.options = pipeline_options.PipelineOptions(streaming=True, save_main_session=True)

        # Creating apache beam pipeline object
        self.pipeline = beam.Pipeline(options=self.options)

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
        features = nlp.types.AnnotateTextRequest.Features(extract_document_sentiment=True)

        (
            self.pipeline
            | 'Consume messages' >> io.gcp.pubsub.ReadFromPubSub(
                                    topic=f'projects/{self.project}/topics/{self.input_topic}')
            | 'get review' >> beam.Map(PipelineComponents.get_review)
            | 'Strip lines' >> beam.Map(PipelineComponents.strip_lines)
            | 'Remove emojis' >> beam.Map(PipelineComponents.remove_emojis)
            | 'convert to doc' >> beam.Map(PipelineComponents.convert_to_doc)
            | 'Call gcloud nlp api' >> nlp.AnnotateText(features)
            | 'process response' >> beam.Map(PipelineComponents.parse_response)
            | 'To result topic' >> beam.io.WriteToPubSub(topic=f'projects/{self.project}/topics/{self.input_topic}')
        )

    def run(self, runner: str):
        if runner == "direct":
            _ = DirectRunner().run_pipeline(self.pipeline, options=self.options).wait_until_finish()
        else:
            _ = DataflowRunner().run_pipeline(self.pipeline, options=self.options)
