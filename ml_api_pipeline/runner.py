import argparse

from ml_api_pipeline.pipeline.pipeline import PipelineBuilder


def main():
    parser = argparse.ArgumentParser(description='Running Apache Beam pipelines on Dataflow')
    parser.add_argument('--project', type=str, required=True, help='Project id')
    parser.add_argument('--region', type=str, required=True, help='Region to run dataflow')
    parser.add_argument('--bucket', type=str, required=True, help='Name of the bucket to host dataflow components')
    parser.add_argument('--input-topic', type=str, required=True, help='input pubsub topic')
    parser.add_argument('--output-topic', type=str, required=True, help='output pubsub topic')
    parser.add_argument('--setup-file', type=str, required=True, help='setup file')
    parser.add_argument('--direct-runner', required=False, action='store_true')
    parser.add_argument('--dataflow-runner', required=False, action='store_true')
    args = parser.parse_args()

    pipeline_builder = PipelineBuilder(args=args)
    pipeline_builder.build()

    if args.direct_runner:
        pipeline_builder.run(runner='direct')
    elif args.dataflow_runner:
        pipeline_builder.run(runner='dataflow')
    else:
        raise ValueError("Invalid runner...")


if __name__ == '__main__':
    main()
