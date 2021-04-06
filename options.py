import argparse
from apache_beam.options.pipeline_options import PipelineOptions


def pipeline_options_def(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_drug',
        dest='input_drug',
        default='resources/drugs.csv',
        help='Input file Drug to process.')

    parser.add_argument(
        '--input_pubmed',
        dest='input_pubmed',
        help='Input file PubMed to process.')

    parser.add_argument(
        '--input_pubmed_json',
        dest='input_pubmed_json',
        help='Input file PubMed Json to process.')

    parser.add_argument(
        '--input_clinical_trials',
        dest='input_clinical_trials',
        help='Input file Clinical Trial to process.')

    parser.add_argument(
        '--output',
        dest='output',
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([

        '--runner=DirectRunner',

        '--project=SET_YOUR_PROJECT_ID_HERE',

        '--region=SET_REGION_HERE',

        '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',

        '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',

        '--job_name=your-wordcount-job',
    ])

    return PipelineOptions(pipeline_args), known_args
