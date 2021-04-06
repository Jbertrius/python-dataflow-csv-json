import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from transform.data_ingestion_ import create_key_value_pair
from options import pipeline_options_def
from apache_beam.io import ReadFromText
from transform.data_ingestion_ import DataIngestion
from apache_beam.pvalue import AsList
import json
from apache_beam.options.pipeline_options import SetupOptions
from transform.left_join import LeftJoin


def run(argv=None, save_main_session=True):
    pipeline_options, known_args = pipeline_options_def(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a Json file.

    data_ingestion_drug = DataIngestion(['atccode', 'drug'])
    data_ingestion_pubmed = DataIngestion(['id', 'title', 'date', 'journal'])
    data_ingestion_clinicals = DataIngestion(['id_clinical', 'scientific_title', 'date_clinical', 'journal_clinical'])

    p = beam.Pipeline(options=pipeline_options)

    # Drugs data get from drugs.csv file, then the data are filtered and mapped to json
    drugs = (p | 'Read from a File Drugs' >> ReadFromText(known_args.input_drug, skip_header_lines=1)
             | 'Map drugs to json' >> beam.Map(lambda s: data_ingestion_drug.parse_method(s))
             | 'filter empty lines from drugs data' >> beam.Filter(lambda x: x)
             )

    # Transfom drug data into Side Input who will pass into pubmed and clinical trials data
    drugs_as_list = drugs | beam.Map(lambda item: item['drug'].lower())

    # Merge PubMed csv file and json file together
    pubmed = (p | 'Read from a pubmed file' >> ReadFromText(known_args.input_pubmed, skip_header_lines=1)
              | 'Map pubmed to json' >> beam.Map(lambda s: data_ingestion_pubmed.parse_method(s))
              | 'detect drug into the csv file title'
              >> beam.FlatMap(create_key_value_pair, AsList(drugs_as_list), 'title')
              )

    pubmedv2 = (p | 'Read from a pubmed json' >> beam.Create(
        json.loads(FileSystems.open(known_args.input_pubmed_json).read()))
                | 'detect drug into the json file title' >> beam.FlatMap(create_key_value_pair, AsList(drugs_as_list),
                                                                         'title'))

    merged_pubmed = ((pubmed, pubmedv2) | 'Merge Json and CSV pubmed' >> beam.Flatten()
                     | beam.Filter(lambda x: x))

    # # Join drug data and pubmed data
    # Source Data
    source_pipeline_name = 'drug_data'

    # Join Data
    join_pipeline_name = 'pub_data'

    pipelines_dictionary = {source_pipeline_name: drugs,
                            join_pipeline_name: merged_pubmed}

    drugs_pubmed_data = (pipelines_dictionary
                         | 'Join' >> LeftJoin(source_pipeline_name, drugs, join_pipeline_name, merged_pubmed, 'drug')
                         )

    # Get Clinical Trials Data
    clinical_trials = (p | 'Read from a Clinical Trials File' >> ReadFromText(known_args.input_clinical_trials,
                                                                              skip_header_lines=1)
                       | 'Map clinical trials to json' >> beam.Map(lambda s: data_ingestion_clinicals.parse_method(s))
                       | 'detect drug in trial' >> beam.FlatMap(create_key_value_pair, AsList(drugs_as_list),
                                                                'scientific_title')
                       | beam.Filter(lambda x: x)
                       )

    # # Join all the data
    # Drug and Pubmed data
    source_pipeline_name_v2 = 'drug_pub_data'

    # Clinical trials Data
    third_pipeline_name = 'clinical_data'

    pipelines_dictionary_v2 = {source_pipeline_name_v2: drugs_pubmed_data,
                               third_pipeline_name: clinical_trials}

    last_join = (pipelines_dictionary_v2
            | 'Joinv2' >> LeftJoin(source_pipeline_name_v2, drugs_pubmed_data, third_pipeline_name, clinical_trials,
                                   'drug')
            )

    (last_join | 'convert to json' >> beam.Map(json.dumps)
     | 'convert to txt' >> beam.io.WriteToText(known_args.output, num_shards=1)
     )

    p.run().wait_until_finish()
