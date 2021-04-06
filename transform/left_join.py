from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import logging


class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, source_pipeline_name, source_data, second_pipeline_name, second_join_data, common_key):
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name

        self.second_join_data = second_join_data
        self.second_pipeline_name = second_pipeline_name

        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key].lower(), data_dict

        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name)
                                >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}

                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()

                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.second_pipeline_name
                                                   )
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, second_pipeline_name):
        group_key, grouped_dict = input_element

        second_dictionary = grouped_dict[second_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]

        for source_dictionary in source_dictionaries:
            try:
                if len(second_dictionary) > 0:
                    for second_item in second_dictionary:
                        source_dictionary.update(second_item)
                        yield source_dictionary
                else:
                    yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary
