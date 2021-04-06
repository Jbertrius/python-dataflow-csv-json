import csv


class DataIngestion:
    """A helper class which contains the logic to translate the file into a
   json format will accept."""

    def __init__(self, field_map):
        self.field_map = field_map

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
    dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
            state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                example string_input: KS,F,1923,Dorothy,654,11/28/2016
        Returns:
        """

        # Use a CSV Reader which can handle quoted strings etc.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:

            row = {}
            i = 0
            # Iterate over the values from our csv file, applying any transformation logic.
            if len(csv_row) == len(self.field_map):
                for value in csv_row:
                    row[self.field_map[i]] = value

                    i += 1
                # print(row)

                return row


def create_key_value_pair(pubmed, druglist, key):
    """

    :type pubmed: dict
    :type druglist: list - list of drugs name
    :type key: string
    """
    for drugname in druglist:
        if drugname in pubmed[key].lower():
            pubmed['drug'] = drugname
            yield pubmed
