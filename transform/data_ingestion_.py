import csv


class DataIngestion:
    """A helper class which contains the logic to translate the file into a
  format BigQuery will accept."""

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
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.  In this example, the data is not transformed, and
            remains in the same format as the CSV.  There are no date format transformations.
                example output:
                      {'state': 'KS',
                       'gender': 'F',
                       'year': '1923-01-01', <- This is the BigQuery date format.
                       'name': 'Dorothy',
                       'number': '654',
                       'created_date': '11/28/2016'
                       }
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
    for drugname in druglist:
        if drugname in pubmed[key].lower():
            pubmed['drug'] = drugname
            yield pubmed
