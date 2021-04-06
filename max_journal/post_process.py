import json
from collections import Counter

path = '../resources/test.json-00000-of-00001'


def journal_max(file_path):
    """

    :type file_path: relative path to the file
    """
    f = open(file_path, 'r')

    data = f.readlines()

    journal_list = list()

    for drug in data:
        item = json.loads(drug)

        if item.get('journal'):
            jl = dict(journal=item['journal'], drug=item['drug'])
            if not jl in journal_list:
                journal_list.append(jl)

        if item.get('journal_clinical'):
            jl = dict(journal=item['journal_clinical'], drug=item['drug'])
            if not jl in journal_list:
                journal_list.append(jl)

    jrl = Counter(item['journal'] for item in journal_list)

    return jrl.most_common(1)[0]


print(journal_max(path))
