from dataclasses import dataclass
import requests
import codecs
import pandas as pd
codecs.register_error("strict", codecs.ignore_errors)


def sorting_helper(row, ordering_columns):
    return zip(ordering_columns, list(sorted(row[ordering_columns].values)))

def post_execution_deduplication(df):
    assert df.columns[-1] == 'COUNT'
    ordering_columns = df.columns[:-1]
    for idx, row in df.iterrows():
        for col, val in sorting_helper(row, ordering_columns):
            df.loc[idx, col] = val
    df = df.drop_duplicates()
    df = df.sort_values(by='COUNT', ascending = False)
    return df



@dataclass
class queryExecutor():
    endpoint: str
    queryparameters: dict()

    def __init__(self, endpoint, query, format='csv', **kwargs):
        self.endpoint = endpoint

        self.queryparameters = kwargs
        self.queryparameters['format'] = format
        self.queryparameters['query'] = query

    def execute(self, result_file, ignore_errors=[504]):

        while True:
            r = requests.get(self.endpoint, params=self.queryparameters)

            if r.status_code == 200:
                data = r.content.decode("utf-8")

                with open(result_file, 'w', encoding="utf-8") as out:
                    out.writelines(data)

                rf = pd.read_csv(result_file, encoding="utf-8")
                rf = post_execution_deduplication(rf)
                rf.to_csv(result_file, encoding="utf-8", index=False)
                return rf

            if r.status_code in ignore_errors:
                print('...retry...')

            else:
                print (r.status_code, r.content)
                return None


