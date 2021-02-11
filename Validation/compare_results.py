import pandas as pd
import tools
import numpy as np

sparql_results = pd.read_csv("/mnt/SSDCashe/WS_TEST/DBpedia_Test4/wdir/entity_stat.csv",sep='\t', index_col=0)

def cols_to_list(row, cols):
    l = []
    for col in cols:
        if (col not in ['min', 'max', 'mean']):
            l.append(row[col])
    return l

sparql_results['min'] = sparql_results.apply(lambda x: np.min(cols_to_list(x, sparql_results.columns)), axis=1)
sparql_results['max'] = sparql_results.apply(lambda x: np.max(cols_to_list(x, sparql_results.columns)), axis=1)
sparql_results['avg'] = sparql_results.apply(lambda x: np.mean(cols_to_list(x, sparql_results.columns)), axis=1)



