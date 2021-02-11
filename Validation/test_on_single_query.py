import codecs
from glob import glob
import pandas as pd
from tqdm import tqdm
from query_template import *
import query_execution
import numpy as np
import sys
import multiprocessing as mp
codecs.register_error("strict", codecs.ignore_errors)

ws = workstation("/mnt/SSDCashe/WS_TEST/DBpedia_Test/")
query = """
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX : <http://dbpedia.org/resource/>
PREFIX dbpedia2: <http://dbpedia.org/property/>
PREFIX dbpedia: <http://dbpedia.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT * {
            {
                SELECT
                    ?extend0 (COUNT(DISTINCT ?base_entity) AS ?COUNT)
                WHERE
                {
                    ?base_entity rdf:type <http://xmlns.com/foaf/0.1/Person> .
                    ?base_entity rdf:type ?extend0 .
                    FILTER regex(STR(?extend0), "yago", "i") .
                }
                GROUP BY ?extend0
                HAVING (COUNT(DISTINCT ?base_entity) >= 10000)
            }
        }        
        ORDER BY DESC(?COUNT)
"""

for timeout in tqdm(np.arange(100000, 1000000, 10000)):
    config_frame = pd.DataFrame()
    config_frame.loc[0, 'min_supp'] = 10000
    config_frame.loc[0, 'max_itemset_len'] = 20
    config_frame.loc[0, 'endpoint'] = 'http://dbpedia.org/sparql'
    config_frame.loc[0, 'format'] = 'csv'
    config_frame.loc[0, 'timeout'] = timeout
    exec = query_execution.queryExecutor(endpoint=config_frame.iloc[0].endpoint, query=query,
                                         min_supp=config_frame.iloc[0].min_supp, format=config_frame.iloc[0].format,
                                         timeout=config_frame.iloc[0].timeout)
    exec.execute(result_file=ws.dir_results + "query_itemsetlen1_" + str(timeout) +'.csv')

#%%
import plotly.express as px
df_collection = {}

for f in glob(ws.dir_results + 'query_itemsetlen1_*.csv'):
    ctimeout = f.split('.')[0].split('_')[-1]
    if ctimeout not in df_collection.keys():
        df_collection[ctimeout] = pd.read_csv(f)

stat = pd.DataFrame()