import codecs
import pandas as pd
from tqdm import tqdm
from query_template import *
import query_execution
import numpy as np
import sys
import multiprocessing as mp
codecs.register_error("strict", codecs.ignore_errors)

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
config_frame = pd.DataFrame()
config_frame.loc[0, 'min_supp'] = 10000
config_frame.loc[0, 'max_itemset_len'] = 20
config_frame.loc[0, 'endpoint'] = 'http://dbpedia.org/sparql'
config_frame.loc[0, 'format'] = 'csv'

ws = workstation("/mnt/SSDCashe/WS_TEST/DBpedia_Test4/")
def execute_task(to, crun):

    exec = query_execution.queryExecutor(endpoint=config_frame.iloc[0].endpoint, query=query,
                                             min_supp=config_frame.iloc[0].min_supp, format=config_frame.iloc[0].format,
                                             timeout=to)
    exec.execute(result_file=ws.dir_results + "query_itemsetlen1_" + str(to) + '_r_' + str(crun) + '.csv')
    print ('query executed', to, '-', crun)

def main():
    worker_count = 45
    pool = mp.Pool(worker_count)
    timeouts = list(np.arange(1000, 1000000, 10000))
    for run in range(1,10,1):
        results = [pool.apply_async(execute_task, args=(to,run, )) for to in timeouts]
        output = [p.get() for p in results]

main()