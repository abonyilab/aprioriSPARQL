import tools
import pandas as pd
dbpedia_dir = "/mnt/Work/DB/Dbpedia/"

file_person_data_raw = dbpedia_dir + 'persondata_wkd_uris_en.ttl.bz2'

file_instance_types_raw = dbpedia_dir + 'instance_types_wkd_uris_en.ttl.bz2'
file_trans_instance_types_raw = dbpedia_dir + 'instance_types_transitive_wkd_uris_en.ttl.bz2'
file_yago_vocab_raw = dbpedia_dir +'yago_links.nt.bz2'
file_yago_types_raw = dbpedia_dir + 'yago_types.nt.bz2'
#file_categ_labels_raw = dbpedia_dir + 'article_categories_wkd_uris_en.ttl.bz2'
ws = tools.workstation("/mnt/SSDCashe/WS_DBpedia_Offline/")
#%%

personsdata = pd.DataFrame()
for df in tools.read_rdf(file_instance_types_raw):
    personsdata = pd.concat([personsdata, df[df.o.str.contains('Person')]])

for df in tools.read_rdf(file_trans_instance_types_raw):
    break
#%%
persons = pd.DataFrame()
for df in tools.read_rdf(file_yago_types_raw):
   persons = pd.concat([persons, df[df.s.isin(df[df.o.str.contains('Person')].s)]])

persons.to_csv(ws.dir_working_data + 'persondata_raw.csv', index=False, sep='\t')
#%%
persons = pd.read_csv(ws.dir_working_data + 'persondata_raw.csv', sep='\t')
persons = persons.sort_values(by='s')

persons_freq_prep = persons[['s', 'o']].groupby('s')['o'].apply(list).reset_index(name='o')
#%%

prefrequent = persons[['s', 'o']].groupby('o').count().sort_values('s', ascending=False).reset_index()
prefrequent = prefrequent[(prefrequent.s > 10000) & (prefrequent.s < 1000000)]

def matching(pattern, matching_list):
    score = 0

    for i in range(0,len(pattern)):

        if pattern[i] in matching_list:
            score+=1

    return score

scoring = pd.DataFrame()
import itertools
from tqdm import tqdm
for t_i in tqdm(itertools.permutations(list(prefrequent.o), 2)):
    cdata = persons_freq_prep.copy()
    cdata['score'] = cdata.o.apply(lambda x : matching(t_i, x))

    crow = scoring.shape[0]
    for i in range(0,len(t_i)):
        scoring.loc[crow, 'extend_' + str(i)] = t_i[i]
    scoring.loc[crow, 'score'] = cdata[cdata.score >= 10000].shape[0]

#%%
persons_itemset = persons[['s', 'o']].groupby('s')['o'].apply(list).reset_index()

import os
argument_list = [ws.workstation_home]
os.system("python execute_pandas_itemset_miner.py " + " ".join(argument_list))
