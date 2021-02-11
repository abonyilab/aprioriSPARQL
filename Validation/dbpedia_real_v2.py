import sys
from tools import *
from glob import glob
import query_execution
import multiprocessing as mp
import pandas as pd
import itertools
import sympy
import numpy as np
min_supp = 10000

ws = workstation("/mnt/SSDCashe/WS_DBpedia_Offline_v2/")


df_to_analyze = ws.dir_working_data + 'persondata_raw.csv'
df = pd.read_csv(df_to_analyze, sep='\t')
df = df.sort_values(by='s')
df = df[['s', 'o']]

prefrequent = df[['s', 'o']].groupby('o').count().sort_values('s', ascending=False).reset_index()
prefrequent = prefrequent[(prefrequent.s >= min_supp) & (prefrequent.s < 1000000)]

df_prep = df[df['o'].isin(prefrequent.o)].sort_values('s')
df_prep = df_prep[['s', 'o']]
df_prep.to_csv(ws.dir_working_data + 'df_prep.csv', index=False, sep='\t')
#%%
df_prep_piv = pd.DataFrame(index = set(df_prep.s), columns=set(df_prep.o))
for idx, row in tqdm(df_prep.iterrows()):
    df_prep_piv.loc[row.s, row.o] = 1

df_prep_piv.to_csv(ws.dir_working_data + 'df_prep_piv.csv', sep='\t')
df_prep_piv = pd.read_csv(ws.dir_working_data + 'df_prep_piv.csv', sep='\t', index_col=0)
df_prep_piv['itemset_max_len'] = df_prep_piv[[col for col in df_prep_piv.columns if 'yago/' in col]].sum(axis=1)
df_prep_piv.to_csv(ws.dir_working_data + 'df_prep_piv_v2.csv', sep='\t')
df_prep_piv = pd.read_csv(ws.dir_working_data + 'df_prep_piv_v2.csv', sep='\t', index_col=0)


import os
crun = 2
while 1:
    print ('current run: ', crun, ' (starting)')
    argument_list = [ws.workstation_home, str(crun), str(10000)]
    os.system("python execute_pandas_itemset_mining_v2_goedel.py " + " ".join(argument_list))
    if crun >= 6:
        break

    crun += 1

