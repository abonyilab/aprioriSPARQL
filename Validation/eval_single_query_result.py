import codecs
import pandas as pd
from tqdm import tqdm
from query_template import *
import query_execution
import numpy as np
import sys
from glob import glob
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
codecs.register_error("strict", codecs.ignore_errors)

ws = workstation("/mnt/SSDCashe/WS_TEST/DBpedia_Test4/")
files = sorted(glob(ws.dir_results + "query_itemsetlen1_*"), key=lambda x: (int(x.split('/')[-1].split('.')[0].split('_')[-3]), int(x.split('/')[-1].split('.')[0].split('_')[-1])))

dfs = {}
prevTimeout = None
for f in files:
    ctimeout = int(f.split('/')[-1].split('.')[0].split('_')[-3])
    if prevTimeout is None: prevTimeout = ctimeout
    crun = int(f.split('/')[-1].split('.')[0].split('_')[-1])

    if ctimeout not in dfs.keys():
        dfs[ctimeout] = {}

    if crun not in dfs[ctimeout].keys():
        dfs[ctimeout][crun] = pd.read_csv(f, sep=',')


stat = pd.DataFrame()
stat_sums = pd.DataFrame()
for timeout in dfs.keys():
    collected_trans = []
    collected_counts = []

    for run in dfs[timeout].keys():
        collected_trans.append(dfs[timeout][run].shape[0])
        stat_sums.loc[stat_sums.shape[0], 'count'] = dfs[timeout][run]['COUNT'].sum()
        stat_sums.loc[stat_sums.shape[0]-1, 'value']  = str(run)+ "_run"
        stat_sums.loc[stat_sums.shape[0]-1, 'timeout'] = timeout

    stat.loc[str(timeout), 'min_collected'] = np.min(collected_trans)
    stat.loc[str(timeout), 'max_collected'] = np.max(collected_trans)
    stat.loc[str(timeout), 'std_deviation'] = np.std(collected_trans)

stat_sums.to_csv(ws.dir_working_data + "sum_items_plot.csv", sep='\t')
stat_sums.set_index(keys='timeout', drop=True, inplace=True)

#%%
import plotly.express as px
fig = px.line(stat,y='max_collected',labels={"index" : "timeout (ms)"
                                             ,'max_collected' : "collected objects"}
                    ,title="Collected objects")

fig.write_html(ws.dir_working_data + "max_collected.html")

#%%

import plotly.express as px
fig = px.line(stat_sums,y='count', color='value', labels={"index" : "timeout (ms)"
                                             ,'count' : "sum of collected transactions"}
                    ,title="Collected objects")

fig.write_html(ws.dir_working_data + "sum_collected.html")


#%%

diff_stat_v2 = pd.DataFrame(columns=['name', 'count', 'timeout'])

diff_stat = pd.DataFrame()
for timeout in dfs.keys():
    for idx, row in dfs[timeout][1].iterrows():
        diff_stat.loc[row.extend0, timeout] = row.COUNT
        diff_stat_v2.loc[diff_stat_v2.shape[0]] = (row.extend0, row.COUNT, timeout)

diff_stat.fillna(0, inplace=True)
diff_stat.to_csv(ws.dir_working_data + 'entity_stat.csv', sep='\t')


#%%
import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly.tools import make_subplots
layout=go.Layout(
    title='<b>Counts of individual objects</b>',
    yaxis=dict(
        title='Individual object',
        showgrid=False
    ),
    xaxis=dict(
        title='Count'
    ),
    showlegend=False,
    margin=dict(
        l=150
    ),
    plot_bgcolor='#000000',
    paper_bgcolor='#000000',
    font=dict(
        family='Segoe UI',
        color='#ffffff'
    )
)

data=[]
for name in set(diff_stat_v2.name):
    data.append(
        go.Box(x = diff_stat_v2[diff_stat_v2.name == name]['count'], name = name.split('/')[-1])
        )
figure = go.Figure(data=data, layout=layout)
figure.write_html(ws.dir_working_data + "box_plot.html")#%%