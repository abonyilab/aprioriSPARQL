import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from tools import *
from glob import glob
import itertools
import numpy as np
ws = workstation('/mnt/SSDCashe/WS/VALIDATION_RUN/')
#%%Explore all run versions
wss = [workstation(x + '/') for x in list(sorted(list(filter(lambda x: 'v' not in x.split('_')[-1],glob('/mnt/SSDCashe/WS/DBpedia_*'))), key=lambda x: int(x.split('_')[-1])))]

#%% Exploring duplicates
execution_exception_token = False
level_results = {}
for ws_a, ws_b in itertools.product(wss, repeat=2):
    cws_name_a = ws_a.workstation_home.split('/')[-2].split('_')[-1]
    cws_name_b = ws_b.workstation_home.split('/')[-2].split('_')[-1]
    files_in_a = [f.split('/')[-1] for f in sorted(glob(ws_a.dir_results + '*.csv'), key=lambda x: int(x.split('/')[-1].split('_')[0]))]
    files_in_b = [f.split('/')[-1] for f in sorted(glob(ws_b.dir_results + '*.csv'), key=lambda x: int(x.split('/')[-1].split('_')[0]))]
    co_files = set(files_in_a).intersection(set(files_in_b))
    try:
        max_co_len = np.max([int(cf.split('_')[0]) for cf in co_files])
    except ValueError:
        max_co_len = 0

    if 'max_co_len' not in level_results.keys():
        level_results['max_co_len'] = pd.DataFrame()

    level_results['max_co_len'].loc[cws_name_a, cws_name_b] = max_co_len

    for i in range(1, max_co_len+1):

        if str(i) + '_level_cnt_difference' not in level_results.keys():
            level_results[str(i) + '_level_cnt_difference'] = pd.DataFrame()

        clevel = str(i) + '_agg_res.csv'

        dfa = pd.read_csv(ws_a.dir_results + clevel)
        dfb = pd.read_csv(ws_b.dir_results + clevel)


        level_results[str(i) + '_level_cnt_difference'].loc[cws_name_a, cws_name_b] = np.abs(dfa.shape[0] - dfb.shape[0])



        if cws_name_a == cws_name_b:
            check_or_create_dir(ws.dir_working_data + "same_timestamp_execution_differences/")
            duplicates = dfa[dfa.duplicated(list(filter(lambda x : 'extend' in x, dfa.columns)), keep=False)].sort_values(by = list(filter(lambda x : 'extend' in x, dfa.columns)))

            if duplicates.shape[0] < 1: continue

            with open(ws.dir_working_data + "same_timestamp_execution_differences/" + cws_name_a + '_lvl_' + str(i) +'.txt', 'w') as out:
                prev_string = None

                for idx, row in duplicates.iterrows():
                    ccols = []
                    for col in list(filter(lambda x : 'extend' in x, dfa.columns)):
                        ccols.append(row[col])
                    cstring = '_'.join(ccols)

                    if prev_string is None:
                        out.write('\n' + cstring)
                        prev_string = cstring

                    if prev_string == cstring:
                        out.write(',' + str(row['COUNT']))

                    else:
                        prev_string = cstring
                        out.write('\n' + cstring)
                        out.write(',' + str(row['COUNT']))



        else:
            check_or_create_dir(ws.dir_working_data + "diff_timestamp_execution_differences/")

            if dfa.shape[0] < 1 or dfb.shape[0] < 1: continue

            dfc = pd.concat([dfa.drop_duplicates(list(filter(lambda x : 'extend' in x, dfa.columns)), keep='first')
                            ,dfb.drop_duplicates(list(filter(lambda x : 'extend' in x, dfb.columns)), keep='first')])
            duplicates = dfc.sort_values(by=list(filter(lambda x: 'extend' in x, dfc.columns)))


            if duplicates.shape[0] < 1: continue
            with open(ws.dir_working_data + "diff_timestamp_execution_differences/" + cws_name_a + "_" + cws_name_b + '_lvl_' + str(i) + '.txt', 'w') as out:

                prev_string = None
                for idx, row in duplicates.iterrows():
                    ccols = []
                    for col in list(filter(lambda x : 'extend' in x, duplicates.columns)):
                        ccols.append(row[col])
                    cstring = '_'.join(ccols)

                    if prev_string is None:
                        out.write('\n' + cstring)
                        prev_string = cstring

                    if prev_string == cstring:
                        out.write(',' + str(row['COUNT']))

                    else:
                        prev_string = cstring
                        out.write('\n' + cstring)
                        out.write(',' + str(row['COUNT']))

#%%Statistics

stat_mean_difference = {}

same_level_diffs_files = sorted(glob(ws.dir_working_data + "same_timestamp_execution_differences/" + "*.txt"), key=lambda x : int(x.split('/')[-1].split('.')[0].split('_')[0]))
diff_level_diff_files = sorted(glob(ws.dir_working_data + "diff_timestamp_execution_differences/" + "*.txt"), key=lambda x : int(x.split('/')[-1].split('.')[0].split('_')[0]))

for f in same_level_diffs_files:
    current_level = f.split('/')[-1].split('.')[0].split('_')[-1]
    current_execution_analysis = f.split('/')[-1].split('.')[0].split('_')[0]
    counts = []

    with open(f, 'r') as ins:
        for l in nonblank_lines(ins):
            if (len(l.split(','))) <= 2: continue
            counts.append(np.mean(np.abs(np.diff([int(i) for i in l.split(',')[1:]]))))

    if current_level not in stat_mean_difference.keys():
        stat_mean_difference[current_level] = pd.DataFrame()

    stat_mean_difference[current_level].loc[current_execution_analysis,current_execution_analysis] = np.mean(counts)


for f in diff_level_diff_files:
    exa, exb = f.split('/')[-1].split('.')[0].split('_')[0], f.split('/')[-1].split('.')[0].split('_')[1]
    current_level = f.split('/')[-1].split('.')[0].split('_')[-1]

    counts = []
    with open(f, 'r') as ins:
        for l in nonblank_lines(ins):
            if (len(l.split(','))) <= 2: continue
            counts.append(np.mean(np.abs(np.diff([int(i) for i in l.split(',')[1:]]))))

    if current_level not in stat_mean_difference.keys():
        stat_mean_difference[current_level] = pd.DataFrame()


    stat_mean_difference[current_level].loc[exa, exb] = np.mean(counts)
    stat_mean_difference[current_level].loc[exb, exa] = np.mean(counts)

    ilist = stat_mean_difference[current_level].index.tolist()
    c = sorted(ilist, key=lambda x: int(x))

    stat_mean_difference[current_level] = stat_mean_difference[current_level].reindex(c)
    stat_mean_difference[current_level] = stat_mean_difference[current_level].reindex(sorted(stat_mean_difference[current_level].columns, key=lambda x: int(x)), axis=1)

#%%
for k in stat_mean_difference.keys():
    stat_mean_difference[k].to_csv(ws.dir_results + k + '.csv', sep=';')