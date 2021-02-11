import sys
from tools import *
from glob import glob
import query_execution
import multiprocessing as mp
import pandas as pd
import itertools
import sympy
import numpy as np

def sorting_helper(row, ordering_columns):
    return zip(ordering_columns, list(sorted(row[ordering_columns].values)))

def frequent_itemset_len(df:pd.DataFrame, matching_items):
    df_items = df[list(matching_items)].dropna(axis=0)
    return df_items.shape[0]

def execute_task(cws:workstation, crun, tasks, prep_df, prep_df_sep ='\t'):
    cthread = str(mp.current_process().name)
    df_prep_piv = pd.read_csv(cws.dir_working_data + prep_df, sep = prep_df_sep, index_col=0)
    print ('(THREAD REPORT) ', cthread, ' TIDY DATASET: ', df_prep_piv.shape)

    df_prep_piv = df_prep_piv[df_prep_piv['itemset_max_len'] >= crun]
    print ('(THREAD REPORT) ', cthread, ' TIDY DATASET (step1): ', df_prep_piv.shape)

    df_prep_piv = df_prep_piv[list(filter(lambda x: 'itemset_max_len' not in x, df_prep_piv.columns))]
    print('(THREAD REPORT) ', cthread, ' TIDY DATASET (step2): ', df_prep_piv.shape)

    needed_columns = set()
    print('(THREAD REPORT) ', cthread, ' Needed cols OK ')

    for t in tasks:
        needed_columns = needed_columns.union(set(list(t)))

    print('task needs: ', len(needed_columns))

    df_prep_piv = df_prep_piv[list(filter(lambda x: x in needed_columns, df_prep_piv.columns))]
    print('(THREAD REPORT) ', cthread,  ' TIDY DATASET - COMPLETED ', df_prep_piv.shape)

    df_res = pd.DataFrame()
    iteration_counter = 0
    print('(THREAD REPORT) ', cthread, '  ', iteration_counter, '/', len(tasks))
    for t in tasks:

        cshape = df_res.shape[0]
        for i in range(0,len(t)):
            df_res.loc[cshape, 'extend_' + str(i)] = t[i]

        df_res.loc[cshape, 'COUNT'] = frequent_itemset_len(df_prep_piv, t)
        iteration_counter+=1

        if iteration_counter % 50 ==0:
            print('(THREAD REPORT) ', cthread, '  ', iteration_counter, '/', len(tasks))

    print('(THREAD REPORT)', cthread, '_' + str(crun), '- FINISHED ALL TASKS')
    df_res.to_csv(cws.dir_executed_queries + cthread + '_' + str(crun) + '.csv', index=False)
    print ('(THREAD EXIT)', cthread, '_' + str(crun))

def vote_helper(cname, prev_names):
    counter = 0
    for pn in prev_names:
        if pn in cname:
            counter+=1
    return counter


def sort_values(df):
    print (df.shape)
    for idx, row in df.iterrows():
        for col, val in sorting_helper(row, df.columns):
            df.loc[idx, col] = val
    df = df.drop_duplicates()
    return df

def named_values(df):
    df['name'] = df.apply(lambda x: '_'.join([x[col] for col in df.columns]), axis=1)
    return df

def vote_on_name(df):
    pass

def parallelize_dataframe(df, func, n_cores=24):
    df_split = np.array_split(df, n_cores)
    pool = mp.Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def main(argv):
    ws = workstation(argv[0])
    crun = int(argv[1])
    min_supp = int(argv[2])
    worker_count = 24
    df_prep_piv = pd.read_csv(ws.dir_working_data + 'df_prep_piv_v2.csv', sep='\t', index_col=0)
    df_prep_piv = df_prep_piv[list(filter(lambda x: 'itemset_max_len' not in x, df_prep_piv.columns))]
    if (crun > 2):
        new_df = pd.read_csv(ws.dir_results + 'result_itemset_len_' + str(crun-1) + '.csv')

        collected_cols = list(filter(lambda x: 'extend_' in x, new_df.columns))
        rematch_items = set()
        for col in collected_cols:
            rematch_items = rematch_items.union(set(new_df[col]))

        df_prep_piv = pd.read_csv(ws.dir_working_data + 'df_prep_piv_v2.csv', sep='\t', index_col=0)
        df_prep_piv = df_prep_piv[list(rematch_items)]
        df_prep_piv['itemset_max_len'] = df_prep_piv[[col for col in df_prep_piv.columns if not 'itemset_max_len' in col]].sum(axis=1)
        df_prep_piv = df_prep_piv[df_prep_piv['itemset_max_len'] >= int(crun)]
        df_prep_piv = df_prep_piv.drop(columns = ['itemset_max_len'])
        df_prep_piv = df_prep_piv.dropna(how='all', axis=1)
        print ('creating tasks')
        tasks = pd.DataFrame(list(itertools.permutations(list(df_prep_piv.columns), crun)))
        print ('task count:', tasks.shape[0], '(eliminating using previous results)')

        tasks = parallelize_dataframe(tasks, sort_values)
        print('task sorting done')

        tasks = tasks.drop_duplicates()
        tasks = parallelize_dataframe(tasks, named_values)
        print('nameing done')

        #tasks['name'] = tasks.apply(lambda x: '_'.join([x[col] for col in tasks.columns]), axis=1)
        new_df['name'] = new_df.apply(lambda x: '_'.join([x[col] for col in new_df.columns if 'extend_' in col]), axis=1)
        itemset_fragment_names = list(new_df['name'])
        tasks['vote'] = tasks['name'].apply(lambda x: vote_helper(x, itemset_fragment_names))
        tasks = tasks[tasks['vote'] > 0]
        tasks_prep = tasks[tasks.columns[:-2]]
        tasks =  [tuple(x) for x in tasks_prep.to_numpy()]
        print('(done) task count:', len(tasks))
    else:
        tasks = list(itertools.permutations(list(df_prep_piv.columns), crun))

    print('tasks: ', len(tasks))
    chunks = [tasks[x:x + int(len(tasks) / worker_count) + 1] for x in range(0, len(tasks), int(len(tasks) / worker_count) + 1)]
    print('chunks: ', len(chunks))

    print('creating pool of workers: ', worker_count)
    pool = mp.Pool(worker_count)
    print('pool initalized: ', worker_count)

    results = [pool.apply_async(execute_task, args=(ws, crun, t, 'df_prep_piv_v2.csv', '\t',)) for t in chunks]
    output = [p.get() for p in results]
    print('processess exited, now collecting results')

    collectables = glob(ws.dir_executed_queries + '*' + '_' + str(crun) + '.csv')
    new_df = pd.DataFrame()
    for c in collectables:
        new_df = pd.concat([new_df, pd.read_csv(c)])

    new_df = new_df[new_df.COUNT >= min_supp]
    new_df = query_execution.post_execution_deduplication(new_df)
    new_df.to_csv(ws.dir_results + 'result_itemset_len_' + str(crun) + '.csv', index=False)
    del pool
    print('results written...')

#pattern: ws_home, current_iteration
if __name__ == '__main__':
    main(sys.argv[1:])