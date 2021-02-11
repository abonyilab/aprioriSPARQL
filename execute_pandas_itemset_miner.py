import sys
from tools import *
from glob import glob
import query_execution
import multiprocessing as mp
import pandas as pd
import itertools

def matching(pattern, matching_list):
    COUNT = 0
    for i in range(0,len(pattern)):
        if pattern[i] in matching_list:
            COUNT+=1
    return COUNT


def execute_task(cws:workstation, crun, tasks, prep_df, prep_df_sep ='\t'):
    cthread = str(mp.current_process().name)

    df_prep = pd.read_csv(prep_df, sep=prep_df_sep)
    df_prep = df_prep.groupby('s')['o'].apply(list).reset_index(name='o')

    scoring = pd.DataFrame()
    for t_i in tasks:
        cdata = df_prep.copy()
        cdata['COUNT'] = cdata.o.apply(lambda x: matching(t_i, x))

        crow = scoring.shape[0]

        for i in range(0, len(t_i)):
            scoring.loc[crow, 'extend_' + str(i)] = t_i[i]
        scoring.loc[crow, 'COUNT'] = cdata[cdata.COUNT == len(t_i)].shape[0]
    scoring = query_execution.post_execution_deduplication(scoring)
    scoring.to_csv(cws.dir_executed_queries + cthread + '_' + str(crun) + '.csv', index=False)

def main(argv):
    ws = workstation(argv[0])
    min_supp = 10000

    worker_count = mp.cpu_count() - 2
    print ('creating pool of workers: ', worker_count)
    pool = mp.Pool(worker_count)

    df_to_analyze = ws.dir_working_data + 'persondata_raw.csv'
    df = pd.read_csv(df_to_analyze, sep='\t')
    df = df.sort_values(by='s')

    prefrequent = df[['s', 'o']].groupby('o').count().sort_values('s', ascending=False).reset_index()
    prefrequent = prefrequent[(prefrequent.s >= min_supp) & (prefrequent.s < 1000000)]

    df_prep = df[df['o'].isin(prefrequent.o)].sort_values('s')
    df_prep = df_prep[['s', 'o']]
    df_prep.to_csv(ws.dir_working_data + 'df_prep.csv',index = False, sep = '\t')

    crun = 2
    while(1):
        if (crun <= 2):
            tasks = list(itertools.permutations(list(prefrequent.o), crun))

        if (crun > 2):
            collectables = glob(ws.dir_executed_queries + '*' + '_' + str(crun-1) + '.csv')

            new_df = pd.DataFrame()
            for c in collectables:
                new_df = pd.concat([new_df, pd.read_csv(c)])

            collected_cols = list(filter(lambda x: 'extend_' in x, new_df.columns))
            rematch_items = set()
            for col in collected_cols:
                rematch_items = rematch_items.union(set(new_df[col]))

            sel = [list(x) for x in [tuple(x) for x in new_df[list(filter(lambda x: 'extend_' in x, new_df.columns))].values]]

            tasks = []
            for prev_items in tqdm(sel):
                fragment = prev_items

                for candidate in rematch_items:
                    excluding = False

                    unique_combinations = []
                    permut = itertools.permutations(prev_items, crun-1)
                    for comb in permut:
                        zipped = zip(comb, [candidate])
                        unique_combinations.append(list(zipped))


                    combination_matches = 0
                    flag_exit_combination = False
                    for comb in unique_combinations:
                        comb = list(comb[0])
                        if candidate in comb:
                            break

                        for previously_is_frequent in sel:
                            co_appear = 0
                            for i in range(0, len(comb),1):
                                if comb[i] not in previously_is_frequent: continue
                                else:
                                    co_appear += 1
                                    #print('item match', co_appear)
                            if co_appear == len(comb):
                                #print ('combination match', combination_matches)
                                combination_matches += 1
                                if combination_matches >= len(unique_combinations):
                                    flag_exit_combination = True
                                    break

                        if combination_matches < len(unique_combinations):
                            excluding = True


                        if (flag_exit_combination): break

                    if not excluding:
                        fragment.append(candidate)
                        tasks.append(fragment)
                        print (len(tasks))


        print ('tasks: ', len(tasks))

        chunks = [tasks[x:x + int(len(tasks)/worker_count) + 1] for x in range(0,len(tasks), int(len(tasks)/worker_count) +1)]
        print('chunks: ', len(chunks))

        results = [pool.apply_async(execute_task, args=(ws, 2, t, ws.dir_working_data + 'df_prep.csv', '\t', )) for t in chunks]
        output = [p.get() for p in results]

        #collect
        collectables = glob(ws.dir_executed_queries + '*' + '_' + str(crun) + '.csv')

        new_df = pd.DataFrame()
        for c in collectables:
            new_df = pd.concat([new_df,pd.read_csv(c)])

        new_df = new_df[new_df.COUNT >= min_supp]
        new_df = query_execution.post_execution_deduplication(new_df)

        collected_cols = list(filter(lambda x: 'extend_' in x ,new_df.columns))
        rematch_items = set()
        for col in collected_cols:
            rematch_items = rematch_items.union(set(new_df[col]))

        prefrequent = pd.DataFrame(rematch_items)
        prefrequent.columns = ['o']
        crun += 1

        df_prep = df_prep[df_prep.o.isin(prefrequent.o)]
        df_prep.to_csv(ws.dir_working_data + 'df_prep.csv', index=False, sep='\t')

        if prefrequent.shape[0] < 1: break

if __name__ == '__main__':
    main(sys.argv[1:])
