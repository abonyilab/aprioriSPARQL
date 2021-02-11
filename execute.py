import sys
from tools import *
from glob import glob
import query_execution
import multiprocessing as mp
import pandas as pd
#pattern: workstation, endpoint, iteration

def execute_task(cws, query_path, endpoint):
    config_frame = pd.read_csv(cws.dir_config + 'execution_config.csv', sep=';')
    currentlog = cws.dir_logs + 'execution_' + str(mp.current_process().name) + '.log'

    try:
        with open(currentlog, 'a', encoding='utf-8') as out:
            out.write('--------------------------------------\n')
            out.write('query_path:' + '\t' + query_path + '\n')

        current_file = query_path.split('/')[-1].split('.')[0]

        info_file = cws.dir_queued_queries + current_file + '.info'

        with open(query_path, 'r') as ins:
            q = '\n'.join(ins.readlines())


        info = pd.read_csv(info_file)

        with open(currentlog, 'a', encoding='utf-8') as out:
            out.write('Query: ' + q)
            out.write('Info file shape: ' + str(info.shape))

        qe = query_execution.queryExecutor(endpoint, q, min_supp=config_frame.iloc[0].min_supp,  format=config_frame.iloc[0].format, timeout=config_frame.iloc[0].timeout) #name = ''
        qe.execute(result_file= cws.dir_query_results + current_file + '.csv')

        df = pd.read_csv(cws.dir_query_results + current_file + '.csv')
        for ext_col in info.columns:
            df[ext_col] = info.loc[0, ext_col]

        df = df[list(sorted(filter(lambda x: 'extend' in x, df.columns))) + ['COUNT']]
        df.to_csv(cws.dir_query_results + current_file + '.csv', index=False)

    except Exception as err:
        with open(currentlog, 'a', encoding='utf-8') as out:
            out.write('FAILED --------------------------------------\n')
            out.write('query_path:' + '\t' + query_path + '\n')
            out.write(str(err) + '\n')

def main(argv):
    print ('Multi thread execution is running...')

    ws = workstation(argv[0])
    endpoint = argv[1]
    current_iteration = int(argv[2])
    current_queries = glob(ws.dir_queued_queries + str(current_iteration) + '_*.rq')
    worker_count = 10
    pool = mp.Pool(worker_count)

    with open(ws.dir_logs + 'main_execution.log', 'a', encoding= 'utf-8') as out:

        out.write('---------------- Current iteration:' +  str(current_iteration) + '------------------\n')
        out.write('Workstation:' + ws.workstation_home + '\n')
        out.write('Target endpoint:' + endpoint + '\n')
        out.write('Target result:' + ws.dir_results + str(current_iteration+1) + '_agg_res.csv' + '\n')
        out.write('Current queued queries:' + str(len(current_queries)))
        out.write('\n\n')
        out.write('Multithread execution: ' + str(worker_count) + ' monitors\n')

    results = [pool.apply_async(execute_task, args=(ws,query,endpoint, )) for query in current_queries]
    output = [p.get() for p in results]

    query_results = glob(ws.dir_query_results + str(current_iteration) + '*.csv')

    with open(ws.dir_logs + 'main_execution.log', 'a', encoding= 'utf-8') as out:
        out.write('Results: ' + str(len(query_results)) + ' result files\n')

    df_result = pd.DataFrame()

    for f in query_results:
        try:
            df_result = pd.concat([df_result, pd.read_csv(f)])
        except Exception as err:
            with open(ws.dir_logs + 'main_execution.log', 'a', encoding='utf-8') as out:
                out.write ('FAILED CONCAT-----------------\n')
                out.write('\n' + str(err) + '\n')

    df_result = query_execution.post_execution_deduplication(df_result)
    df_result.to_csv(ws.dir_results + str(current_iteration+1) + '_agg_res.csv', index=False)

    with open(ws.dir_logs + 'main_execution.log', 'a', encoding= 'utf-8') as out:
        out.write('End of result aggregation \n')

if __name__ == '__main__':
    main(sys.argv[1:])
