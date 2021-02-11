import codecs
import pandas as pd
from query_template import *
import query_execution

codecs.register_error("strict", codecs.ignore_errors)

ws = workstation("/mnt/SSDCashe/WS/GRAPHDB_v3/")  # set the execution path
endpoint = 'http://localhost:7200/repositories/WH_2'  # set the queried endpoint
max_itemset_len = 20 #Set max iteration
min_supp = 2 #Set min supp
format='csv' #Don't touch it, except endpoint wont provide CSV
timeout=0 #Timeout
current_header = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
"""

config_frame = pd.DataFrame()
config_frame.loc[0, 'min_supp'] = min_supp
config_frame.loc[0, 'max_itemset_len'] = max_itemset_len
config_frame.loc[0, 'endpoint'] = endpoint
config_frame.loc[0, 'format'] = format
config_frame.loc[0, 'timeout'] = timeout
config_frame.to_csv(ws.dir_config + 'execution_config.csv', sep=';', index=False)

# %%

#Set it
counting_variable = variable(id = None
                             , name = 'base_entity'
                             , type = '<http://abonyilab.com/wireharness_ontology/Product>'
                             , unique_property_selector = None
                             , unique_property_value = None)

#Set it
introduced_variable = variable(id = None
                               , name = 'extend'
                               , type = '<http://abonyilab.com/wireharness_ontology/Module>'
                               , unique_property_selector = None
                               , unique_property_value = None
                               , counted_variable = True
                               , counted_variable_id = 0
                               , optional_selector= '')
#Set it
base_connector = variable_connector(counting_variable, introduced_variable, '<http://abonyilab.com/wireharness_ontology/has_Module>')

b = sparql_apriori_template(current_header, min_supp=config_frame.iloc[0].min_supp)
b.variables.append(counting_variable)
b.variables.append(introduced_variable)
b.connectors.append(base_connector)
b.counting_variable = counting_variable
b.introduced_variable = introduced_variable
exec = query_execution.queryExecutor(endpoint=config_frame.iloc[0].endpoint,
                                     query=b.compile(),
                                     name = '',
                                     min_supp=config_frame.iloc[0].min_supp,
                                     format=config_frame.iloc[0].format)
                                     #timeout=config_frame.iloc[0].timeout)
exec.execute(result_file=ws.dir_results + '1_agg_res.csv')

#%%
df = pd.read_csv(ws.dir_results + '1_agg_res.csv')
df['itemset_length'] = df.apply(lambda x: len(set(x[df.columns[:-1]].values)), axis=1)
previous_max_length = df.itemset_length.max()
for itemset_len in range(1, max_itemset_len):
    print ('Current iteraton: ', itemset_len)
    df = pd.read_csv(ws.dir_results + str(itemset_len) + '_agg_res.csv')
    df = df[df.COUNT >= min_supp]
    df = df.reset_index(drop=True)

    introduced_variable.counted_variable_id = itemset_len
    for idx, row in df.iterrows():
        cstatement = sparql_apriori_template(current_header, min_supp=config_frame.iloc[0].min_supp)
        cstatement.variables.append(counting_variable)

        variable_id = 0
        df_extending = pd.DataFrame()
        for col in list(df.columns[:-1]):
            fixed_variable = variable(
                id = '<' + row[col] + '>'
                , name = introduced_variable.name
                , type = introduced_variable.type
                , unique_property_selector=introduced_variable.unique_property_selector
                , unique_property_value=introduced_variable.unique_property_value
                , counted_variable=introduced_variable.is_counted_variable
                , counted_variable_id=variable_id)

            df_extending.loc[0, 'extend' + str(variable_id)] = row[col]
            variable_id += 1

            cstatement.variables.append(fixed_variable)
            cstatement.connectors.append(variable_connector(counting_variable, fixed_variable, base_connector.connector))

        cstatement.counting_variable = counting_variable
        cstatement.introduced_variable = introduced_variable
        cstatement.variables.append(introduced_variable)
        cstatement.connectors.append(variable_connector(counting_variable, introduced_variable, base_connector.connector))

        with open(ws.dir_queued_queries + str(itemset_len) + '_' + str(idx) + '.rq', 'w', encoding='utf-8') as out:
            out.writelines(cstatement.compile())

        df_extending.to_csv(ws.dir_queued_queries + str(itemset_len) + '_' + str(idx) + '.info', index=False, encoding='utf-8')

    argument_list = [ws.workstation_home, endpoint, str(itemset_len)]
    os.system("python execute.py " + " ".join(argument_list))

    df = pd.read_csv(ws.dir_results + str(itemset_len+1) + '_agg_res.csv')
    df['itemset_length'] = df.apply(lambda x: len(set(x[df.columns[:-1]].values)), axis=1)

    if (previous_max_length == df.itemset_length.max()):
        print ('[STOPPING] Cannot extend anymore, this was the final extension')
        break

    else:
        previous_max_length = df.itemset_length.max()


#%% CREATE statistics
from glob import glob
with open (ws.dir_results + '_statistics.log', 'w') as out:
    for file in sorted(glob(ws.dir_results + '*_res.csv'), key=lambda x: int(x.split('/')[-1].split('_')[0])):
        out.write('---------------------'+ '\n')
        out.write('File: ' + file + '\n')
        df = pd.read_csv(file)
        out.write('File length:' + str(df.shape[0]) + '\n')
        df['itemset_length'] = df.apply(lambda x: len(set(x[df.columns[:-1]].values)), axis=1)
        out.write('Max itemset length:' + str(df.itemset_length.max()) + '\n')

#%% Post execute on result files
for file in sorted(glob(ws.dir_results + '*_res.csv'), key=lambda x: int(x.split('/')[-1].split('_')[0])):
    df = pd.read_csv(file)
    df['itemset_length'] = df.apply(lambda x: len(set(x[df.columns[:-1]].values)), axis=1)
    df = df[df['itemset_length'] == df.itemset_length.max()]
    df.to_csv(ws.dir_working_data + file.split('/')[-1], index=False)
