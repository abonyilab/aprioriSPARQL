#region imports
import pandas as pd
import glob
from tqdm import tqdm
import os
import bz2
import gzip
#endregion

def object_to_int(rdfstring):
    return int(rdfstring.split('^')[0])

def autoconvert_subject(sub):
    try:
        if '<http://ma-graph.org/entity/' in sub:
            return object_to_url_lastpart(sub)
    except:
        pass
    try:
        if '<http://' in sub:
            return object_to_url_lastpart(sub)
    except:
        pass
    return sub

def autoconvert_object(obj):
    try:
        if '<http://www.w3.org/2001/XMLSchema#integer>' in obj:
            return object_to_int(obj)
    except:
        pass

    try:
        if '<http://www.w3.org/2001/XMLSchema#date>' in obj:
            return object_to_year(obj)
    except:
        pass
    try:
        if '<http://www.w3.org/2001/XMLSchema#string>' in obj:
            return object_to_str(obj)
    except:
        pass

    try:
        if '<http://ma-graph.org/entity/' in obj:
            return object_to_url_lastpart(obj)
    except:
        pass

    try:
        if '<http://' in obj:
            return object_to_url_lastpart(obj)
    except:
        pass

    return obj


def object_to_str(rdfstring):
    return rdfstring.split('^')[0].strip()

def object_to_year(rdfstring):
    return int(object_to_str(rdfstring).split('-')[0])

def object_to_url_lastpart(rdfstring):
    return rdfstring.strip('<>').split('/')[-1]

def stringstrip(rdfstring):
    return rdfstring.strip()

def simplify_predicate_lasturitag(rdfstring):
    return rdfstring.strip('<>').split('/')[-1]

def read_rdf(file, max_chunksize= 1000000, default_sep = None):
    filename, file_extension = os.path.splitext(file)

    if file_extension == ".bz2":
        if default_sep is None: default_sep = ' '
        for cindex in pd.read_csv(bz2.BZ2File(file)
                , names=['s', 'p', 'o', '.']
                , error_bad_lines=False
                , warn_bad_lines=False
                , sep = default_sep
                , skiprows=1
                , header=None
                , iterator=True
                , chunksize=max_chunksize):
            cindex = cindex[['s', 'p', 'o']]
            yield cindex

    elif file_extension == '.gz':
        if default_sep is None: default_sep = '\t'
        for cindex in pd.read_csv(gzip.open(file)
                , names=['s', 'p', 'o', '.']
                , error_bad_lines=False
                , warn_bad_lines=False
                , sep=default_sep
                , skiprows=1
                , header=None
                , iterator=True
                , chunksize=max_chunksize):
            cindex = cindex[['s', 'p', 'o']]
            yield cindex

    elif file_extension == '.ttl' or file_extension == '.nt':
        if default_sep is None: default_sep = '\t'
        for cindex in pd.read_csv(file
                , names=['s', 'p', 'o', '.']
                , error_bad_lines=False
                , warn_bad_lines=False
                , sep=default_sep
                , skiprows=1
                , header=None
                , iterator=True
                , chunksize=max_chunksize):
            cindex = cindex[['s', 'p', 'o']]
            yield cindex




def file_iter(file, names, skiprows = 0, default_sep='\t', max_chunksize = 100000000):
    for cindex in pd.read_csv(file
                , names = names
                , error_bad_lines = False
                , warn_bad_lines = False
                , sep = default_sep
                , skiprows = skiprows
                , encoding = 'utf-8'
                , header=None
                , iterator=True
                , chunksize=max_chunksize):
        yield cindex

def read_file_auto(file, max_chunksize= 1000000, default_sep = None):
    filename, file_extension = os.path.splitext(file)
    if file_extension == '.gz':
        if default_sep is None: default_sep = '\t'
        for cindex in pd.read_csv(gzip.open(file)
                , error_bad_lines=False
                , warn_bad_lines=False
                , sep=default_sep
                , skiprows=1
                , header=None
                , iterator=True
                , chunksize=max_chunksize):

            yield cindex

def post_clean_predicates(pred):
    return pred.split('>')[0].strip('<>')

def rdf_predicates(file, maxiter = -1):
    preds = pd.DataFrame()
    prev_count = 0
    citer = 0
    for ch in tqdm(read_rdf(file)):
        try:
            ch.p = ch.p.apply(post_clean_predicates)
            c = ch[['p']].drop_duplicates()
            preds = preds.append(c).drop_duplicates()
        except:
            pass

        if maxiter == -1:
            if prev_count >= preds.shape[0]:break
            else:prev_count = preds.shape[0]

        if maxiter == -1: continue
        if citer >= maxiter: break
        citer+=1
    return preds

def index_get_iterated_property(iterator, needed_property):
    save_pd = pd.DataFrame()
    for f in tqdm(iterator):
        saveable = f[f[f.columns[1]] == needed_property]
        if saveable.shape[0] > 0:
            save_pd = save_pd.append(saveable)
    return save_pd

def index_get_iterated_properties(iterator, needed_properties):
    save_pd = pd.DataFrame()
    for f in tqdm(iterator):
        saveable = f[f[f.columns[1]].isin(needed_properties)]
        if saveable.shape[0] > 0:
            save_pd = save_pd.append(saveable)
    return save_pd

def index_get_iterated_subjects(iterator, needed_subjects):
    save_pd = pd.DataFrame()
    for f in tqdm(iterator):
        saveable = f[f[f.columns[0]].isin(needed_subjects)]
        if saveable.shape[0] > 0:
            save_pd = save_pd.append(saveable)
    return save_pd

def get_dataexample(file, predicate):
    example_data = pd.DataFrame()
    for l in tqdm(read_rdf(file)):
        pdt = l[l['p'].str.contains(predicate)]
        example_data = example_data.append(pdt)
        break
    return example_data

def subject_selector_regex(file, subject, hitnrun=True):
    example_data = pd.DataFrame()
    for l in tqdm(read_rdf(file)):
        pdt = l[l['s'].str.contains(subject)]
        example_data = example_data.append(pdt)
        if example_data.shape[0] >=1:break

    return example_data

def fileiter_subject_selector_by_ids(file, subject):
    for l in tqdm(read_rdf(file)):
        pdt = l[l['s'].isin(subject)]
        if pdt.shape[0] > 0: yield pdt

def fileiter_unique_subjects(file):
    for l in tqdm(read_rdf(file)):
        pdt = l[['s']]
        if pdt.shape[0] > 0: yield pdt

def fileiter_predicate_selector(file, pred_regex):
    for l in tqdm(read_rdf(file)):
        pdt = l[l['p'].str.contains(pred_regex)]
        if pdt.shape[0] >= 1: yield pdt

def fileiter_predicate_object_selector(file, pred_regex, obj_regex):
    for l in tqdm(read_rdf(file)):
        pdt = l[(l['p'].str.contains(pred_regex)) & (l.o.str.contains(obj_regex))]
        if pdt.shape[0] >= 1: yield pdt
