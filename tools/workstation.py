from dataclasses import dataclass
import os
import os.path

def check_or_create_dirs(musthave_dirs):
    for chdir in musthave_dirs:
        check_or_create_dir(chdir)


def check_or_create_dir(musthavedir: str):
    if not os.path.isdir(musthavedir):
        os.mkdir(musthavedir)
        return musthavedir, True
    return musthavedir, False


def require_files(required_files):
    for chfile in required_files:
        print(chfile, ' OK? >> ', os.path.isfile(chfile))
        assert os.path.isfile(chfile)


def require_file(required_file):
    print(required_file, ' OK? >> ', os.path.isfile(required_file))
    assert os.path.isfile(required_file)

@dataclass
class workstation():
    workstation_home : str

    def __init__(self, home):
        self.workstation_home = home
        self.dir_working_data = home + 'wdir/'
        self.dir_errored = home + 'errors/'
        self.dir_config = home + 'config/'
        self.dir_queued_queries = home + "queued_queries/"
        self.dir_executed_queries = home + 'executed_queries/'
        self.dir_query_results = home + 'query_results/'
        self.dir_results = home + 'results/'
        self.dir_logs = home + 'logs/'
        check_or_create_dirs([self.workstation_home,self.dir_query_results, self.dir_config, self.dir_working_data, self.dir_logs, self.dir_errored,  self.dir_executed_queries, self.dir_queued_queries, self.dir_results])

