from tools.workstation import *
from tools.linked_data_tools import *

def nonblank_lines(f):
    for l in f:
        line = l.rstrip()
        if line:
            yield line

def stringify(*args):
    s = ''
    currentarg_index = 0
    for arg in args:
        if currentarg_index > 0:
            s += "\t" + str(arg)
        else:
            s += str(arg)
        currentarg_index += 1
    return s

def stringify_statement(*args):
    s = ''
    currentarg_index = 0

    for arg in args:

        if currentarg_index > 0:
            s += " " + str(arg)
        else:
            s += str(arg)

        currentarg_index += 1

    return s + ' .'

