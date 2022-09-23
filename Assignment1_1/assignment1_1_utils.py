import sys
import os
sys.path.append('Framework')
from map_reduce_lib import MapReduceLib

def open_file_return_lines(filepath:str, remove_headers:bool=True ):
    if not os.path.isfile(filepath):
        print('File `%s` not found.' % filepath)
        sys.exit(-1)

    # Read data into separate lines
    file_contents = MapReduceLib.read_files([filepath])
    if remove_headers:
        file_contents.pop(0) # to remove the headers

    return file_contents
