
from os.path import exists


class reader_csv:
    
    
    def __init__(fileName, compacted=False):
        if not exists(fileName):
            return
        with open(fileName) as fp:
            
        return