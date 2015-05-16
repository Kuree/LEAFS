import logging
import threading
import time
import json

class MyFormat(logging.Formatter):    
    pathwidth = 60
    fmt = '{asctime:15s} {levelname:8s} {funcpath:<' + str(pathwidth) + 's} {msg:s}' # avoid formatting the format string
    
    def format(self, record):
        # need to format the time    
        record.__dict__['asctime'] = self.formatTime(record)
        #print (record.__dict__)
        # create the function path
        fp = '{module}:{lineno} -- {processName}/{threadName}:{funcName}'.format(**record.__dict__)
        
        x = len(fp)
        #truncate if need
        if x > (self.pathwidth - 3):
            print (x-self.pathwidth+3)
            fp = '...' + fp[x-self.pathwidth+3::]
        record.__dict__['funcpath'] = fp
        
        #print (record.__dict__)
        return self.fmt.format(**record.__dict__)
    
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(MyFormat())
log = logging.getLogger()
log.addHandler(ch)
log.setLevel(ch.level)

def log(level, msg):
    '''
    Simple logging method. Will move to class-based logger later
    '''
    logging.log(level, msg)