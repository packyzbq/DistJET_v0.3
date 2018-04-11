import logging
import os
from Config import Config
cfg = Config()

loglevel={'info':logging.INFO, 'debug':logging.DEBUG}


def getLogger(name, level=None, applog=False):
    log_dir = cfg.getCFGattr('rundir')+'/DistJET_log'
    if applog:
        #log_dir = Config.getCFGattr('Rundir')
        #if not log_dir:
        #    log_dir = os.getcwd()
        log_dir += '/Applog'
    #else:
    #    log_dir = cfg.getCFGattr('Logdir')
    if not level:
        level = cfg.getCFGattr('log_level')
        if not level:
            level = 'info'
    #print 'log %s level = %s'%(name,level)
    #if not log_dir:
    #    log_dir = os.getcwd() + '/DistJET_log'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    format = logging.Formatter('[%(asctime)s] %(threadName)s %(levelname)s: %(message)s')
    console = None
    if Config.getCFGattr('logconsole'):
        console = logging.StreamHandler()
        console.setFormatter(format)
    handler = logging.FileHandler(log_dir + '/DistJET.' + name + '.log')
    handler.setFormatter(format)

    logger = logging.getLogger('DistJET.' + name)
    logger.setLevel(loglevel[level])
    logger.addHandler(handler)
    if console:
        print "log %s add console handler"%name
        logger.addHandler(console)
    return logger

def setlevel(level, logger=None):
    if logger:
        logger.setLevel(loglevel[level])
    Config.setCfg('log_level',level)

def setConsole(flag=False):
	Config.setCfg('logconsole',flag)
