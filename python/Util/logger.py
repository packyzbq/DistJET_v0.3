import logging
import os
from Config import Config

loglevel={'info':logging.INFO, 'debug':logging.DEBUG}


def getLogger(name, level=None, applog=False):
    if applog:
        log_dir = Config.getCFGattr('Rundir')
        if not log_dir:
            log_dir = os.getcwd()
        log_dir += '/DistJET_log/Applog'
    else:
        log_dir = Config.getCFGattr('Logdir')
    if not level:
        level = Config.getCFGattr('Log_Level')
        if not level:
            level = 'info'
    if not log_dir:
        log_dir = os.getcwd() + '/DistJET_log'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    format = logging.Formatter('[%(asctime)s] %(threadName)s %(levelname)s: %(message)s')
    console = None
    if Config.getCFGattr('LogConsole'):
        console = logging.StreamHandler()
        console.setFormatter(format)
    handler = logging.FileHandler(log_dir + '/DistJET.' + name + '.log')
    handler.setFormatter(format)

    logger = logging.getLogger('DistJET.' + name)
    logger.setLevel(loglevel[level])
    logger.addHandler(handler)
    if console:
        print "add console handler"
        logger.addHandler(console)
    return logger

def setlevel(level, logger=None):
    if logger:
        logger.setLevel(loglevel[level])
    Config.setCfg('Log_Level',level)

def setConsole(flag=False):
	Config.setCfg('LogConsole',flag)
