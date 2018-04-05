import os,sys
import traceback
assert os.environ['DistJETPATH']
assert os.environ['JUNOTOP']
sys.path.append(os.environ['DistJETPATH'])

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
else:
    print("SETUP: Find Boost")

# argv[1] = appfile, argv[2] = config, argv[3]=log_level, argv[4] = app_config_file, argv[5] = log_screen, argv[6] = Rundir
if len(sys.argv) < 4:
    print('@master need at least 2 parameter(given %d), args=%s, exit'%(len(sys.argv)-1, sys.argv))
    exit()

#clean up last logfile
#if os.path.exists('DistJET_log'):
#    os.removedirs('DistJET_log')

print sys.argv[1:]

appfile = sys.argv[1]
config_path = sys.argv[2]
log_level = sys.argv[3]
app_config_path = sys.argv[4]
if len(sys.argv) > 5:
    log_screen = sys.argv[5]
else:
    log_screen = False
if len(sys.argv) > 6:
    rundir = sys.argv[6]
else:
	rundir = 'null'

if log_level == "debug":
    print appfile
if os.path.exists(appfile):
    module_path = os.path.dirname(appfile)
    module_path = os.path.abspath(module_path)
    sys.path.append(module_path)
    module_name = os.path.basename(appfile)
    if module_name.endswith('.py'):
        module_name = module_name[:-3]
else:
    print('@master: cannot find app module %s, exit'%sys.argv[1])

#rundir = os.getcwd()
import python.Util.Config as Conf

module=None
try:
    module = __import__(module_name)
except ImportError:
    print('@master: import user define module error, exit=%s' % traceback.format_exc())
    exit()

import python.Util.logger as logger
logger.setlevel(log_level)
logger.setConsole(log_screen)

from python.JobMaster import JobMaster
applications=[]

if module.__dict__.has_key('run') and callable(module.__dict__['run']):
    app = module.run(app_config_path)
    print app
    applications.extend(app)
else:
    print('@master: No callable function "run" in app module, exit')
    exit()

if config_path == "null" or not os.path.exists(os.path.abspath(config_path)):
    print('@master: Cannot find configuration file [%s]'%os.path.abspath(config_path))
    config_path = os.getenv('DistJETPATH')+'/config/config.ini'
Conf.set_inipath(config_path)
if rundir != 'null':
    print "set rundir = %s"%rundir
    Conf.Config.setCfg('Rundir',rundir)
cfg = Conf.Config()
print "rundir="+cfg.getCFGattr("Rundir")

master = JobMaster(applications=applications)
if master.getRunFlag():
    with open("running.log","w+") as rf:
        rf.write('True')
        rf.flush()
    print('@master: start running')
    master.startProcessing()
else:
    print('@master: Load Application error, exit')

#
#import shutil
#if os.path.exists(rundir+'/.DistJET_tmp'):
#    shutil.rmtree(rundir+'/.DistJET_tmp')
