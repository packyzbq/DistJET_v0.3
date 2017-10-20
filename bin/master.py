import os,sys
import traceback

assert os.environ['DistJETPATH']
assert os.environ['JUNOTOP']

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
else:
    print("SETUP: Find Boost")

# argv[1] = appfile, argv[2] = config, argv[3]=log_level, argv[4] = app_config_file
if len(sys.argv) < 3:
    print('@master need at least 2 parameter(given %d), args=%s, exit'%(len(sys.argv)-1, sys.argv))
    exit()

appfile = sys.argv[1]
config_path = sys.argv[2]
log_level = sys.argv[3]
app_config_path = sys.argv[4]

if os.path.exists(appfile):
    module_path = os.path.dirname(appfile)
    module_path = os.path.abspath(module_path)
    sys.path.append(module_path)
    module_name = os.path.basename(appfile)
    if module_name.endswith('.py'):
        module_name = module_name[:-3]
else:
    print('@master: cannot find app module %s, exit'%sys.argv[1])

rundir = os.getcwd()
import python.Util.Config as Conf
Conf.Config.setCfg('Rundir',rundir)

module=None
try:
    module = __import__(module_name)
except ImportError:
    print('@master: import user define module error, exit=%s' % traceback.format_exc())
    exit()

import python.Util.logger as logger
logger.setlevel(log_level)

from python.JobMaster import JobMaster
applications=[]

if module.__dict__.has_key('run') and callable(module.__dict__['run']):
    app = module.run(app_config_path)
    print app
    applications.extend(app)
else:
    print('@master: No callable function "run" in app module, exit')
    exit()

if config_path == 'null' or not os.path.exists(os.path.abspath(config_path)):
    print('@master: Cannot find configuration file [%s]'%os.path.abspath(config_path))
    config_path = os.getenv('DistJETPATH')+'/config/default.cfg'
Conf.set_inipath(config_path)
cfg = Conf.Config()

master = JobMaster(applications=applications)
if master.getRunFlag():
    print('@master: start running')
    master.startProcessing()