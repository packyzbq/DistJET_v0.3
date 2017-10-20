import sys,os
assert os.environ['DistJETPATH']
assert os.environ['JUNOTOP']

#argv[1]=capacity, argv[2]=conf_file
if len(sys.argv) <= 2:
    print('@worker, need at least 2 parameter(given %d), exit'%(len(sys.argv)-1))
    exit()

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
else:
    print('SETUP: find Boost')

import python.Util.Config as CONF
CONF.config.setCfg('Rundir',os.getcwd())
cfg_path = sys.argv[2]
if sys.argv[2] != 'null' and not os.path.exists(sys.argv[2]):
    CONF.set_inipath(os.path.abspath(sys.argv[2]))

CONF.set_inipath(cfg_path)
cfg = CONF.Config()

import python.WorkerAgent as WA
capacity = int(sys.argv[1])
worker_module_path = None

agent = WA.WorkerAgent(capacity)
agent.run()

import threading
print('@agent: Agent exit, remains %d thread running, threads list = %s'%(threading.active_count(),threading.enumerate()))
exit()