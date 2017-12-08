import sys,os
assert os.environ['DistJETPATH']
assert os.environ['JUNOTOP']

sys.path.append(os.environ['DistJETPATH'])
#argv[1]=capacity, argv[2]=conf_file, argv[3]=log_level
if len(sys.argv) <= 2:
    print('@worker, need at least 2 parameter(given %d), exit'%(len(sys.argv)-1))
    exit()

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
else:
    print('SETUP: find Boost')

import python.Util.logger as logger
if len(sys.argv) == 4 and sys.argv[3] in ['info','debug']:
    logger.setlevel(sys.argv[3])
else:
    logger.setlevel('info')

import python.Util.Config as CONF
CONF.Config.setCfg('Rundir',os.getcwd())
cfg_path = sys.argv[2]
if sys.argv[2] != 'null' and not os.path.exists(sys.argv[2]):
    CONF.set_inipath(os.path.abspath(sys.argv[2]))

CONF.set_inipath(cfg_path)
cfg = CONF.Config()
'''
import time
times=0
while 'running.log' not in os.listdir('.'):
    time.sleep(1)
    times+=1
    if times > 10:
        print('@agent: Agent cannot find master,exit')
        exit()
    continue
'''
import python.WorkerAgent as WA
capacity = int(sys.argv[1])
worker_module_path = None

agent = WA.WorkerAgent(capacity=capacity)
agent.run()

import threading
print('@agent: Agent exit, remains %d thread running, threads list = %s'%(threading.active_count(),threading.enumerate()))
exit()
