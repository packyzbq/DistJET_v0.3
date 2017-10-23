from optparse import OptionParser
import os,sys
import subprocess

if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
sys.path.append(os.environ['DistJETPATH'])

parser = OptionParser(usage="%prog AppFile [opts] --ini <file>",description="start the master on local/HTCondor with config file")
parser.add_option("--condor", dest="dst",action="store_true")
parser.add_option("--local", dest="dst", action="store_false")
parser.add_option("--debug", dest="debug",action="store_true")
parser.add_option("--ini",dest="script_file")
parser.add_option("--back",dest="back",action="store_true",help="run on the back")
parser.add_option("--app-conf",dest="app_conf",help="application config file path")

(options,args) = parser.parse_args()

parg = ''
parg += args[0]


if options.script_file:
    if os.path.exists(options.script_file):
        #from python.Util import Conf
        #Conf._inipath = options.script_file
        parg += ' %s'%options.script_file
    else:
        print('[Warning] Cannot find ini file %s, skip it'%options.script_file)
else:
    parg+=' null'

if options.debug:
    parg += ' debug'
else:
    parg += ' info'

if options.app_conf:
    if not os.path.exists(os.path.abspath(options.app_conf)):
        print 'Can not find app config file %s, exit'%options.app_conf
        exit()
    else:
        parg += ' %s'%os.path.abspath(options.app_config)
else:
    parg += ' None'

# run on local node
if not options.dst:
    # check runtime env
    try:
        rc = subprocess.Popen(["mpichversion"], stdout=subprocess.PIPE)
        print('SETUP: find mpich tool')
    except:
        print("can't find mpich tool, please setup mpich2 first")
        exit()

    app_file = args[0]
    if not os.path.exists(app_file):
        app_file = os.environ['DistJETPATH']+'/Application/'+app_file
        if not os.path.exists(app_file):
            print('[Error] Cannot find app script:%s, exit'%app_file)
            sys.exit(1)
    print("mpiexec python $DistJET/bin/master.py %s" %parg)
    if options.back:
        os.system("mpiexec python $DistJETPATH/bin/master.py %s &" %parg)
    else:
        os.system("mpiexec python $DistJETPATH/bin/master.py %s" %parg)

# run on HTCondor
else:
    pass

