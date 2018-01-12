import argparse
import subprocess
import os,sys

assert os.environ.has_key("DistJETPATH")

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--conf",dest="config_path", type=str,help="configure file path")
    parser.add_argument("--server",dest="server_host", type=str,help="where to start name service")
    parser.add_argument("-m","--app", dest="app_module", type=str,help="the app you want to run")
    parser.add_argument("--app-conf",dest="app_config", type=str,help="the configure file of Application")
    parser.add_argument("-n","--worker-num",dest="worker_num", type=int,help="the number of worker you want to start")
    parser.add_argument("-d",dest="log_level",action="store_true", default=False, help="debug mode")
    parser.add_argument("-s","--screen", dest="screen",action="store_true", help="print log output into screen")
    args = parser.parse_args()
    if (not args.app_module) or (not args.worker_num):
        print parser.error("less of app or worker number")

def gen_submit(num):
    subfile = "job_submit"
    while subfile in os.listdir("."):
        subfile+="_1"
    with open(subfile,'w+') as sub:
        sub.write("Universe = vanilla")
        sub.write("Executable = %s"%os.environ['DistJETPATH']+'/bin/run-sshd.sh')
        sub.write("Accounting_Group = juno")
        sub.write("getenv = True")
        sub.write("initialdir = tmp/res")
        sub.write('Requirements = Target.OpSysAndVer =?= "SL6" && regexp("jnws*", Name)')
        sub.write("Queue %s"%str(num))


args = getArgs()

#submit condor_job
proc = subprocess.Popen(['condor_submit %s'%()])


#config path
config_path = args.config_path
if config_path is None:
   config_path="null"

#loglevel
loglevel = "info"
if args.log_level:
    loglevel = "debug"

#app config
app_config = args.app_config
if app_config is None:
    app_config

master = subprocess.Popen(['mpiexec -nameserver localhost python %s %s %s %s %s'%(os.environ['DistJETPATH']+'/bin/master.py',args.app_module, config_path,loglevel,args.screen)],shell=True)

worker = subprocess.Popen(['mpiexec -nameserver localhost python %s '])




