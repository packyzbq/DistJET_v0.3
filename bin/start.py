import argparse
import subprocess
import os,sys
import time

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
        sub.write("Executable = %s"%os.environ['DistJETPATH']+'/bin/ssh/run-sshd.sh')
        sub.write("Accounting_Group = juno")
        sub.write("getenv = True")
        sub.write("initialdir = tmp/res")
        sub.write('Requirements = Target.OpSysAndVer =?= "SL6" && regexp("jnws*", Name)')
        sub.write("Queue %s"%str(num))


args = getArgs()


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
    app_config = "null"

#server_host
server = args.server_host
if server is None:
	server = "localhost"

#Rundir 
curr_dir = os.path.abspath(os.getcwd("."))
runno = 0
while os.path.exists(curr_dir+"/Rundir_"+runno):
	runno = runno+1
rundir = curr_dir+"/Rundir_"+runno
os.mkdir(rundir)
os.chdir(rundir)

gen_submit(args.worker_num)
#submit job script
rc = subprocess.Popen(['condor_submit job_desc'],shell=True)
rc.wait()

os.chdir(rundir)
#check job is running
rc = subprocess.Popen(['/bin/bash %s'%os.environ["DistJETPATH"]+'/bin/ssh/refresh-running-node.sh'],stdout=subprocess.PIPE,shell=True)
output,err = rc.communicate()
ask_time = 1
while int(output[-2:]) != args.worker_num:
	time.sleep(ask_time*2)
	ask_time = ask_time+1
	if ask_time > 100:
		ask_time=100

os.chdir(curr_dir)


master = subprocess.Popen(['mpiexec -nameserver %s python %s %s %s %s %s %s'
						%(server,os.environ['DistJETPATH']+'/bin/master.py',args.app_module, config_path,loglevel,args.screen,rundir)],shell=True)

while not os.path.exists(rundir+'/port.txt'):
	time.sleep(1)

worker = subprocess.Popen(['mpiexec -n %s -nameserver %s -f %s python %s %s %s %s %s'
						%(str(args.worker_num),server,environ['DistJETPATH']+'/bin/worker.py'), 1, 'null',loglevel,rundir])

master.wait()

#clean ssh jobs
rc = subprocess.Popen(['condor_rm %s'%os.environ['USER']],shell=True)
rc.wait()





