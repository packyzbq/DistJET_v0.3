import os,sys
import python.Util.Config as Config
sys.path.append(os.environ['DistJETPATH'])
task_path = Config.Config.getCFGattr('Rundir')+'/task_log'
succ = 0
running = 0
err = 0
for file_name in os.listdir(task_path):
    if file_name.endswith('.tmp'):
        running+=1
    elif file_name.endswith('.err'):
        err+=1
    else:
        succ+=1
if succ==0 and running==0 and err==0:
    print "System is initializing, no task generate"
    exit()
#parse AppMgr.log
total = 0
if os.path.exists(Config.Config.getCFGattr('Rundir')+'/DistJET_log/DistJET.AppMgr.log'):
    appmgr_log = open(Config.Config.getCFGattr('Rundir')+'/DistJET_log/DistJET.AppMgr.log','a')
    for line in appmgr_log.readlines():
        if '[AppMgr]: Create' in line:
            total = int(line[-6])
            break
    appmgr_log.close()

print "-----task status-----"
print "running: %d tasks"%running
print "success: %d tasks"%succ
print "error: %d tasks"%err
print "total: %d tasks"%total