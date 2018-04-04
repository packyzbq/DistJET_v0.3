import htcondor
import classad
import time
import os
import subprocess

class backend:
    def __init__(self):
        self.schedd=htcondor.Schedd()
        self.jobentry = {'Universe': 'vanilla', 'Executable': '/junofs/users/zhaobq/DistJET_v0.3/Backend/HTCONDOR/run-sshd.sh', 'Accounting_Group': 'juno', 'getenv': 'True',
                    'Requirements': 'Target.OpSysAndVer =?= "SL6" && regexp("jnws*", Name)'}
        self.sub = htcondor.Submit(self.jobentry)
        self.hostlist={}
        self.clusterID=0

    def apply(self,num):
        if not os.path.exists(os.environ['HOME']+'/.DistJET/ssh-auth'):
            rc = subprocess.Popen(['bash create_ssh.sh'],shell=True)
            rc.wait()
        with self.schedd.transaction() as txn:
            self.clusterID = self.sub.queue(txn,count=num)

        while(True):
            wait_flag = False
            time.sleep(5)
            waitlist=[]
            #cleanup hostlist
            self.hostlist.clear()
            # jobstatus 1:IDLE 2:Runing 3:Removed 4:Completed 5:Held 6:Transferring Output 7:Suspended
            for job in self.schedd.xquery(requirements="ClusterID == %d"%self.clusterID,projection=['JobStatus','ProcID','RemoteHost']):
                if job['JobStatus']== 2 and job.get("RemoteHost") is not None:
                    host = job.get("RemoteHost").split("@")[1]
                    if self.hostlist.has_key(host):
                        self.hostlist[host]+=1
                    else:
                        self.hostlist[host]=1
                else:
                    waitlist.append(str(self.clusterID)+'.'+str(job['ProcID']))
                    wait_flag = True
            if not wait_flag:
                # resource is applied
                break
            #print("Running node is %s \nIdle nodes is %s"%([host for host in self.hostlist.keys()],waitlist))
        return self.hostlist

    def release(self,**kwargs):
        self.schedd.act(htcondor.JobAction.Remove, 'ClusterID == %d'%self.clusterID)
        return True


