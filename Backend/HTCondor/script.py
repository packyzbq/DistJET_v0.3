import htcondor
import classad
import time

class backend:
    def __init__(self):
        self.schedd=htcondor.Schedd()
        self.jobentry = {'Universe': 'vanilla', 'Executable': 'run-sshd.sh', 'Accounting_Group': 'juno', 'getenv': 'True',
                    'Requirements': 'Target.OpSysAndVer =?= "SL6" && regexp("jnws*", Name)'}
        self.sub = htcondor.Submit(self.jobentry)
        self.hostlist={}
        self.clusterID=0

    def apply(self,num):
        with self.schedd.transaction() as txn:
            clusterID = self.sub.queue(txn,count=num)

        wait_flag = False
        while(True):
            time.sleep(5)
            #cleanup hostlist
            self.hostlist.clear()
            # jobstatus 1:IDLE 2:Runing 3:Removed 4:Completed 5:Held 6:Transferring Output 7:Suspended
            for job in self.schedd.xquery(requirements="ClusterID == %d"%clusterID,projection=['JobStatus','ProcID','RemoteHost']):
                if job['JobStatus']== 2 and job.get("RemoteHost") is not None:
                    host = job.get("RemoteHost").split("@")[1]
                    if self.hostlist.has_key(host):
                        self.hostlist[host]+=1
                    else:
                        self.hostlist[host]=1
                else:
                    wait_flag = True
                    break
            if not wait_flag:
                # resource is applied
                break
        return self.hostlist

    def release(self,**kwargs):
        pass


