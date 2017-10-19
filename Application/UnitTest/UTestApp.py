from python.IApplication.JunoApp import JunoApp
from python.Task import TaskStatus
import os
import subprocess

class UnitTestApp(JunoApp):
    def __init__(self,rootdir, name, config_path=None):
        super(UnitTestApp,self).__init__(rootdir,name,config_path)
        self.task_reslist={}

    def split(self):
        rc = subprocess.Popen(["python","$JUNOTESTROOT/python/JunoTest/junotest","UnitTest","list"],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        unitcase, err = rc.communicate()
        if err:
            self.log.error('[APP_%d] @split() err = %s'%(self.id,str(err)))
        case = unitcase.split('\n')
        startline = 0
        for line in case:
            startline+=1
            if 'unittest cases' in line:
                break
        for c in case[startline:]:
            if c!= '' :
                self.data[c] = ""
        self.log.info('[App_%d] split data = %s'%(self.id, self.data))
        self.setStatus('data')
        return self.data


    def merge(self, tasklist):
        for task in tasklist.values():
            with open(self.res_dir+'/summary.log','a+') as resfile:
                if task.status == TaskStatus.COMPLETED:
                    resfile.writelines(task.tid+' '+task.data+'  SUCCESS\n')
                    resfile.flush()
                else:
                    resfile.writelines(task.tid+' '+task.data+'  SUCCESS\n')


    def analyze_log(self,logname):
        logpath = self.res_dir+'/app_%s_task_%s'%(self.id,logname)
        if not os.path.exists(logpath)
            self.log.error("[App_%d] Cannot find log file %s"%(self.id,logpath))
            return False
        with open(logpath,'a+') as logFile:
            self.log.debug('[App_%d] Parse log file %s'%(self.id,logpath))
            for line in logFile:
                if line.find('ERROR') != -1:
                    self.log.info('Find ERROR in log file, task fail')
                    return False
                else:
                    return True
