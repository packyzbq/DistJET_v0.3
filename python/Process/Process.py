import time
import types
import os,sys
sys.path.append(os.environ['DistJETPATH'])
import threading
import select
import subprocess
import Queue
import traceback
from Parser import Parser
from python.Task import Task
import python.Util.logger as logger


class status:
    (SUCCESS, FAIL, TIMEOUT, OVERFLOW, ANR) = range(0,5)
    DES = {
        FAIL: 'Task fail, return code is not zero',
        TIMEOUT: 'Run time exceeded',
        OVERFLOW: 'Memory overflow',
        ANR: 'No responding'
    }
    @staticmethod
    def describe(stat):
        if status.DES.has_key(stat):
            return status.DES[stat]

class CommandPack:
    def __init__(self, command, task_log=None, proc_log=None, finalize_flag=False):
        self.command=[]
        if type(command) == types.ListType:
            self.command.extend(command)
        else:
            self.command.append(command)
        self.task_log = task_log
        self.proc_log = proc_log
        self.finalize_flag = finalize_flag

#FIXME start a sh process, cannot know when the command finished
class Process_withENV(threading.Thread):
    """
    start process with setup env,
    """
    def __init__(self,initial,logpath,shell=True, timeout=None,ignoreFail=False, task_callback=None, finalize_callback=None):
        """
        :param initial: the command of setup
        :param shell:
        :param timeout: select time out
        :param ignoreFaile:
        :param hook: when complete tasks, call this method
        """
        super(Process_withENV,self).__init__()
        #print self.start

        self.shell = shell
        self.ignoreFail = ignoreFail
        print "@Process: ignoreFail = %s"%str(self.ignoreFail)
        self.exec_queue_lock = threading.RLock()
        self.executable = Queue.Queue()

        self.log = open(logpath+'/process.log','w+')
        self.hook = task_callback
        self.finalize_callback = finalize_callback
        self.initial = []
        if(type(initial) == types.ListType):
            self.initial.extend(initial)
        if(type(initial) == types.StringType):
            self.initial.append(initial)

        self.stdout = subprocess.PIPE
        self.stdin = subprocess.PIPE
        self.process = subprocess.Popen(['bash'], stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid)
        self.pid = self.process.pid

        #self.logFile.write('@Process: create process for tasks, pid= %d\n' %self.pid)\
        self.log.write('@Process: create process for tasks, pid=%d\n'%self.pid)
        self.log.flush()

        self.timeout = timeout
        self.recode = None
        self.status = None

        self.start_time = None
        self.end = None
        self.killed = None
        self.fatalLine = None
        self.logParser = Parser()

        self.stop_flag = False

    def set_task(self,task, genLog=True):
        command_list=[]
        if task:
            tmp_list, errmsg= task.genCommand()
            if errmsg:
                self.log.write(errmsg+'\n')
            command_list = []
            for comm in tmp_list:
                if not comm.endswith('\n'):
                    comm+='\n'
                command_list.append(comm)

        if genLog:
            commpack = CommandPack(command_list,task_log=task.res_dir+'/task_'+str(task.tid)+'_log.log')
        else:
            commpack = CommandPack(command_list,proc_log=self.log, finalize_flag=True)
        self.executable.put(commpack)
        self.log.write('[Proc] Add task command=%s, logfile=%s\n'%(commpack.command,str(commpack.task_log)+'|'+str(commpack.proc_log)))
        self.log.flush()
        print 'set task %s'%commpack.command
        '''
        self.exec_queue_lock.acquire()
        try:
            for comm in command_list:
                if not comm.endswith('\n'):
                    comm+='\n'
                self.executable.put(comm)
                self.WorkerLog.debug("<process> add command %s"%command_list)
               # if comm != 'exit':
               #     self.executable.put('echo "recode:$?"\n')
        finally:
            self.exec_queue_lock.release()
        '''

    def stop(self,force=False):
        self.stop_flag = True
        if force:
            self._kill_task()
        self.process.wait()




    def initialize(self):
        """
        setup env for process
        :return: -1 no process; -2 setup timeout; 0 success;1 other error
        """
        if self.process is None:
            return -1
        if self.initial is None:
            return 0
        self.log.write("<process>@init: initial comm = %s\n"%self.initial)
        self.log.flush()
        for comm in self.initial:
            if comm[-1] != '\n':
                comm+='\n'
            self.process.stdin.write(comm)
        self.process.stdin.write('echo "recode=$?"\n')
        while True:
            fs = select.select([self.process.stdout],[],[],10)
            if not fs[0]:
                #self.logFile.write('@Process initial timeout')
                #self.logFile.flush()
                self.log.write('[ERROR]@Process initial timeout\n')
                self.log.flush()
                return -2
            if self.process.stdout in fs[0]:
                data = os.read(self.process.stdout.fileno(), 1024)
                if "recode" in data:
                    ds = data.split()
                    recode = ds[-1].split("recode")[-1][1:]
                    return int(recode)
                else:
                    #self.logFile.write('[Setup_INFO] %s'%data)
                    #self.logFile.flush()
                    self.log.write('[Setup_INFO] %s\n'%data)
                    self.log.flush()
            else:
                return 1

    def finalize_and_cleanup(self, task):
        if task and type(task)!=types.StringType:
            task.boot.append('exit')
        else:
            task = Task(-1)
            task.boot.append('exit')
        self.set_task(task,genLog=False)

    def run(self):
        script_list = []
        while not self.stop_flag:
            try:
                self.exec_queue_lock.acquire()
                if not self.executable.empty():
                    commpack = self.executable.get()
                    self.exec_queue_lock.release()
                    # for one task ,multi script
                    # get script list from commpack
                    script_list = commpack.command
                    logfile = None
                    if commpack.task_log:
                        logfile = open(commpack.task_log, 'w+')
                    elif commpack.proc_log:
                        logfile = commpack.proc_log
                    if commpack.finalize_flag:
                        self.hook = self.finalize_callback
                    index = 0
                    while len(script_list) != 0:
                        script = script_list[index]
                        index+=1
                        print "<process> get script=%s"%script
                        if script == 'exit':
                            logfile.write("[Proc] Ready to exit")
                            logfile.flush()
                            self.stop_flag=True
                            break
                        if "recode" not in script:
                            self.start_time = time.time()
                            logfile.write('\n' + '*' * 20 + ' script "%s" Running log ' % script[:-1] + '*' * 20 + '\n')
                            logfile.flush()
                        self.process.stdin.write(script)
                        self.process.stdin.write('echo "@recode:$?"\n')
                        while True:
                            fs = select.select([self.process.stdout],[],[],self.timeout)
                            if not fs[0]:
                                # No response
                                self.status = status.ANR
                                logfile.write('[Proc] Task no response, ready to kill')
                                logfile.flush()
                                self.end = time.time()
                                if self.hook and callable(self.hook):
                                    self.hook(self.status,self.recode, self.start_time, self.end)
                                self._kill_task()
                                #tmp_list=[]
                                #while not self.executable.empty():
                                #    tmp_list.append(self.executable.get())
                                #self.process = self._restart()
                                #if tmp_list:
                                #    self.set_exe(tmp_list)
                                script_list=[]
                                break
                            data = os.read(self.process.stdout.fileno(),1024)
                            if not data:
                                logfile.write("[proc] No data output ,break")
                                logfile.flush()
                                script_list = []
                                break
                            logfile.write(data)
                            logfile.flush()
                            st = data.split("\n")
                            #if len(st) >= 2 and "recode" in st[-2]:
                            fin_flag = False
                            
						    # no log parse 
                            if (len(st)>=2 and "@recode" in st[-2]) or (st[-1]!="" and "@recode" in st[-1])
                                self.end = time.time()

                            for line in st:
                                if "@recode" in st:
                                    fin_flag = True
                                    self.end = time.time()
                                    self.recode = line[line.find("@recode=")+8:]
                                    logfile.write("\n\n\nreturn code = %s"%self.recode)
                                    logfile.write("\nstart time = %s \nend time = %s\n\n"%(time.asctime(time.localtime(self.start_time)), time.asctime(time.localtime(self.end))))
                                    logfile.flush()
                                    if int(self.recode) == 0:
                                        self.status = status.SUCCESS
                                    if self.hook and callable(self.hook):
                                        self.hook(self.status, self.recode, self.start_time, self.end)
                                    break

                                elif not self.ignoreFail and (self.logParser and (not self._parseLog(line))):
                                    fin_flag = True
                                    self.status = status.FAIL
                                    self.recode = -1
                                    self.end = time.time()
                                    if self.hook and callable(self.hook):
                                        self.hook(self.status, self.recode, self.start_time, self.end)
                                    logfile.write(line)
                                    logfile.write("\n\n\n @execute error, stop running")
                                    logfile.flush()
                                    #self._kill_task()
                                    #self.process = self._restart()
                                    script_list = []
                                    break
                                #else:
                                #    logfile.write(line)
                                #    logfile.flush()
                            if fin_flag:
                                break

                        if commpack.task_log:
                            logfile.flush()
                            logfile.close()

                else:
                    self.exec_queue_lock.release()
                    time.sleep(1)
            except Exception,e:
                self.log.write('@Process catch error: %s\n'%e.message)
                print traceback.format_exc()

        self._burnProcess()



    def _kill_task(self):
        if not self.process:
            return
        import os, signal
        self.process.stdout.flush()
        pgrp = os.getpgid(self.pid)
        #self.logFile.write('[Proc] kill pid=%d\n' % pgrp)
        self.log.write('[Proc] kill pid=%d\n' % pgrp)
        try:
            os.killpg(pgrp,signal.SIGHUP)
            self.process.wait()
        except:
            #self.logFile.write('[Proc] KILLError: %s'%traceback.format_exc())
            self.log.write('[Error] KILLError: %s\n'%traceback.format_exc())
        self.log.flush()

    def _restart(self):
        proc = subprocess.Popen(['bash'], shell=self.shell, stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid)
        self.pid = proc.pid
        #self.logFile.write('[Proc] Restart a new process,pid=%d'%self.pid)
        #self.logFile.flush()
        self.log.write('[Proc] Restart a new process,pid=%d\n'%self.pid)
        self.log.flush()

        return proc

    #def _cleanup_process(self):


    def _parseLog(self,data):
        result,self.fatalline = self.logParser.parse(data)
        return result

    def _burnProcess(self):
        #self.WorkerLog.debug('[Proc] Terminate Process....')
        self.log.write('[Proc] Terminate Process....\n')
        self.log.flush()
        self.process.terminate()
        self.process.wait()
        self.log.close()

def hook(status, recode, start_time, end_time):
    print 'hook method called, status= %s, recode=%s'%(str(status),str(recode))

def add(proc,comm):
    for com in comm:
        proc.set_exe([com])
        #print '@thread add comm %s'%com
        time.sleep(2)

if __name__ == '__main__':
    comm = ['$JUNOTESTROOT/python/JunoTest/junotest UnitTest Tutorial\n','$JUNOTESTROOT/python/JunoTest/junotest UnitTest JunoTest\n','$JUNOTESTROOT/python/JunoTest/junotest UnitTest Cf252\n','exit']
    setup = 'source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre2/setup.sh'
    import logging
    log = logging.getLogger('test.log')
    with open('output.txt','w+') as output:
        proc = Process_withENV(setup,output,log,hook=hook)
        print "@proc setup recode=%d"%proc.initialize()
        thread = threading.Thread(target=add,args=[proc,comm])
        thread.start()
        #proc.set_exe(comm)
        print "proc.start = %s, thread.start=%s"%(proc.start,thread.start)
        proc.start()
        proc.join()

    print '@proc finished'

