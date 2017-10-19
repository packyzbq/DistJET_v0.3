import time
import types
import os,sys
import threading
import select
import subprocess
import Queue
import traceback
from Parser import Parser


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

#FIXME start a sh process, cannot know when the command finished
class Process_withENV(threading.Thread):
    """
    start process with setup env,
    """
    def __init__(self,initial,logfile, shell=True, timeout=0, ignoreFaile=False, hook=None):
        """
        :param initial: the command of setup
        :param logfile:  open file where exec_log write
        :param shell:
        :param timeout: select time out
        :param ignoreFaile:
        :param hook: when complete tasks, call this method
        """
        super(Process_withENV,self).__init__()
        print self.start

        self.shell = shell
        self.ignoreFail = ignoreFaile
        self.exec_queue_lock = threading.RLock()
        self.executable = Queue.Queue()

        self.hook = hook
        self.logFile = logfile
        self.initial = []
        if(type(initial) == types.ListType):
            self.initial.extend(initial)
        if(type(initial) == types.StringType):
            self.initial.append(initial)

        self.stdout = subprocess.PIPE
        self.stdin = subprocess.PIPE
        print "<before create process>"
        self.process = subprocess.Popen(['bash'], stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid)
        self.pid = self.process.pid

        self.logFile.write('@Process: create process for tasks, pid= %d\n' %self.pid)
        print "<after create and write log>"

        self.timeout = timeout
        self.recode = None
        self.status = None

        self.start_time = None
        self.end = None
        self.killed = None
        self.fatalLine = None
        self.logParser = Parser()

        self.ignoreFail = False
        self.stop = False

    def set_exe(self,command_list):
        assert type(command_list) == types.ListType
        self.exec_queue_lock.acquire()
        try:
            for comm in command_list:
                self.executable.put(comm)
                print "<process> add command %s"%command_list
               # if comm != 'exit':
               #     self.executable.put('echo "recode:$?"\n')
        finally:
            self.exec_queue_lock.release()

    def stop(self,force=False):
        self.stop = True
        if force:
            self._kill_task()
        self.process.wait()


    def initialize(self):
        """
        setup env for process
        :return: -1 no process; -2 setup timeout; 0 success;
        """
        if self.process is None:
            return -1
        if self.initial is None:
            return 0
        print "@init: initial comm = %s"%self.initial
        for comm in self.initial:
            if comm[-1] != '\n':
                comm+='\n'
            self.process.stdin.write(comm)
        self.process.stdin.write('echo "recode=$?"\n')
        while True:
            fs = select.select([self.process.stdout],[],[],10)
            if not fs[0]:
                self.logFile.write('@Process initial timeout')
                self.logFile.flush()
                return -2
            if self.process.stdout in fs[0]:
                data = os.read(self.process.stdout.fileno(), 1024)
                if "recode" in data:
                    ds = data.split()
                    recode = ds[-1].split("recode")[-1][1:]
                    return int(recode)
                else:
                    self.logFile.write('[Setup_INFO] %s'%data)

                    self.logFile.flush()
            else:
                return 0

    def finalize_and_cleanup(self, command):
        if type(command) == types.ListType:
            self.set_exe(command)
        self.set_exe(["exit"])

    def run(self):
        while not self.stop:
            try:
                self.exec_queue_lock.acquire()
                print "<process> executable size = %d"%self.executable.qsize()
                if not self.executable.empty():
                    script = self.executable.get()
                    self.exec_queue_lock.release()
                    print "<process> get script=%s"%script
                    if script == 'exit':
                        break
                    if "recode" not in script:
                        self.start_time = time.time()
                        self.logFile.write('\n' + '*' * 20 + ' script "%s" Running log ' % script[:-1] + '*' * 20 + '\n')
                        self.logFile.flush()
                    self.process.stdin.write(script)
                    self.process.stdin.write('echo "recode:$?"\n')
                    while True:
                        fs = select.select([self.process.stdout],[],[],self.timeout)
                        if not fs[0]:
                            # No response
                            self.status = status.ANR
                            self.logFile.write('[Proc] Task no response, ready to kill')
                            if self.hook and callable(self.hook):
                                self.hook(self.status,self.recode, self.start_time, self.end)
                            self._kill_task()
                            tmp_list=[]
                            while not self.executable.empty():
                                tmp_list.append(self.executable.get())
                            self.process = self._restart()
                            if tmp_list:
                                self.set_exe(tmp_list)
                            break
                        data = os.read(self.process.stdout.fileno(),1024)
                        if not data:
                            break
                        st = data.split("\n")
                        if "recode" in st[-2]:
                            self.end = time.time()
                            for line in st[:-2]:
                                self.logFile.write(line)
                                self.logFile.flush()
                            self.recode = st[-2][-1]
                            self.logFile.write("\nreturn code = %s"%self.recode)
                            self.logFile.write("\nstart time = %s \nend time = %s"%(time.asctime(time.localtime(self.start_time)), time.asctime(time.localtime(self.end))))
                            self.logFile.flush()
                            if int(self.recode) == 0:
                                self.status = status.SUCCESS
                            if self.hook and callable(self.hook):
                                self.hook(self.status, self.recode, self.start_time, self.end)
                            break
                        elif (not self.ignoreFail) and (self.logParser and (not self._parseLog(data))):
                            self.status = status.FAIL
                            self.recode = -1
                            if self.hook and callable(self.hook):
                                self.hook(self.status, self.recode, self.start_time, self.end)
                            #self._kill_task()
                            #self.process = self._restart()
                            break
                        else:
                            self.logFile.write(data)
                            self.logFile.flush()

                else:
                    self.exec_queue_lock.release()
                    time.sleep(1)
            except Exception,e:
                self.logFile.write('@Process catch error: %s'%e.message)
                print traceback.format_exc()

        self._burnProcess()



    def _kill_task(self):
        if not self.process:
            return
        import os, signal
        self.process.stdout.flush()
        pgrp = os.getpgid(self.pid)
        self.logFile.write('[Proc] kill pid=%d\n' % pgrp)
        try:
            os.killpg(pgrp,signal.SIGHUP)
            self.process.wait()
        except:
            self.logFile.write('[Proc] KILLError: %s'%traceback.format_exc())

    def _restart(self):
        proc = subprocess.Popen(['bash'], shell=self.shell, stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid)
        self.pid = proc.pid
        self.logFile.write('[Proc] Restart a new process,pid=%d'%self.pid)
        self.logFile.flush()

        return proc

    def _parseLog(self,data):
        result,self.fatalline = self.logParser.parse(data)
        return result

    def _burnProcess(self):
        self.process.terminate()
        self.process.wait()

def hook(status, recode):
    print 'hook method called, status= %s, recode=%s'%(str(status),str(recode))

def add(proc,comm):
    for com in comm:
        proc.set_exe([com])
        #print '@thread add comm %s'%com
        time.sleep(2)

if __name__ == '__main__':
    comm = ['echo $HOME\n','echo "hello world"\n','python ./wait.py\n','exit']
    setup = 'source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre2/setup.sh'
    with open('output.txt','w+') as output:
        proc = Process_withENV(setup,output,timeout=5,hook=hook)
        print "@proc setup recode=%d"%proc.initialize()
        thread = threading.Thread(target=add,args=[proc,comm])
        thread.start()
        #proc.set_exe(comm)
        print "proc.start = %s, thread.start=%s"%(proc.start,thread.start)
        proc.start()
        proc.join()

    print '@proc finished'

