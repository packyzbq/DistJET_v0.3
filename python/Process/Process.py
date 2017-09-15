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
class Process_withENV:
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
        self.process = subprocess.Popen(['sh'], shell=self.shell, stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid())
        self.pid = self.process.pid

        self.logFile.write('@Process: create process for tasks, pid= %d\n' %self.pid)

        self.timeout = timeout
        self.recode = None
        self.status = None

        self.start = None
        self.killed = None
        self.fatalLine = None
        self.logPharser = Parser()

        self.ignoreFail = False
        self.stop = False

    def set_exe(self,command_list):
        self.exec_queue_lock.acquire()
        try:
            for comm in command_list:
                self.executable.put(comm)
            if command_list[-1] != 'exit':
                self.executable.put('echo "recode:$?"')
        finally:
            self.exec_queue_lock.release()

    def stop(self):
        self.stop = True


    def initialize(self):
        """
        setup env for process
        :return: -1 no process; -2 setup timeout; 0 success;
        """
        if self.process is None:
            return -1
        if self.initial is None:
            return 0
        for comm in self.initial:
            self.process.stdin.write(comm)
        while True:
            fs = select.select([self.process.stdout],[],[],10)
            if not fs[0]:
                self.logFile.write('@Process initial timeout')
                return -2
            if self.process.stdout in fs[0]:
                data = os.read(self.process.stdout.fileno(), 1024)
                if "recode" in data:
                    ds = data.split()
                    recode = ds[-1].split("recode")[-1][1:]
                    return recode
                else:
                    self.logFile.write('[Setup_INFO] %s'%data)

    def run(self):
        while not self.stop:
            try:
                self.exec_queue_lock.acquire()
                if not self.executable.empty():
                    script = self.executable.get()
                    self.exec_queue_lock.release()
                    if script == 'exit':
                        break
                    self.start = time.time()
                    self.logFile.write('\n' + '*' * 20 + ' script "%s" Running log ' % script[:-1] + '*' * 20 + '\n')
                    self.process.stdin.write(script)
                    while True:
                        fs = select.select([self.process.stdout],[],[],self.timeout)
                        if not fs[0]:
                            # No response
                            self.status = status.ANR
                            if self.hook and callable(self.hook):
                                self.hook(self.status,self.recode)
                            self._kill_task()
                            self.process = self._restart()
                            break
                        data = os.read(self.process.stdout.fileno(),1024)
                        if not data:
                            break
                        st = data.split("\n")
                        if "exitcode" in st[-2]:
                            for line in st[:-2]:
                                self.logFile.write(line)
                            self.recode = st[-2][-1]
                            self.logFile.write("return code = %s"%self.recode)
                            if int(self.recode) == 0:
                                self.status = status.SUCCESS
                            if self.hook and callable(self.hook):
                                self.hook(self.status, self.recode)
                            break
                        elif (not self.ignoreFail) and (not self._parseLog(data)):
                            self.status = status.FAIL
                            self.recode = -1
                            if self.hook and callable(self.hook):
                                self.hook(self.status, self.recode)
                            self._kill_task()
                            self.process = self._restart()
                            break
                        else:
                            self.logFile.write(data)

                else:
                    self.exec_queue_lock.release()
                    time.sleep(1)
            except Exception,e:
                self.logFile.write('@Process catch error: %s'%e.message)

        self._burnProcess()



    def _kill_task(self):
        self.logFile.write('@Process: kill task\n')
        if not self.process:
            return
        import os, signal
        self.process.stdout.flush()
        pgrp = os.getpgid(self.pid)
        try:
            os.killpg(pgrp,signal.SIGHUP)
            self.process.wait()
        except:
            self.logFile.write()

    def _restart(self):
        proc = subprocess.Popen(['sh'], shell=self.shell, stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,preexec_fn=os.setsid())
        self.pid = proc.pid

        return proc

    def _parseLog(self,data):
        result,self.fatalline = self.logParser.parse(data)
        return result

    def _burnProcess(self):
        self.process.terminate()
        self.process.wait()



class Process:
    def __init__(self, exe, logfile, env_comm, shell=True,timeout=0, ignoreFail=False):
        self.shell = shell
        self.ignoreFail = ignoreFail
        self.executable = exe

        # Log file name depends on what we are running
        self.logFile = logfile
        self.logFile.write('@Process: create process for tasks,executable=%s\n'%self.executable)

        self.logParser = Parser()
        # Merge stdout and stderr
        self.stdout  = subprocess.PIPE
        self.process = None
        self.pid = None
        self.returncode = None
        self.status = None
        self.timeout = timeout
        self.duration = None
        self.start = None
        self.killed = None
        self.fatalLine = None

    def run(self):
        self.start = time.time()
        self.logFile.write(self.executable)
        self.process = subprocess.Popen(args=self.executable, shell=self.shell, stdout=self.stdout,
                                        stderr=subprocess.STDOUT)
        self.pid = self.process.pid
        self.logFile.write('\n'+'*'*20+' Running log '+'*'*20+'\n')
        while True:
            fs = select.select([self.process.stdout], [], [])
            if not fs[0]:
                # No response
                self.status = status.ANR
                self._kill()
                break
            if self.process.stdout in fs[0]:
                # Incoming message to parse
                data = os.read(self.process.stdout.fileno(), 1024)
                if not data:
                    break
                # If it is called in analysis step, we print the log info to screen
                self.logFile.write(data)
                if (not self.ignoreFail) and (not self._parseLog(data)):
                    self.status = status.FAIL
                    self._kill()
                    break
        self._burnProcess()
        self.logFile.write('\n\n\n\n\n')
        return self.returncode

    def getDuration(self):
        return self.duration

    def _kill(self):
        self.logFile.write('@Process: kill task\n')
        if not self.process:
            return
        import os, signal
        try:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
        except:
            pass

    def _burnProcess(self):
        self.returncode = self.process.wait()
        if self.ignoreFail:
            tmp_recode = self.returncode
            self.returncode = 0
            self.logFile.write('\n@Process: original return code = %s, ignoreFail -> return code = %s\n'%(tmp_recode,self.returncode))
        if self.status:
            return
        if 0 == self.returncode:
            self.status = status.SUCCESS
        else:
            self.status = status.FAIL
        # FIXME: it seems that root macro process won't give a 0 return code
        if type(self.executable) == types.ListType and self.executable[0] == 'root':
            self.status = status.SUCCESS
        if type(self.executable) == types.StringType and self.executable.startswith('root'):
            self.status = status.SUCCESS
        self.logFile.write('-'*10+'\n')
        self.logFile.write('@Process: result status = %s, recode = %s\n'%(self.status,self.returncode))

    def outcome(self):
        if self.status == status.SUCCESS:
            return True, ''
        if self.fatalLine:
            return False, 'FatalLine: ' + self.fatalLine
        return False, status.describe(self.status)

    def _parseLog(self,data):
        result,self.fatalline = self.logParser.parse(data)
        return result
