import time
import types

class TaskStatus:
    """
    task status enumeration
    """
    NEW             = 0
    INITIALIZED     = 1
    PROCESSING      = 2
    COMPLETED       = 3
    FAILED          = 4
    LOST            = 5
    HALT            = 6 # have scheduled , to be performed

class TaskDetail:
    """
    Details about tasks' status for a single execution attempt
    """

    def __init__(self):
        self.assigned_wid = -1
        self.time_start = 0
        self.time_exec = 0
        self.time_end = 0
        self.time_scheduled = 0
        self.info = None

        self.error = None

    def assign(self, wid):
        if wid <= 0:
            return False
        else:
            self.assigned_wid = wid
            self.time_scheduled = time.time()
            return True

    def fail(self, time_start, time_finish=time.time(), error_code=0):
        self.time_start = time_start
        self.time_end = time_finish
        self.error = error_code
        self.info = 'Fail'

    def complete(self, time_start, time_end):
        self.time_start = time_start
        self.time_end = time_end
        self.info = 'Complete'

    def withdraw(self, time_term):
        self.time_end = time_term
        self.info = 'Cancel'

class Task(object):
    """
    The object split from application. Include a tracer to record the history
    """
    task_id = 0

    def __init__(self, tid=None):
        if tid:
            self.tid = tid
            Task.task_id = self.tid+1
        else:
            self.tid = Task.task_id
            Task.task_id+=1
        self.status = TaskStatus.NEW
        self.history = [TaskDetail()]
        self.attemptime=0

        self.boot = []
        self.data = {}
        self.args = {}

        self.res_dir = None

    def initial(self, work_script=None, args=None, data = None, res_dir="./"):
        """
        :param work_script: the script path
        :param args: {}
        :param data: {}
        :param res_dir:
        :return:
        """
        if args is None:
            args = {}
        self.boot = work_script
        self.res_dir = res_dir
        self.data = data
        self.args = args
        self.status = TaskStatus.INITIALIZED

    def toDict(self):
        tmpdict = {}
        tmpdict['tid'] = self.tid
        tmpdict['boot'] = self.boot
        tmpdict['data'] = self.data
        tmpdict['args'] = self.args
        tmpdict['resdir'] = self.res_dir
        return tmpdict

    def status(self):
        return self.status

    def update(self,task):
        for i in range(0,len(task.history)):
            if i >= len(self.history):
                self.history.extend(task.history[i:])
                break
            if self.history[i].info is None:
                self.history[i] = task.history[i]


    def fail(self, time_start, time_end=time.time(), error = 0):
        self.status = TaskStatus.FAILED
        self.history[-1].fail(time_start, time_end, error)

    def complete(self, time_start, time_end):
        self.status = TaskStatus.COMPLETED
        self.history[-1].complete(time_start,time_end)

    def assign(self, wid):
        if not self.status is TaskStatus.INITIALIZED:
            self.history.append(TaskDetail())
        self.attemptime+=1
        self.history[-1].assign(wid)
        self.status = TaskStatus.HALT

    def withdraw(self, time_term):
        self.status = TaskStatus.INITIALIZED
        self.history[-1].withdraw(time_term)

    def getAttempt(self):
        return self.attemptime



    def getdata(self):
        return self.data

    def genCommand(self):
        #TODO change gen task command method
        comm_list=[]
        errmsg=None
        comm=None
        if not self.data:
            #FIXME: quick fix, can't generate command list
            if not self.args:
                comm_list.extend(self.boot)
            else:
                comm = self.boot[0]
                if self.args.has_key(0):
                    comm += self.args[0]
                comm_list.append(comm)
        elif type(self.data) == types.DictType:
            for k, data in self.data.items():
                if self.boot[k]:
                    comm = self.boot[k]+' '+data
                    if self.args.has_key(k) and self.args[k]:
                        comm+= ' '+self.args[k]
                    comm_list.append(comm)
                else:
                    errmsg='[Task] Gen Command Fail, cannot find boot script <%d> for data <%s>'%(k,data)
        else:
            errmsg = '[Task] Cannot recognize the boot<%s> and data<%s>'%(self.boot,self.data)
        return comm_list,errmsg



class ChainTask(Task):
    def __init__(self,tid=None):
        #Task.__init__(tid=tid)
        super(ChainTask,self).__init__(tid)
        self._father = set()  # task id set
        self._child = set()   # task id set

    def set_father(self,father):
        if isinstance(father,Task):
            father = father.tid
        else:
            print type(father)
        assert type(father) == types.IntType
        if father in self._father:
            return False
        self._father.add(father)
        return True

    def remove_father(self,father):
        if isinstance(father,Task):
            father = father.tid
        assert type(father) == types.IntType
        if father not in self._father:
            return False
        self._father.remove(father)
        return True

    def father_len(self):
        return len(self._father)

    def get_father_list(self):
        return self._father

    def set_child(self, child):
        if isinstance(child,Task):
            child = child.tid
        else:
            print type(child)
        assert type(child) == types.IntType
        if child in self._child:
            return False
        self._child.add(child)
        return True

    def remove_child(self, child):
        if isinstance(child,Task):
            child = child.tid
        assert type(child) == types.IntType
        if child not in self._child:
            return False
        self._child.remove(child)
        return True

    def child_len(self):
        return len(self._child)

    def get_child_list(self):
        return self._child
    
    def toDict(self):
        tmpdict = {}
        tmpdict['tid'] = self.tid
        tmpdict['boot'] = self.boot
        tmpdict['data'] = self.data
        tmpdict['args'] = self.args
        tmpdict['resdir'] = self.res_dir
        tmpdict['father'] = self._father
        tmpdict['child'] = self._child
        return tmpdict

