import Queue

import WorkerRegistry
import Task
import IAppManager

from Util import logger
from Util.Config import Config

scheduler_log = logger.getLogger('AppMgr')



class IScheduler:
    def __init__(self, master, appmgr, worker_registry=None):
        self.master = master
        self.appid = None
        self.worker_registry = worker_registry
        self.appmgr = appmgr
        self.task_todo_queue = Queue.Queue() # task queue, store task obj
        scheduler_log.info('[Scheduler] Load tasks created by AppMgr')
        self.task_list = self.appmgr.get_app_task_list()
        for tid, task in self.task_list.items():
            if not isinstance(task, Task.ChainTask):
                self.task_todo_queue.put(tid)
            elif task.father_len() == 0:
                self.task_todo_queue.put(tid)

        scheduler_log.info('[Scheduler] Load %d tasks, add %d task to queue'%(len(self.task_list),self.task_todo_queue.qsize()))
        self.scheduled_task_list = {}       # wid: tid_list
        self.completed_queue = Queue.Queue()
        self.runflag = self.task_todo_queue.qsize() > 0

    def initialize(self):
        """
        Initialize the TaskScheduler passing the job input parameters as specified by the user when starting the run.
        :return:
        """
        pass

    def run(self):
        """
        :return:
        """
        pass

    def finalize(self):
        """
        The operation when Scheduler exit
        :return:
        """
        pass

    def assignTask(self, w_entry):
        """
        The master call this method when a Worker ask for tasks
        :param w_entry:
        :return: a list of assigned task obj
        """
        raise NotImplementedError

    def setWorkerRegistry(self, worker_registry):
        """
        :param worker_registry:
        :return:
        """
        self.worker_registry = worker_registry

    def has_more_work(self):
        """
        Return ture if current app has more work( when the number of works of app is larger than sum of workers' capacities)
        :return: bool
        """
        return not self.task_todo_queue.empty()
        #if not self.task_todo_queue.empty():
            #scheduler_log.debug('task_todo_quue has task num = %d'%self.task_todo_queue.qsize())
        #if self.task_todo_queue.empty():
            #scheduler_log.info('[Scheduler] Complete tasks number: %s; All task number: %s'%(self.completed_queue.qsize(),len(self.task_list)))
            #return False
        #if self.task_todo_queue.empty():
            #return not self.completed_queue.qsize() == len(self.task_list)
        #else:
            #return True

    def is_all_task_scheduled(self):
        if not self.task_todo_queue.empty():
            scheduler_log.debug('@is_all_task_sch: todo queue has tasks')
            return False
        schedule_list_copy = self.scheduled_task_list.copy()
        sum = 0
        for l in schedule_list_copy.values():
            if l:
                sum+=len(l)
        if sum+self.completed_queue.qsize() >= len(self.task_list):
            return True
        else:
            scheduler_log.debug('@is_all_task_sch: has chain tasks')
            return False

    def has_scheduled_work(self,wid=None):
        if wid:
            return len(self.scheduled_task_list[wid])!=0
        else:
            flag = False
            for k in self.scheduled_task_list.keys():
                if len(self.scheduled_task_list[k]) != 0:
                    #scheduler_log.debug('worker %d has task %s'%(k,self.scheduled_task_list[k]))
                    flag = True
                    break
        return flag

    def task_failed(self, wid, task):
        """
        called when tasks completed with failure
        :param wid: worker id
        :param tid: task id
        :param time_start:  the start time of the task, used for recoding
        :param time_finish: the end time of the task, used for recoding
        :param error: error code of the task
        :return:
        """
        raise NotImplementedError

    def task_completed(self, wid, task):
        """
        this method is called when task completed ok.
        :param wid:
        :param tid:
        :param time_start:
        :param time_finish:
        :return:
        """
        raise NotImplementedError

    def get_task(self,tid):
        return self.task_list[tid]

    def worker_finalized(self, wid):
        """
        worker finalize a app, and can start another app
        :param wid:
        :return:
        """
        raise  NotImplementedError

    def setup_worker(self):
        """
        returns the setup command of the app
        :return:
        """
        return self.appmgr.setup_app()

    def uninstall_worker(self):
        """
        return the unsetup command of app
        :return:
        """
        return self.appmgr.uninstall_app()


# -------------------------discard-------------------------------
    def init_worker(self):
        app = self.appmgr.get_current_app()
        task_dict = {}
        task_dict['boot'] = app.app_init_boot
        task_dict['args'] = {}
        task_dict['data'] = {}
        task_dict = dict(task_dict, **app.app_init_extra)
        task_dict['resdir'] = app.res_dir
        return task_dict

    def fin_worker(self):
        app = self.appmgr.get_current_app()
        task_dict = {}
        task_dict['boot'] = app.app_fin_boot
        task_dict['resdir'] = app.res_dir
        task_dict['data'] = {}
        task_dict['args'] = {}
        task_dict = dict(task_dict,**app.app_fin_extra)
        return task_dict

    def worker_initialized(self, wid):
        """
        called by Master when a worker agent successfully initialized the worker, (maybe check the init_output)
        when the method returns, the worker can be marked as ready
        :param wid:
        :return:
        """
        raise NotImplementedError

    def worker_added(self, wid):
        """
        This method is called by RunMaster when the new worker agent is added. Application specific initialization data
        may be assigned to w_entry.init_input at this point.
        :param wid:
        :return:
        """
        raise NotImplementedError

    def worker_removed(self, wid, time_point):
        """
        This method is called when the worker has been removed (either lost or terminated due to some reason).
        :param wid:
        :return:
        """
        raise
# -------------------------discard-------------------------------

class SimpleTaskScheduler(IScheduler):

    def assignTask(self, wid):
        room = self.worker_registry.get_entry(wid).capacity()
        task_list = []
        if not self.scheduled_task_list.has_key(wid):
            self.scheduled_task_list[wid] = []
        if self.task_todo_queue.empty():
            # FIXME: now worker ask for one task each time
            return task_list
            # TODO: need a select algothm to choose from which worker pull back tasks
            # pull idle task back from worker and assign to other worker
            # try pull back task
            '''for workerid, worker_task_list in self.scheduled_task_list.items():
                if wid == workerid:
                    continue
                while len(worker_task_list) > self.worker_registry.get_capacity(workerid) :
                    flag, tmptask = self.master.try_pullback(workerid,worker_task_list[-1])
                    if flag:
                        worker_task_list.pop()
                        break
                task = self.task_list[tmptask.tid]
                task.update(tmptask)
                task.assign(wid)
                task_list.append(task)
                self.scheduled_task_list[wid].append(task.tid)'''

        else:
            # assign 1 task once
            print "assign 1 task to worker %d, todo_queue_size = %d"%(wid,self.task_todo_queue.qsize())
            task = self.selectTask()
            if task is None:
                return task_list
            task.assign(wid)
            task_list.append(task)
            self.scheduled_task_list[wid].append(task.tid)
            # assign tasks depends on the capacity of task
            #while room >= 1 and not self.task_todo_queue.empty():
            #    tid = self.task_todo_queue.get()
            #    self.get_task(tid).assign(wid)
            #    task_list.append(tid)
            #    room-=1
            #    self.scheduled_task_list[wid].append(tid)
        #print "task_list = %s"%task_list
        if task_list:
            scheduler_log.info('[Scheduler] Assign %s to worker %s' % (self.scheduled_task_list[wid][-room:], wid))
        return task_list


    def task_completed(self, wid, task):
        wid = int(wid)
        tid = task.tid
        # delete from scheduled task list
        if tid in self.scheduled_task_list[wid]:
            self.scheduled_task_list[wid].remove(tid)
        scheduler_log.info('[Scheduler] Task %s complete' % tid)
        scheduler_log.debug('[Scheduler] Task %s complete, remove form scheduled_task_list, now = %s' % (tid, self.scheduled_task_list))
        self.completed_queue.put(task)
        # update chain task
        #print type(task)
        self.updateTask(task)
        if isinstance(task,Task.ChainTask):
            for child_id in task.get_child_list():
                child = self.get_task(child_id)
                #child = self.task_list(child.tid)
                child.remove_father(task)
                if child.father_len() == 0:
                    self.task_todo_queue.put(child.tid)
                    scheduler_log.debug('[Scheduler] ChainTask %s add to todo list'%(child.tid))
                #if self.updateTask(child):
                #    scheduler_log.debug('[Scheduler] Update task successfully')
                scheduler_log.debug("[Scheduler] Task %s remove father %s , father list = %s"%(str(child.tid),str(task.tid),[str(tid) for tid in child.get_father_list()]))
                #scheduler_log.debug("[Scheduler] Task Compare: child:<%s>, task:<%s>"%(child.toDict(),self.get_task(child.tid).toDict()))

    def task_failed(self, wid, task):
        wid = int(wid)
        tid = task.tid
        if tid in self.scheduled_task_list[wid]:
            self.scheduled_task_list[wid].remove(tid)
        scheduler_log.info('[Scheduler] Task %s failed, errmsg=%s'%(tid,task.history[-1].error))
        attempt = self.master.cfg.getPolicyattr('task_attempt_time')
        if task.attemptime < attempt:
            scheduler_log.info('[Scheduler] Redo Task %s'%tid)
            self.task_todo_queue.put(task.tid)
        else:
            scheduler_log.info('[Scheduler] Task %s execute times is limited, ignore'%tid)
            self.completed_queue.put(task)
        self.updateTask(task)

    def worker_initialized(self, wid):
        entry = self.worker_registry.get_entry(wid)
        try:
            entry.alive_lock.acquire()
            entry.status = WorkerRegistry.WorkerStatus.INITILAZED
        finally:
            entry.alive_lock.release()

    def worker_removed(self, wid, time_point):
        """
        when worker is force removed, add scheduled tasks back to queue
        :param wid:
        :param time_point:
        :return:
        """
        tl = []
        scheduler_log.info("[Scheduler] Remove worker %s..."%(wid))
        if self.scheduled_task_list.has_key(wid):
            for tid in self.scheduled_task_list[wid]:
                tl.append(tid)
                task = self.get_task(tid)
                task.withdraw(time_point)
                self.task_todo_queue.put(task.tid)
                self.scheduled_task_list[wid].remove(tid)
            scheduler_log.info("[Scheduler] Pull back tasks %s from worker %s"%(tl, wid))


    def worker_finalized(self, wid):
        if self.worker_registry.isAlive(wid):
            try:
                w = self.worker_registry.get_entry(wid)
                w.lock.acquire()
                w.status = WorkerRegistry.WorkerStatus.FINALIZED
                if not self.worker_registry.terminate_worker(wid):
                    scheduler_log.error('[Scheduler] Cannot remove worker %s'%wid)
            finally:
                w.lock.release()

    def updateTask(self,task):
        if not task: #or isinstance(task,Task.Task):
            return False
        if self.task_list.has_key(task.tid):
            self.task_list[task.tid] = task
            return True
        else:
            return False

    # the schedule algorithm that decide which task is assigned
    # default pick up the first task in queue
    # return task obj
    def selectTask(self,**kwargs):
        pre_task = None
        while not self.task_todo_queue.empty():
            tid = self.task_todo_queue.get()
            task = self.get_task(tid)
            if isinstance(task, Task.ChainTask) and task.father_len() != 0:
                if pre_task and pre_task.tid == task.tid:
                    scheduler_log.warning("There is only one task with father %s" % task.get_father_list())
                    return None
                else:
                    pre_task = task
                    self.task_todo_queue.put(tid)
                continue
            else:
                return task
