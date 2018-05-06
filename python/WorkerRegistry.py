from Util import logger
wRegistery_log = logger.getLogger("Registry")

import threading
import time
import traceback

from Util.Config import Config

class WorkerStatus:
    (NEW,
    INITIALIZED,
    INITIALIZING,
    INITIALIZE_FAIL,
    SCHEDULED,
    RUNNING,
    ERROR,
    LOST,
    RECONNECT,
    FINALIZED,
    FINALIZING,
    FINALIZE_FAIL,
    IDLE) = range(0,13)
    des = {
        NEW: "NEW",
        INITIALIZING: "INITIALIZING",
        INITIALIZE_FAIL:"INITIALIZE_FAIL",
        SCHEDULED: "SCHEDULED",
        RUNNING:"RUNNING",
        ERROR:"ERROR",
        LOST:"LOST",
        RECONNECT:"RECONNECT",
        FINALIZED:"FINALIZED",
        FINALIZING:"FINALIZING",
        FINALIZE_FAIL:"FINALIZE_FAIL",
        IDLE:"IDLE"
    }
    @staticmethod
    def desc(status):
        if WorkerStatus.des.has_key(status):
            return WorkerStatus.des[status]
        else:
            return None

class WorkerEntry:
    def __init__(self, wid, w_uuid, max_capacity):
        self.wid = wid
        self.policy = Config()
        self.w_uuid = w_uuid
        self.registration_time = time.time()
        self.last_contact_time = self.registration_time
        self.idle_time = 0

        self.init_times = 0
        self.fin_times = 0

        self.max_capacity = max_capacity
        self.assigned = 0 # the number assigned to the worker

        self.status = WorkerStatus.NEW
        self.lock = threading.RLock()
        self.running_task = None

    def capacity(self):
        return self.max_capacity-self.assigned

    def isLost(self):
        if self.policy.getPolicyattr('lost_worker_timeout'):
            return time.time()-self.last_contact_time > self.policy.getPolicyattr('lost_worker_timeout')
        return False

    def getStatus(self):
        return self.status

    def reinit(self):
        self.lock.acquire()
        try:
            self.init_times+=1
            return self.init_times < Config.getPolicyattr('initial_try_time')
        finally:
            self.lock.release()
    def refin(self):
        self.lock.acquire()
        try:
            self.init_times += 1
            return self.init_times < Config.getPolicyattr('fin_try_time')
        finally:
            self.lock.release()

    def task_done(self,taskid):
        self.assigned-=1

    def task_assigned(self, taskid):
        self.assigned+=1

    def get_assigned(self):
        """
        help for monitor to get which task is running on nodes
        :return: tid list
        """
        return self.assigned

    def toDict(self):
        localtime = time.localtime(self.last_contact_time)
        t = time.strftime("%H:%M:%S",localtime)
        return {'wid':self.wid, 'status':self.status, 'running_task':self.running_task,'last_connect':t}



class WorkerRegistry:
    def __init__(self):
        self.__all_workers={}           # wid: worker_entry
        self.__all_workers_uuid = {}    # w_uuid: wid
        self.last_wid = 0
        self.lock = threading.RLock()

        self.alive_workers = set([])       # w_uuid

    def size(self):
        return len(self.__all_workers)

    def add_worker(self, w_uuid, max_capacity):
        self.lock.acquire()
        try:
            if self.__all_workers_uuid.has_key(w_uuid):
                wid = self.__all_workers_uuid[w_uuid]
                wRegistery_log.warning('[WorkerRegistry]worker already registered: wid=%d, worker_uuid=%s',wid,w_uuid)
                return None
            else:
                self.last_wid+=1
                w = WorkerEntry(self.last_wid, w_uuid, max_capacity)
                w.alive = True
                self.__all_workers[self.last_wid] = w
                self.__all_workers_uuid[w_uuid] = self.last_wid
                self.alive_workers.add(w_uuid)
                wRegistery_log.info('[WorkerRegistry]new worker registered: wid=%d, worker_uuid=%s',self.last_wid, w_uuid)
                return w
        except:
            wRegistery_log.error('[WorkerRegistry]: Error occurs when adding worker, msg=%s', traceback.format_exc())
        finally:
            self.lock.release()
            wRegistery_log.debug('[WorkerRegistry] After add worker')

    def remove_worker(self, wid):
        """
        remove worker, if worker alive, return worker's uuid to finalize worker, otherwise stop worker
        :param wid:
        :return: bool, str(failure reason)
        """
        try:
            w_uuid = self.__all_workers[wid].w_uuid
        except KeyError:
            wRegistery_log.warning('[WorkerRegistry]attempt to remove not registered worker: wid=%d', wid)
            return False,"Worker Not Registered"
        else:
            wRegistery_log.info('[WorkerRegistry]worker removed: wid=%d',wid)
            try:
                self.lock.acquire()
                del(self.__all_workers[wid])
                del(self.__all_workers_uuid[w_uuid])
                if w_uuid in self.alive_workers:
                    self.alive_workers.remove(w_uuid)
            except KeyError:
                wRegistery_log.warning('[WorkerRegistry]: can not find worker when remove worker=%d, uuid=%s', wid, w_uuid)
                return False, "Worker Not Found"
            finally:
                self.lock.release()
            return True ,None

    def terminate_worker(self,wid):
        wentry = self.get_entry(wid)
        if wentry.w_uuid in self.alive_workers:
            if wentry.getStatus() == WorkerStatus.FINALIZED:
                #self.alive_workers.remove(wentry.w_uuid)
                return True
            else:
                wRegistery_log.error('[WorkerRegistry]worker %d is not finalized, status=%s'%(wid,wentry.getStatus()))
                return False
        else:
            wRegistery_log.error('[WorkerRegistry]Cannot find worker %d'%wid)
            return False

    def get_entry(self,wid):
        if self.__all_workers.has_key(wid):
            return self.__all_workers[wid]
        else:
            wRegistery_log.error('[Registry] Cannot find worker %s, skip'%(wid))
            return None

    def get_by_uuid(self, w_uuid):
        e=None
        if self.__all_workers_uuid.has_key(w_uuid):
            e = self.get_entry(self.__all_workers_uuid[w_uuid])
        else:
            wRegistery_log.error('[Registry] Cannot find worker uuid = %s, skip'%(w_uuid))
        return e



    def task_done(self,wid):
        """
        for sync the task recorded by worker entry
        :param wid:
        :param task:
        :return: Boolean
        """
        self.__all_workers[wid].task_done()

    def task_assigned(self,wid):
        self.__all_workers[wid].task_assigned()

    def check_avalible(self):
        """
        check and return avalible workers( alive , assigned < capacity)
        :return: list of wid
        """
        avalible_list = []
        self.lock.acqurie()
        try:
            for uuid in self.alive_workers:
                entry = self.get_by_uuid(uuid)
                # the smaller wid has the high priority
                if entry and (entry.capacity() > 0):
                    avalible_list.append(entry)
        finally:
            self.lock.release()
        return avalible_list

    def isAlive(self,wid):
        e = self.get_entry(wid)
        if e:
            return e in self.alive_workers
        else:
            return False

    def hasAlive(self):
        return len(self.alive_workers) != 0

    def __iter__(self):
        return self.__all_workers.copy().__iter__()



    """
    ----------------------------useless function------------------------------------
    """
    def get_worker_list(self):
        return self.__all_workers.values()

    def get_worker_status(self):
        status = {}
        for wid,entry in self.__all_workers.items():
            status[wid] = WorkerStatus.desc(entry.status)
        return status

    def get_capacity(self, wid):
        return self.__all_workers[wid].max_capacity

    def worker_reinit(self, wid):
        e = self.get_entry(wid)
        if e:
            return e.reinit()
        return

    def worker_refin(self, wid):
        e = self.get_entry(wid)
        if e:
            return e.refin()
        return False

    def sync_capacity(self, wid, capacity):
        # TODO the num of assigned task is incompatable with worker
        wentry = self.get_entry(wid)
        if wentry is not None and capacity != wentry.max_capacity - wentry.assigned:
            wentry.alive_lock.acquire()
            wentry.assigned = wentry.max_capacity - capacity
            wentry.alive_lock.release()

    def checkIdle(self,exp=[]):
        """
        check if all workers is in IDLE status
        :return:
        """
        if exp:
            wRegistery_log.debug('[Registry] check idle exclude worker %s, type of exp = %s'%(exp,type(exp[0])))
        if len(self.alive_workers) == 0:
            return False
        flag = True
        self.lock.acquire()
        try:
            for uuid in self.alive_workers:
                wentry = self.get_by_uuid(uuid)
                if not wentry:
                    wRegistery_log.warning('[Registry] worker %s is not exists, skip'%uuid)
                    continue
                if str(wentry.wid) in exp or int(wentry.wid) in exp:
                    continue
                elif wentry.status in [WorkerStatus.RUNNING, WorkerStatus.INITIALIZED, WorkerStatus.SCHEDULED]:
                    #wRegistery_log.info('[Registry] worker %s is in status=%s, cannot finalize'%(wentry.wid, WorkerStatus.desc(wentry.status)))
                    flag = False
                    return flag
            return flag
        finally:
            self.lock.release()

    def checkRunning(self):
        """
        Return if any worker is running
        :return:
        """
        self.lock.acquire()
        try:
            for uuid in self.alive_workers:
                entry = self.get_by_uuid(uuid)
                if entry and entry.status and entry.status == WorkerStatus.RUNNING:
                    return True
            return False
        finally:
            self.lock.release()

    def checkFinalize(self,exp=[]):
        self.lock.acquire()
        try:
            for uuid in self.alive_workers:
                entry = self.get_by_uuid(uuid)
                if entry and entry.status and entry.status != WorkerStatus.FINALIZED:
                    wRegistery_log.warning('[Registry] @checkFinalize: worker %s status = %s'%(entry.wid,WorkerStatus.desc(entry.status)))
                    return False
            return True
        finally:
            self.lock.release()

    def checkError(self, wid=None):
        err_list = []
        self.lock.acquire()
        try:
            if wid is None:
                for uuid in self.alive_workers:
                    entry=self.get_by_uuid(uuid)
                    if entry and entry.status and entry.status in [WorkerStatus.FINALIZE_FAIL, WorkerStatus.INITIALIZE_FAIL]:
                        wRegistery_log.warning('[Registry] @checkError: worker %s status error, status = %s'%(entry.wid,WorkerStatus.desc(entry.status)))
                        err_list.append(entry.wid)
                return err_list
            else:
                entry = self.get_entry(wid)
                if entry:
                    return entry.status in [WorkerStatus.FINALIZE_FAIL, WorkerStatus.INITIALIZE_FAIL]
                else:
                    return False
        finally:
            self.lock.release()

    def setContacttime(self, uuid, time):
        try:
        	wid = self.__all_workers_uuid[uuid]
        	self.__all_workers[wid].last_contact_time = time
        except:
            print "Can not find worker :%s, all worker is below:\n"%uuid
            #for uuid in self.__all_workers_uuid.keys():
            #    print uuid+'\n'

    def setTask(self,wid, tid):
        entry = self.get_entry(wid)
        if entry:
            entry.running_task = tid

    def task_complete(self,wid):
        entry = self.get_entry(wid)
        if entry:
            entry.running_task = None



    def setStatus(self,wid,status):
        wid = int(wid)
        wentry = self.get_entry(wid)
        if wentry is None:
            return
        if status == WorkerStatus.IDLE and wentry.status != status:
            wentry.idle_time = time.time()
        if wentry.alive:
            wentry.lock.acquire()
            wentry.status = status
            wentry.lock.release()
        else:
            wRegistery_log.warning('[Registry] Worker %s is not alive'%wid)
            wentry.lock.acquire()
            wentry.alive = True
            wentry.status = status
            wentry.lock.release()
            self.lock.acquire()
            try:
                self.alive_workers.add(wentry.w_uuid)
            finally:
                self.lock.release()

    def setAlive(self,uuid):
        """
        when a worker reconnect to master, call this
        :param wid:
        :return:
        """
        if uuid in self.__all_workers_uuid.keys():
            self.lock.acquire()
            try:
                self.alive_workers.add(uuid)
            finally:
                self.lock.release()
            self.setStatus(self.__all_workers_uuid[uuid],WorkerStatus.RECONNECT)
        else:
            wRegistery_log.error("[Registry] uuid %s is not in registery"%uuid)


    def checkLostWorker(self, wid=None):
        """
        check if there are lost workers, and do some handle
        :return:
        """
        if wid:
            entry = self.get_entry(wid)
            if entry and ((entry.getStatus() == WorkerStatus.LOST) or (entry.isLost())):
                return True
            else:
                return False

        lostworker = []
        for w_u in self.alive_workers.copy():
            w = self.get_by_uuid(w_u)
            if w.w_uuid in self.alive_workers and w.isLost():
                lostworker.append(w.wid)
                w.alive = False
                self.lock.acquire()
                try:
                    self.alive_workers.remove(w.w_uuid)
                finally:
                    self.lock.release()
        return lostworker


    def checkIDLETimeout(self):
        list = []
        timeout = Config.getCFGattr('idle_worker_timeout')
        if not timeout or timeout==0:
            return None
        else:
            for w in self.__all_workers.values():
                if w.alive and w.worker_status == WorkerStatus.IDLE:
                    if w.idle_time != 0:
                        if Config.getPolicyattr('idle_worker_time') and time.time()-w.idle_time > Config.getPolicyattr('idle_worker_time'):
                            list.append(w.wid)
                    else:
                        w.idle_time = time.time()
        return list
