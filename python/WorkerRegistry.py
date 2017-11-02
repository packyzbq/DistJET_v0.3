wRegistery_log = None

import threading
import time
import traceback

from Util.Config import Config

class WorkerStatus:
    NEW = -1
    INITIALIZED = 0
    INITIALIZE_FAIL = 1
    IDLE = 7
    RUNNING = 2
    ERROR = 3
    LOST = 4
    FINALIZED = 5
    FINALIZE_FAIL = 6

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
        if self.policy.getPolicyattr('LOST_WORKER_TIMEOUT'):
            return time.time()-self.last_contact_time > self.policy.getPolicyattr('LOST_WORKER_TIMEOUT')
        return False

    def getStatus(self):
        return self.status

    def reinit(self):
        self.lock.acquire()
        try:
            self.init_times+=1
            return self.init_times < Config.getPolicyattr('INITIAL_TRY_TIME')
        finally:
            self.lock.release()
    def refin(self):
        self.lock.acquire()
        try:
            self.init_times += 1
            return self.init_times < Config.getPolicyattr('FIN_TRY_TIME')
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
                wRegistery_log.warning('worker already registered: wid=%d, worker_uuid=%s',wid,w_uuid)
                return None
            else:
                self.last_wid+=1
                w = WorkerEntry(self.last_wid, w_uuid, max_capacity)
                w.alive = True
                self.__all_workers[self.last_wid] = w
                self.__all_workers_uuid[w_uuid] = self.last_wid
                self.alive_workers.add(w_uuid)
                wRegistery_log.info('new worker registered: wid=%d, worker_uuid=%s',self.last_wid, w_uuid)
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
        :return: bool, uuid
        """
        try:
            w_uuid = self.__all_workers[wid].w_uuid
        except KeyError:
            wRegistery_log.warning('attempt to remove not registered worker: wid=%d', wid)
            return False,None
        else:
            if w_uuid in self.alive_workers:
                # finalize worker
                wRegistery_log.info('attempt to remove alive worker: wid=%d',wid)
                return False,w_uuid
            else:
                wRegistery_log.info('worker removed: wid=%d',wid)
                try:
                    self.lock.acquire()
                    del(self.__all_workers[wid])
                    del(self.__all_workers_uuid[w_uuid])
                except KeyError:
                    wRegistery_log.warning('[WorkerRegistry]: can not find worker when remove worker=%d, uuid=%s', wid, w_uuid)
                    return False, None
                finally:
                    self.lock.release()
                return True ,None

    def terminate_worker(self,wid):
        wentry = self.get_entry(wid)
        if wentry.w_uuid in self.alive_workers:
            if wentry.getStatus() == WorkerStatus.FINALIZED:
                self.alive_workers.remove(wentry.w_uuid)
                return True
            else:
                wRegistery_log.error('worker %d is not finalized, status=%s'%(wid,wentry.getStatus()))
                return False
        else:
            wRegistery_log.error('Cannot find worker %d'%wid)
            return False

    def get_entry(self,wid):
        if self.__all_workers.has_key(wid):
            return self.__all_workers[wid]
        else:
            #print 'Can not find worker %s, this is all workers %s'%(wid, self.__all_workers.keys())
            return None

    def get_by_uuid(self, w_uuid):
        return self.get_entry(self.__all_workers_uuid[w_uuid])

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
        for uuid in self.alive_workers:
            entry = self.get_by_uuid(uuid)
            # the smaller wid has the high priority
            if(entry.capacity() > 0):
                avalible_list.append(entry)
        return avalible_list

    def isAlive(self,wid):
        return self.get_entry(wid).w_uuid in self.alive_workers

    def hasAlive(self):
        return len(self.alive_workers) != 0

    def __iter__(self):
        return self.__all_workers.copy().__iter__()



    """
    ----------------------------useless function------------------------------------
    """
    def get_worker_list(self):
        return self.__all_workers.values()

    def get_capacity(self, wid):
        return self.__all_workers[wid].max_capacity

    def worker_reinit(self, wid):
        return self.get_entry(wid).reinit()

    def worker_refin(self, wid):
        return self.get_entry(wid).refin()

    def sync_capacity(self, wid, capacity):
        # TODO the num of assigned task is incompatable with worker
        wentry = self.get_entry(wid)
        if capacity != wentry.max_capacity - wentry.assigned:
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
        flag = True
        for uuid in self.alive_workers:
            wentry = self.get_by_uuid(uuid)
            if str(wentry.wid) in exp or int(wentry.wid) in exp:
                continue
            elif wentry.status in [WorkerStatus.RUNNING, WorkerStatus.INITILAZED]:
                wRegistery_log.info('[Registry] worker %s is in status=%s, cannot finalize'%(wentry.wid, wentry.status))
                flag = False
                return flag
        return flag

    def checkFinalize(self,exp=[]):
        for uuid in self.alive_workers:
            entry = self.get_by_uuid(uuid)
            if entry.status != WorkerStatus.FINALIZED:
                return False
        return True

    def setContacttime(self, uuid, time):
        wid = self.__all_workers_uuid[uuid]
        self.__all_workers[wid].last_contact_time = time

    def setStatus(self,wid,status):
        wid = int(wid)
        wentry = self.get_entry(wid)
        if wentry.alive:
            wentry.alive_lock.acquire()
            wentry.status = status
            wentry.alive_lock.release()
        else:
            wRegistery_log.warning('[Registry] Worker %s is not alive')
            wentry.alive_lock.acquire()
            wentry.alive = True
            wentry.status = status
            wentry.alive_lock.release()
            self.alive_workers.add(wentry.w_uuid)


    def checkLostWorker(self):
        """
        check if there are lost workers, and do some handle
        :return:
        """
        lostworker = []
        for w in self.__all_workers.values():
            if w.isLost():
                lostworker.append(w.wid)
                w.alive = False
                self.alive_workers.remove(w.w_uuid)
        return lostworker

    def checkIDLETimeout(self):
        list = []
        timeout = Config.getCFGattr('IDLE_WORKER_TIMEOUT')
        if not timeout or timeout==0:
            return None
        else:
            for w in self.__all_workers.values():
                if w.alive and w.worker_status == WorkerStatus.IDLE:
                    if w.idle_time != 0:
                        if time.time()-w.idle_time > Config.getPolicyattr('IDLE_WORKER_TIMEOUT'):
                            list.append(w.wid)
                    else:
                        w.idle_time = time.time()
        return list