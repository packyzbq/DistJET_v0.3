import Queue
import datetime
import json
import os,sys
import select
import subprocess
import threading
import multiprocessing
import time
import traceback

import IR_Buffer_Module as IM

import HealthDetect as HD
from BaseThread import BaseThread
from MPI_Wrapper import Tags ,Client
from Util import logger
from WorkerRegistry import WorkerStatus
from python.Util import Config
from python.Util import Package

wlog = None

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


class HeartbeatThread(BaseThread):
    """
    ping to master, provide information and requirement
    """
    def __init__(self, client, worker_agent, cond):
        BaseThread.__init__(self, name='HeartbeatThread')
        self._client = client
        self.worker_agent = worker_agent
        self.queue_lock = threading.RLock()
        self.acquire_queue = Queue.Queue()         # entry = key:val
        self.interval = Conf.Config.getCFGattr('HeartBeatInterval') if Conf.Config.getCFGattr('HeartBeatInterval') else 1
        self.cond = cond
        global wlog
    def run(self):
        #add first time to ping master, register to master
        send_dict = {}
        send_dict['flag'] = 'FP'
        send_dict[Tags.MPI_REGISTY] = {'capacity':self.worker_agent.capacity}
        send_dict['ctime'] = time.time()
        send_dict['uuid'] = self.worker_agent.uuid
        send_str = Package.pack_obj(send_dict)
        wlog.debug('[HeartBeat] Send msg = %s'%send_dict)
        ret = self._client.send_string(send_str, len(send_str),0,Tags.MPI_REGISTY)
        if ret != 0:
            #TODO send error,add handler
            pass

        # wait for the wid and init msg from master
        self.cond.acquire()
        self.cond.wait()
        self.cond.release()

        while not self.get_stop_flag():
            try:
                self.queue_lock.acquire()
                send_dict.clear()
                while not self.acquire_queue.empty():
                    tmp_d = self.acquire_queue.get()
                    if send_dict.has_key(tmp_d.keys()[0]):
                        wlog.warning('[HeartBeatThread]: Reduplicated key=%s when build up heart beat message, skip it'%tmp_d.keys()[0])
                        continue
                    send_dict = dict(send_dict, **tmp_d)
                self.queue_lock.release()
                send_dict['Task'] = {}
                while not self.worker_agent.task_completed_queue.empty():
                    task = self.worker_agent.task_completed_queue.get()
                    send_dict['Task'] = dict(send_dict['Task'],**task)
                send_dict['uuid'] = self.worker_agent.uuid
                send_dict['wid'] = self.worker_agent.wid
                send_dict['health'] = self.worker_agent.health_info()
                send_dict['rTask'] = self.worker_agent.getRuntasklist()
                send_dict['ctime'] = time.time()
                # before send heartbeat, sync agent status
                self.worker_agent.status_lock.acquire()
                send_dict['wstatus'] = self.worker_agent.status
                self.worker_agent.status_lock.release()
                send_str = json.dumps(send_dict)
#                wlog.debug('[HeartBeat] Send msg = %s'%send_str)
                ret = self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
                if ret != 0:
                    #TODO add send error handler
                    pass
            except Exception:
                wlog.error('[HeartBeatThread]: unkown error, thread stop. msg=%s', traceback.format_exc())
                break
            else:
                time.sleep(self.interval)

        # the last time to ping Master
        if not self.acquire_queue.empty():
            remain_command = ''
            while not self.acquire_queue.empty():
                remain_command+=self.acquire_queue.get().keys()
            wlog.waring('[HeartBeat] Acquire Queue has more command, %s, ignore them'%remain_command)
        send_dict.clear()
        send_dict['wid'] = self.worker_agent.wid
        send_dict['uuid'] = self.worker_agent.uuid
        send_dict['flag'] = 'LP'
        send_dict['Task'] = {}
        while not self.worker_agent.task_completed_queue.empty():
            task = self.worker_agent.task_completed_queue.get()
            #FIXME: change to task obj
            send_dict['Task'] = dict(send_dict['Task'],**task)
        # add node health information
        send_dict['health'] = self.worker_agent.health_info()
        send_dict['ctime'] = time.time()
        #send_dict['wstatus'] = self.worker_agent.worker.status
        send_str = Package.pack_obj(send_dict)
        wlog.debug('[HeartBeat] Send msg = %s'%send_dict)
        ret = self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
        if ret != 0:
            #TODO add send error handler
            pass



    def set_ping_duration(self, interval):
        self.interval = interval


class WorkerAgent:

    def __init__(self,name=None,capacity=1):
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        if name is None:
            name = self.uuid
        global wlog
        wlog = logger.getLogger('Worker_%s'%name)
        self.worker_class = None

        self.recv_buff = IM.IRecv_buffer()
        self.__should_stop = False
        Config.Config()
        self.cfg = Config.Config
        if self.cfg.isload():
            wlog.debug('[Agent] Loaded config file')
        wlog.debug('[Agent] Start to connect to service <%s>' % self.cfg.getCFGattr('svc_name'))
        self.client = Client(self.recv_buff, self.cfg.getCFGattr('svc_name'), self.uuid)
        ret = self.client.initial()
        if ret != 0:
            #TODO client initial error, add handler
            wlog.error('[Agent] Client initialize error, errcode = %d'%ret)
            #exit()

        self.wid = None
        self.appid = None
        self.capacity = capacity
        self.task_queue = Queue.Queue(maxsize=self.capacity) #store task obj
        self.task_completed_queue = Queue.Queue()
        self.ignoreTask=[]

        self.initExecutor=None #init task obj
        self.tmpLock = threading.RLock()
        self.finExecutor=None

        self.fin_flag = False
        self.initial_flag = False
        self.app_fin_flag = False
        self.halt_flag = False

        self.heartcond = threading.Condition()
        self.heartbeat = HeartbeatThread(self.client, self, self.heartcond)

        self.worker_list = []
        self.worker_status={}
        self.cond_list = {}

    def run(self):