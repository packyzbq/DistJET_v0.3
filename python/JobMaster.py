import Queue
import datetime
import json
import time
import os

from Util import logger

control_log = logger.getLogger('Control_Log')
master_log = logger.getLogger('Master')

import IR_Buffer_Module as IM
import IScheduler
import WorkerRegistry

WorkerRegistry.wRegistery_log = master_log
import MPI_Wrapper

MPI_Wrapper.MPI_log = master_log

from BaseThread import BaseThread
from IAppManager import SimpleAppManager
from Task import TaskStatus
from python.Util.Conf import Config, set_inipath
from WorkerAgent import status


def MSG_wrapper(**kwd):
    return json.dumps(kwd)


class ControlThread(BaseThread):
    """
    monitor the worker registry to manage worker
    """

    def __init__(self, master):
        BaseThread.__init__(self, 'ControlThread')
        self.master = master
        self.processing = False

    def run(self):
        control_log.info('Control Thread start...')
        while not self.get_stop_flag():
            # check if lost
            lostworker = self.master.worker_registry.checkLostWorker()
            if lostworker:
                control_log.warning('Lost worker = %s' % lostworker)
                for wid in lostworker:
                    self.master.remove_worker(wid)
            # check if idle timeout
            # if Config.getCFGattr('IDLE_WORKER_TIMEOUT'):
            #    workerlist = self.master.worker_registry.checkIDLETimeout()
            #    for wid in workerlist:
            #        self.master.remove_worker(wid)

            time.sleep(self.master.cfg.getPolicyattr('CONTROL_DELAY'))

    def activateProcessing(self):
        self.processing = True


class IJobMaster:
    """
    interface used by Scheduler to control task scheduling
    """
    def stop(self):
        pass

    def try_pullback(self,wid, tid):
        raise NotImplementedError


class JobMaster(IJobMaster):
    def __init__(self, applications=None):
        self.cfg = Config()
        self.svc_name = self.cfg.getCFGattr('svc_name')
        if not self.svc_name:
            self.svc_name = 'Default'
        # worker list
        self.worker_registry = WorkerRegistry.WorkerRegistry()
        # scheduler
        self.task_scheduler = None
        self.appmgr = None

        self.recv_buffer = IM.IRecv_buffer()

        self.control_thread = ControlThread(self)

        self.applications = applications  # list

        self.command_q = Queue.Queue()

        self.__wid = 1
        self.__all_final_flag = False
        # TODO(optional) load customed AppManager
        self.appmgr = SimpleAppManager(apps=self.applications)
        master_log.debug('[Master] Appmgr has instanced')
        if not self.appmgr.runflag:
            # appmgr load task error, exit
            return
        # self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        if self.appmgr.get_current_app():
            self.task_scheduler = self.appmgr.get_current_app().scheduler(self, self.appmgr, self.worker_registry)
        else:
            self.task_scheduler = IScheduler.SimpleScheduler(self, self.appmgr, self.worker_registry)
        self.task_scheduler.appid = self.appmgr.get_current_appid()
        self.server = MPI_Wrapper.Server(self.recv_buffer, self.svc_name)
        ret = self.server.initialize()
        if ret != 0:
            master_log.error('[Master] Server initialize error, stop. errcode = %d' % ret)
            # TODO add error handler
            # exit()

        self.__stop = False

    def stop(self):
        # TaskScheduler is not a thread
        # if self.task_scheduler and self.task_scheduler.is_alive():
        #    self.task_scheduler.join()
        #    master_log.info('[Master] TaskScheduler has joined')
        if self.control_thread and self.control_thread.isAlive():
            self.control_thread.stop()
            self.control_thread.join()
            master_log.info('[Master] Control Thread has joined')
        ret = self.server.stop()
        if ret != 0:
            master_log.error('[Master] Server stop error, errcode = %d' % ret)
            # TODO add solution
        else:
            master_log.info('[Master] Server stop')
        self.__stop = True

    def register_worker(self, w_uuid, capacity=1):
        master_log.debug('[Master] register worker %s' % w_uuid)
        worker = self.worker_registry.add_worker(w_uuid, capacity)
        if not worker:
            master_log.warning('[Master] The uuid=%s of worker has already registered', w_uuid)
        else:
            worker_module_path = self.appmgr.current_app.specifiedWorker
            send_dict = {'wid': worker.wid, 'appid': self.task_scheduler.appid,
                         'init': self.task_scheduler.init_worker(), 'uuid': w_uuid, 'wmp': worker_module_path}
            self.command_q.put({MPI_Wrapper.Tags.MPI_REGISTY_ACK: send_dict})
            # send_str = MSG_wrapper(wid=worker.wid,appid=self.task_scheduler.appid, init=self.task_scheduler.init_worker())
            # self.server.send_string(send_str, len(send_str), w_uuid, MPI_Wrapper.Tags.MPI_REGISTY_ACK)

    def remove_worker(self, wid):
        flag, uuid = self.worker_registry.remove_worker(wid)
        if flag:
            self.task_scheduler.worker_removed(wid, time.time())
        else:
            # TODO MPI Layer handle this problem
            pass

    def anaylize_health(self, info):
        # TODO give a threshold of the health of a node
        pass

    def startProcessing(self):
        while not self.__stop:
            current_uuid = None
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                if msg.tag != -1:
                    master_log.debug('[Master] Receive msg = %s' % msg.sbuf[0:msg.size])
                    if msg.tag == MPI_Wrapper.Tags.MPI_DISCONNECT:
                        master_log.info("[Master] Agent disconnect")
                        continue
                    recv_dict = json.loads(msg.sbuf[0:msg.size])
                    current_uuid = recv_dict['uuid']
                    # master_log.debug('[Master] Receive msg from worker %s, keys = %s'%(recv_dict['wid'] if recv_dict.has_key('wid') else None,recv_dict.keys()))
                    if recv_dict.has_key('flag'):
                        if recv_dict['flag'] == 'firstPing' and msg.tag == MPI_Wrapper.Tags.MPI_REGISTY:
                            # register worker
                            # check dict's integrity
                            if self.check_msg_integrity('firstPing', recv_dict):
                                master_log.debug('[Master] Receive REGISTY msg = %s' % recv_dict)
                                self.register_worker(recv_dict['uuid'],
                                                     recv_dict[str(MPI_Wrapper.Tags.MPI_REGISTY)]['capacity'])
                            else:
                                master_log.error('firstPing msg incomplete, key=%s' % recv_dict.keys())
                                # self.command_q.put({MPI_Wrapper.Tags.APP_INI: self.appmgr.get_app_init(self.appmgr.current_app_id), 'uuid':recv_dict['uuid']})
                        elif recv_dict['flag'] == 'lastPing':
                            # last ping from worker, sync completed task, report node's health, logout and disconnect worker
                            master_log.debug(
                                '[Master] From worker %s Receive DISCONNECT msg = %s' % (recv_dict['wid'], recv_dict))
                            for tid, val in recv_dict['Task']:
                                # handle complete task
                                if self.check_msg_integrity('Task', val):
                                    if val['task_stat'] == status.SUCCESS:
                                        self.task_scheduler.task_completed(recv_dict['wid'], tid, val['time_start'],
                                                                           val['time_fin'])
                                    elif val['task_stat'] in [status.FAIL, status.TIMEOUT, status.OVERFLOW, status.ANR]:
                                        self.task_scheduler.task_failed(recv_dict['wid'], tid, val['time_start'],
                                                                        val['time_fin'], val['errcode'])
                                else:
                                    master_log.error('Task msg incomplete, key=%s' % val.keys())
                            # logout and disconnect worker
                            self.remove_worker(recv_dict['wid'])

                    # normal ping msg
                    if recv_dict.has_key('Task'):
                        # handle complete tasks
                        for tid, val in recv_dict['Task'].items():
                            # handle complete task
                            if self.check_msg_integrity('Task', val):
                                if val['task_stat'] == status.SUCCESS:
                                    self.task_scheduler.task_completed(recv_dict['wid'], tid, val['time_start'],
                                                                       val['time_fin'])
                                elif val['task_stat'] in [status.FAIL, status.TIMEOUT, status.OVERFLOW, status.ANR]:
                                    self.task_scheduler.task_failed(recv_dict['wid'], tid, val['time_start'],
                                                                    val['time_fin'], val['errcode'])
                                else:
                                    master_log.warning('[Master] Can not recognize the task status %s of Task %s' % (
                                    val['task_stat'], tid))
                            else:
                                master_log.error('Task msg incomplete, key=%s' % val.keys())
                    if recv_dict.has_key('health'):
                        # TODO handle node health
                        pass
                    if recv_dict.has_key('ctime'):
                        if not (recv_dict.has_key('flag') and recv_dict['flag'] == 'lastPing'):
                            self.worker_registry.setContacttime(recv_dict['uuid'], recv_dict['ctime'])
                    if recv_dict.has_key('wstatus'):
                        wentry = self.worker_registry.get_entry(recv_dict['wid'])
                        if wentry:
                            master_log.debug('[Master] Set worker %s status = %s' % (wentry.wid, recv_dict['wstatus']))
                            wentry.setStatus(recv_dict['wstatus'])

                    if recv_dict.has_key(str(MPI_Wrapper.Tags.APP_INI)):
                        v = recv_dict[str(MPI_Wrapper.Tags.APP_INI)]
                        master_log.debug('[Master] From worker %d receive a App_INI msg = %s' % (recv_dict['wid'], v))
                        if v['recode'] == status.SUCCESS:
                            self.task_scheduler.worker_initialized(recv_dict['wid'])
                            master_log.info('worker %d initialize successfully' % recv_dict['wid'])
                        # # assign tasks to worker
                        #                            assigned = self.task_scheduler.assignTask(recv_dict['wid'])
                        #                            if not assigned:
                        #                                master_log.error('[Master] Assign task to worker_%s error, worker is not alive'%recv_dict['wid'])
                        #                                #TODO connect worker check if worker is alive
                        #                            else:
                        #                                send_dict = {'tag':MPI_Wrapper.Tags.TASK_ADD}
                        #                                for tmptask in assigned:
                        #                                    send_dict = dict(send_dict, **({tmptask.tid:tmptask.toDict()}))
                        #                                self.command_q.put(send_dict)
                        # FIXME: reinit should in workeragent
                        else:
                            # initial worker failed
                            # TODO: Optional, add other operation
                            master_log.error('worker %d initialize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                            # check re-initial times in policy
                            if self.worker_registry.worker_reinit(v['wid']):
                                send_dict = {'wid': v['wid'], 'appid': self.task_scheduler.appid,
                                             'init': self.task_scheduler.init_worker(),
                                             'uuid': recv_dict['uuid']}
                                self.command_q.put({MPI_Wrapper.Tags.MPI_REGISTY_ACK: send_dict})
                            else:
                                # terminate worker
                                # send_dict = {'tag': MPI_Wrapper.Tags.WORKER_STOP}
                                self.command_q.put({MPI_Wrapper.Tags.WORKER_STOP: ""})

                    if recv_dict.has_key(str(MPI_Wrapper.Tags.TASK_ADD)):
                        v = recv_dict[str(MPI_Wrapper.Tags.TASK_ADD)]
                        master_log.debug('[Master] From worker %s receive a TASK_ADD msg = %s' % (recv_dict['wid'], v))
                        self.worker_registry.sync_capacity(recv_dict['wid'], int(v))
                        task_list = self.task_scheduler.assignTask(recv_dict['wid'])
                        if not task_list:
                            master_log.debug('[Master] No more task to do')
                            # according to Policy ,check other worker status and idle worker
                            if Config.getPolicyattr('WORKER_SYNC_QUIT'):
                                if self.worker_registry.checkIdle(exp=[recv_dict['wid']]):  # exp=[recv_dict['wid']]
                                    if self.__all_final_flag:
                                        master_log.info('[Master] Have send all finalize msg, skip this')
                                    else:
                                        tmp_dict = self.task_scheduler.fin_worker()
                                        master_log.debug('[Master] All worker have done, finalize all worker')
                                        self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict, 'extra': []})
                                        self.__all_final_flag = True
                                else:
                                    master_log.debug('[Master] There are still running worker, halt')
                                    self.command_q.put({MPI_Wrapper.Tags.WORKER_HALT: ''})
                            else:
                                tmp_dict = self.task_scheduler.fin_worker()
                                self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict})
                        else:
                            task_dict = {}
                            for tmptask in task_list:
                                task_dict[tmptask.tid] = tmptask.toDict()
                                master_log.info(
                                    '[Master] Assign task %d to worker %d' % (tmptask.tid, recv_dict['wid']))
                            self.command_q.put({MPI_Wrapper.Tags.TASK_ADD: task_dict})

                    if recv_dict.has_key(str(MPI_Wrapper.Tags.APP_FIN)):
                        v = recv_dict[str(MPI_Wrapper.Tags.APP_FIN)]
                        master_log.debug('[Master] From worker %s receive a APP_FIN msg = %s' % (recv_dict['wid'], v))
                        if v['recode'] == status.SUCCESS:
                            self.task_scheduler.worker_finalized(recv_dict['wid'])
                            self.worker_registry.setStatus(recv_dict['wid'], recv_dict['wstatus'])
                            master_log.info('worker %s finalized, set worker entry status = %s' % (
                            recv_dict['wid'], recv_dict['wstatus']))
                            if not self.appmgr.has_next_app():
                                master_log.debug('[Master] No more apps, worker logout')
                                self.command_q.put({MPI_Wrapper.Tags.LOGOUT: ""})
                            else:
                                # if all worker in Finalization status, go next app
                                if self.worker_registry.checkFinalize():
                                    # all worker in Finalization
                                    master_log.info('[Master] all worker finalized, do new app')
                                    self.appmgr.next_app()
                                    if self.appmgr.get_current_app():
                                        self.task_scheduler = self.appmgr.get_current_app().scheduler(self, self.appmgr,
                                                                                                      self.worker_registry)
                                    else:
                                        self.task_scheduler = IScheduler.SimpleScheduler(self, self.appmgr,
                                                                                         self.worker_registry)
                                    self.task_scheduler.appid = self.appmgr.get_current_appid()
                                    worker_module_path = self.appmgr.current_app.specifiedWorker
                                    send_dict = {MPI_Wrapper.Tags.MPI_REGISTY_ACK: {'appid': self.task_scheduler.appid,
                                                                                    'init': self.task_scheduler.init_worker(),
                                                                                    'wmp': worker_module_path,
                                                                                    'flag': 'NEWAPP'
                                                                                    },
                                                 'extra': []
                                                 }
                                    self.command_q.put(send_dict)
                                    self.__all_final_flag = False
                                    master_log.debug('[Master] setup new app, send RegAck to worker')
                                else:
                                    master_log.info('[Master] other worker is not finalized, wait...')
                        # FIXME: refinalize should doing in workeragent
                        else:
                            master_log.error('worker %d finalize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                            if self.worker_registry.worker_refin(recv_dict['wid']):
                                tmp_dict = self.task_scheduler.fin_worker()
                                self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict})
                            else:
                                send_dict = {MPI_Wrapper.Tags.WORKER_STOP: ""}
                                self.command_q.put(send_dict)

            while not self.command_q.empty():
                send_dict = self.command_q.get()
                if len(send_dict) != 0:
                    tag = send_dict.keys()[0]
                    if send_dict.has_key('extra') and (not send_dict['extra']):
                        del (send_dict['extra'])
                        send_str = json.dumps(send_dict)
                        master_log.debug('[Master] Send msg = %s, tag=%s, uuid=%s' % (
                        send_str, tag, self.worker_registry.alive_workers))
                        for uuid in self.worker_registry.alive_workers:
                            self.server.send_string(send_str, len(send_str), str(uuid), tag)
                    elif send_dict.has_key('extra') and send_dict['extra']:
                        tmplist = send_dict['extra']
                        del (send_dict['extra'])
                        send_str = json.dumps(send_dict)
                        master_log.debug('[Master] Send msg = %s' % send_str)
                        for uuid in tmplist:
                            self.server.send_string(send_str, len(send_str), uuid, tag)
                    else:
                        send_str = json.dumps(send_dict)
                        master_log.debug('[Master] Send to worker %s msg = %s' % (current_uuid, send_str))
                        self.server.send_string(send_str, len(send_str), current_uuid, tag)
            # master stop condition
            # time.sleep(1)
            if not self.task_scheduler.has_more_work() and not self.task_scheduler.has_scheduled_work():
                self.appmgr.finalize_app()
                if not self.appmgr.has_next_app() and self.worker_registry.size() == 0:
                    master_log.info("[Master] Application done, logout workers")
                    # TODO logout worker

                    self.stop()

    def check_msg_integrity(self, tag, msg):
        if tag == 'Task':
            return set(['task_stat', 'time_start', 'time_fin', 'errcode']).issubset(set(msg.keys()))
        elif tag == 'firstPing':
            return set(['uuid']).issubset(set(msg.keys()))
            # return set(['uuid', 'capacity']).issubset(set(msg.keys()))
        elif tag == 'health':
            return set(['CpuUsage', 'MemoUsage']).issubset(set(msg.keys()))

    def getRunFlag(self):
        return self.appmgr.runflag

    def finalize_worker(self, uuid):
        tmp_dict = self.task_scheduler.fin_worker()
        self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict, 'extra': [uuid]})

    def pullback_task(self, tid_list, wid):
        master_log.debug('[Master] Pull back tasks=%s from worker %s' % (tid_list, wid))
        uuid = self.worker_registry.get_entry(wid).w_uuid
        send_dict = {MPI_Wrapper.Tags.TASK_REMOVE: tid_list, 'extra': [uuid]}
        self.command_q.put(send_dict)


