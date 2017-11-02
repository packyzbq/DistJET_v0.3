import Queue
import datetime
import time
import os
import traceback

from Util import logger

control_log = logger.getLogger('Control_Log')
master_log = logger.getLogger('Master')

import IR_Buffer_Module as IM
import IScheduler
import WorkerRegistry
import python.Util.Package as Pack

WorkerRegistry.wRegistery_log = master_log
import MPI_Wrapper

MPI_Wrapper.MPI_log = master_log

from BaseThread import BaseThread
from IAppManager import SimpleAppManager
from Task import TaskStatus
from python.Process.Process import status
from python.Util.Config import Config, set_inipath



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
        #master_log.debug('[Master] Appmgr has instanced')
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
            exit()

        self.__stop = False

    def stop(self):
        # TaskScheduler is not a thread
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
                         'init': self.task_scheduler.setup_worker(), 'uuid': w_uuid, 'wmp': worker_module_path}
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
        try:
            while not self.__stop:
                current_uuid=None
                msg = self.recv_buffer.get()
                if msg.tag!= -1:
                    master_log.debug('[Master] Receive msg = %s' % msg.sbuf[:msg.size])
                    if msg.tag == MPI_Wrapper.Tags.MPI_DISCONNECT:
                        master_log.info("[Master] Agent disconnect")
                        continue
                    try:
                        message = msg.sbuf[0:msg.size]
                        recv_dict = Pack.unpack_obj(Pack.unpack_from_json(message)['dict'])
                    except:
                        master_log.error("[Master] Parse msg error, errmsg=%s, "%traceback.format_exc())
                    current_uuid = recv_dict['uuid']
                    master_log.debug('[Master] Receive message from %s'%current_uuid)
                    if recv_dict.has_key('flag'):
                        if recv_dict['flag'] == 'FP' and msg.tag == MPI_Wrapper.Tags.MPI_REGISTY:
                            # register worker
                            # {uuid:str, MPI_REGISTY:{ capacity: int}, ctime:int, flag:"FP"}
                            master_log.info('[Master] Receive REGISTY msg = %s' % recv_dict)
                            self.register_worker(recv_dict['uuid'],recv_dict[MPI_Wrapper.Tags.MPI_REGISTY]['capacity'])
                        elif recv_dict['flag'] == 'LP':
                            # last ping from worker, sync completed task, report node's health, logout and disconnect worker
                            master_log.info('[Master] From worker %s Receive DISCONNECT msg = %s' % (recv_dict['wid'], recv_dict))
                            for task in recv_dict['Task']:
                                if task.status == TaskStatus.COMPLETED:
                                    self.task_scheduler.task_completed(recv_dict['wid'],task)
                                elif task.status == TaskStatus.FAILED:
                                    self.task_scheduler.task_failed(recv_dict['wid'],task)
                            self.remove_worker(recv_dict['wid'])
                        #continue
                    # Normal ping msg
                    if recv_dict.has_key('Task'):
                        for task in recv_dict['Task']:
                            if task.status == TaskStatus.COMPLETED:
                                self.task_scheduler.task_completed(recv_dict['wid'],task)
                            elif task.status == TaskStatus.FAILED:
                                self.task_scheduler.task_failed(recv_dict['wid'],task)
                            else:
                                master_log.warning('[Master] Can not recognize the task status %s of Task'%task.status)
                    if recv_dict.has_key('Health'):
                        #TODO add monitor
                        pass
                    if recv_dict.has_key('ctime'):
                        self.worker_registry.setContacttime(recv_dict['uuid'],recv_dict['ctime'])
                    if recv_dict.has_key('wstatus'):
                        self.worker_registry.setStatus(recv_dict['wid'],recv_dict['status'])
                        master_log.debug('[Master] Set worker %s status = %s' % (recv_dict['wid'], recv_dict['wstatus']))
                    if recv_dict.has_key(MPI_Wrapper.Tags.APP_INI):
                        # result of slave setup
                        # wid:int, uuid:str, APP_INI:{recode:int, errmsg:str}
                        v = recv_dict[MPI_Wrapper.Tags.APP_INI]
                        master_log.debug('[Master] From worker %d receive a App_INI msg = %s' % (recv_dict['wid'], v))
                        if v['recode'] == status.SUCCESS:
                            master_log.info('worker %d initialize successfully' % recv_dict['wid'])
                        else:
                            # initial worker failed
                            master_log.error('worker %d initialize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                            if self.worker_registry.worker_refin(recv_dict['wid']):
                                init_comm = self.task_scheduler.setup_worker()
                                send_dict = {'wid':recv_dict['wid'],
                                             'appid':self.task_scheduler.appid,
                                             'init':init_comm,
                                             'uuid':recv_dict['uuid']}
                                self.command_q.put({MPI_Wrapper.Tags.MPI_REGISTY_ACK: send_dict})
                            else:
                                # terminate worker
                                self.command_q.put({MPI_Wrapper.Tags.WORKER_STOP:''})
                    if recv_dict.has_key(MPI_Wrapper.Tags.TASK_ADD):
                        v = recv_dict[MPI_Wrapper.Tags.TASK_ADD]
                        master_log.debug('[Master] From worker %s receive a TASK_ADD msg = %s' % (recv_dict['wid'], v))
                        task_list = self.task_scheduler.assignTask(recv_dict['wid'])
                        if not task_list:
                            master_log.debug('[Master] No more task to do')
                            # according to Policy ,check other worker status and idle worker
                            if Config.getPolicyattr('WORKER_SYNC_QUIT'):
                                if self.worker_registry.checkIdle(exp=[recv_dict['wid']]):  # exp=[recv_dict['wid']]
                                    if self.__all_final_flag:
                                        master_log.info('[Master] Have send all finalize msg, skip this')
                                    else:
                                        # finalize all worker
                                        master_log.debug('[Master] All worker have done, finalize all worker')
                                        self.command_q.put({MPI_Wrapper.Tags.APP_FIN: '', 'extra': []})
                                else:
                                    master_log.debug('[Master] There are still running worker, halt')
                                    self.command_q.put({MPI_Wrapper.Tags.WORKER_HALT: ''})
                            else:
                                master_log.info('[Master] Finalize worker %s'%recv_dict['wid'])
                                comm_list = self.task_scheduler.uninstall_worker()
                                self.command_q.put({MPI_Wrapper.Tags.APP_FIN: comm_list})

                        else:
                            # assign tasks to worker
                            tid_list = [task.tid for task in task_list]
                            master_log.info(
                                '[Master] Assign task %s to worker %d' % (tid_list, recv_dict['wid']))
                            self.command_q.put({MPI_Wrapper.Tags.TASK_ADD: task_list})
                    if recv_dict.has_key(MPI_Wrapper.Tags.APP_FIN):
                        v = recv_dict[MPI_Wrapper.Tags.APP_FIN]
                        master_log.debug('[Master] From worker %s receive a APP_FIN msg = %s' % (recv_dict['wid'], v))
                        if v['recode'] == status.SUCCESS:
                            self.task_scheduler.worker_finalized(recv_dict['wid'])
                            master_log.debug('[Master] Have finalized worker %s'%recv_dict['wid'])
                            # TODO if more app need to be done
                            # no more app need to do, logout worker
                            self.command_q.put({MPI_Wrapper.Tags.LOGOUT:''})
                        else:
                            master_log.error('worker %d finalize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                            if self.worker_registry.worker_refin(recv_dict['wid']):
                                self.command_q.put({MPI_Wrapper.Tags.APP_FIN:self.task_scheduler.uninstall_worker()})
                            else:
                                # over the refinalize times, force to shutdown
                                self.command_q.put({MPI_Wrapper.Tags.WORKER_STOP:''})

                while not self.command_q.empty():
                    send_dict = self.command_q.get()
                    if len(send_dict) != 0:
                        tag = send_dict.keys()[0]
                        if send_dict.has_key('extra') and (not send_dict['extra']):
                            del (send_dict['extra'])
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send msg = %s, tag=%s, uuid=%s' % (
                            send_dict, tag, self.worker_registry.alive_workers))
                            for uuid in self.worker_registry.alive_workers:
                                send_str=Pack.pack2json({'uuid':uuid,'dict':send_str})
                                self.server.send_string(send_str, len(send_str), str(uuid), tag)
                        elif send_dict.has_key('extra') and send_dict['extra']:
                            tmplist = send_dict['extra']
                            del (send_dict['extra'])
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send msg = %s' % send_dict)
                            for uuid in tmplist:
                                send_str=Pack.pack2json({'uuid':uuid,'dict':send_str})
                                self.server.send_string(send_str, len(send_str), uuid, tag)
                        else:
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send to worker %s msg = %s' % (current_uuid, send_dict))
                            send_str = Pack.pack2json({'uuid':current_uuid,'dict':send_str})
                            self.server.send_string(send_str, len(send_str), current_uuid, tag)
                # master stop condition
                # time.sleep(1)
                if not self.task_scheduler.has_more_work() and not self.task_scheduler.has_scheduled_work():
                    self.appmgr.finalize_app()
                    if not self.appmgr.has_next_app() and self.worker_registry.size() == 0:
                        master_log.info("[Master] Application done, logout workers")
                        if self.worker_registry.hasAlive():
                            #TODO logout/disconnect worker -- force stop?
                            stop_line = Pack.pack_obj({MPI_Wrapper.Tags.WORKER_STOP:''})
                            for uuid in self.worker_registry.alive_workers:
                                self.server.send_string(stop_line,len(stop_line),uuid,MPI_Wrapper.Tags.WORKER_STOP)
                            continue
                        self.stop()
        except KeyboardInterrupt, Exception:
            self.stop()


    def getRunFlag(self):
        return self.appmgr.runflag

    def finalize_worker(self, uuid):
        tmp_dict = self.task_scheduler.fin_worker()
        self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict, 'extra': [uuid]})

    def try_pullback(self,wid, tid):
        master_log.debug('[Master] Try pull back task %s from worker %s'%(tid,wid))
        uuid = self.worker_registry.get_entry(wid).w_uuid
        send_dict = {MPI_Wrapper.Tags.TASK_REMOVE: tid, 'extra': [uuid]}
        self.command_q.put(send_dict)


