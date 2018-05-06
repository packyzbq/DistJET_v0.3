import Queue
import time
import os
import traceback
import threading

from Util import logger

control_log = logger.getLogger('WatchDog_Log')
handler_log = logger.getLogger('Handler')
master_log = logger.getLogger('Master')
health_log = logger.getLogger('Health')

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
from python.Util.Recoder import BaseElement,BaseRecoder
from python.WorkerRegistry import WorkerStatus



class WatchDogThread(BaseThread):
    """
    monitor the worker registry to manage worker
    """

    def __init__(self, master):
        BaseThread.__init__(self, 'WatchDog')
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
                    #TODO: maybe use other method to deal with the lost worker
                    #self.master.remove_worker(wid)
                    self.master.worker_registry.setStatus(wid,WorkerStatus.LOST)
                    self.master.lost_worker(wid)

            # check if all worker idle
            if self.master.worker_registry.checkIdle():
                # finalize all worker
                self.master.finalize_worker()

            # check idle timeout worker
            idleworker = self.master.worker_registry.checkIDLETimeout()
            if idleworker:
                control_log.warning('Find Idle timeout worker %s'%idleworker)
                # TODO: do something to reduce the resource

            #check error status worker
            errworker = self.master.worker_registry.checkError()
            if errworker:
                control_log.warning('Find error status worker %s'%errworker) 
            #for wid in errworker:
                #self.master.remove_worker(wid)

            # print worker status
            master_log.info('[Master] Worker status = %s'%self.master.worker_registry.get_worker_status())

            # save worker status to file
            rundir = self.master.cfg.getCFGattr('rundir')
            with open(os.environ['HOME']+'/.DistJET/worker','w+') as workerfile:
                workerfile.truncate()
                workerfile.write('wid\tstatus\trunning\tlasttime\n')
                for wid in self.master.worker_registry:
                    entry = self.master.worker_registry.get_entry(wid)
                    if entry is None:
                        continue
                    w_d = entry.toDict()
                    workerfile.write(str(w_d['wid'])+'\t'+str(WorkerStatus.desc(w_d['status']))+'\t'+str(w_d['running_task'])+'\t'+str(w_d['last_connect']+'\n'))
                workerfile.flush()


            if not lostworker and not idleworker:
                control_log.debug('No lost worker and idle worker')
            if self.master.cfg.getPolicyattr('control_delay'):
                time.sleep(self.master.cfg.getPolicyattr('control_delay'))
            else:
                time.sleep(0.1)

    def activateProcessing(self):
        self.processing = True


class HandlerThread(BaseThread):
    def __init__(self, master,cond):
        BaseThread.__init__(self,'Handler')
        self.master = master
        self.message_queue = Queue.Queue()
        self.uuid_queue = Queue.Queue()
        self.cond = cond
        self.sleep_flag = False

    def setMessage(self,message,uuid):
        self.message_queue.put(message)
        self.uuid_queue.put(uuid)

    def isSleep(self):
        return self.sleep_flag

    def run(self):
        handler_log.info('Handler thread start...')
        while not self.get_stop_flag():
            self.sleep_flag = False
            if self.message_queue.empty():
                self.sleep_flag = True
                self.cond.acquire()
                self.cond.wait()
                self.cond.release()
            else:
                msg = self.message_queue.get()
                recv_dict = Pack.unpack_obj(Pack.unpack_from_json(msg.sbuf[0:msg.size])['dict'])
                current_uuid = self.uuid_queue.get()
                
                recode_ele = None
                if recv_dict.has_key('wid'):
                    recode_ele = BaseElement(recv_dict['wid'])

                if recv_dict.has_key('flag'):
                    if recv_dict['flag'] == 'FP' and msg.tag == MPI_Wrapper.Tags.MPI_REGISTY:
                        # register worker
                        # {uuid:str, MPI_REGISTY:{ capacity: int}, ctime:int, flag:"FP"}
                        master_log.info('[Master] Receive REGISTY msg = %s' % recv_dict)
                        self.master.register_worker(recv_dict['uuid'], recv_dict[MPI_Wrapper.Tags.MPI_REGISTY]['capacity'])
                        self.master.setStart()
                    elif recv_dict['flag'] == 'LP':
                        # last ping from worker, sync completed task, report node's health, logout and disconnect worker
                        master_log.info(
                            '[Master] From worker %s Receive DISCONNECT msg = %s' % (recv_dict['wid'], recv_dict))
                        for task in recv_dict['Task']:
                            if task.status == TaskStatus.COMPLETED:
                                self.master.task_scheduler.task_completed(recv_dict['wid'], task)
                            elif task.status == TaskStatus.FAILED:
                                self.master.task_scheduler.task_failed(recv_dict['wid'], task)
                        self.master.remove_worker(recv_dict['wid'])
                        # continue
                # Normal ping msg
                if recv_dict.has_key('Task'):
                    if recv_dict['Task']:
                        master_log.debug('[Master] Receive finished task %s, status = %s'%([task.tid for task in recv_dict['Task']],[task.status for task in recv_dict['Task']]))
                    for task in recv_dict['Task']:
                        if task.status == TaskStatus.COMPLETED:
                            self.master.task_scheduler.task_completed(recv_dict['wid'], task)
                        elif task.status == TaskStatus.FAILED:
                            self.master.task_scheduler.task_failed(recv_dict['wid'], task)
                        else:
                            master_log.warning('[Master] Can not recognize the task status %s of Task' % task.status)
                        self.master.worker_registry.task_complete(recv_dict['wid'])
                if recv_dict.has_key('health') and recv_dict['health'] and recode_ele:
                    # plan 1
                    #health_log.info('Worker %s : %s'%(recv_dict['wid'], recv_dict['health']))
                    # plan 2
                    tmpdict = recv_dict['health']
                    recode_ele.cpuid = tmpdict['CpuId']
                    recode_ele.cpurate = tmpdict['CpuUsage']
                    recode_ele.mem = tmpdict['MemoUsage']['MemUsage']
                    recode_ele.extra=tmpdict['extra']
                if recv_dict.has_key('ctime'):
                    replay = 0
                    if (not recv_dict.has_key('flag')): #or (recv_dict.has_key('flag') and recv_dict['flag'] != 'LP'):
                        #self.master.worker_registry.setContacttime(recv_dict['uuid'], recv_dict['ctime'])
                        if self.master.worker_registry.checkLostWorker(wid=recv_dict['wid']):
                            self.master.worker_registry.setAlive(recv_dict['uuid'])
                        self.master.worker_registry.setContacttime(recv_dict['uuid'],time.time())
                    if recode_ele:
                        replay=time.time()
                        recode_ele.delay=replay-float(recv_dict['ctime'])
                        if recode_ele.delay < 0:
                            master_log.error('[Master] error response time, wid=%d ,start=%f, response=%f'%(recv_dict['wid'],float(recv_dict['ctime']),replay))
                    if self.master.cfg.getCFGattr('delay_rec')=="True":
                        print("DELAY_REC = %s"%self.master.cfg.getCFGattr('delay_rec'))
                        #for testing, reply a ack msg
                        send_dict = {'ctime':recv_dict['ctime'],'response':replay}
                        send_str = Pack.pack2json({'uuid':current_uuid,'dict':Pack.pack_obj({MPI_Wrapper.Tags.EXTRA:send_dict})})
                        #if self.master.worker_registry.isAlive(recv_dict['wid']):
                        if not recv_dict.has_key('flag'):
                    	    self.master.server.send_string(send_str, len(send_str), current_uuid, int(MPI_Wrapper.Tags.EXTRA))
                if recv_dict.has_key('wstatus'):
                    self.master.worker_registry.setStatus(recv_dict['wid'], recv_dict['wstatus'])
                    master_log.debug('[Master] Set worker %s status = %s' % (recv_dict['wid'], WorkerStatus.desc(recv_dict['wstatus'])))
                if recv_dict.has_key('rTask') and recode_ele:
                    recode_ele.running_task = recv_dict['rTask']

                if recv_dict.has_key(MPI_Wrapper.Tags.APP_INI):
                    # result of slave setup
                    # wid:int, uuid:str, APP_INI:{recode:int, errmsg:str}
                    v = recv_dict[MPI_Wrapper.Tags.APP_INI]
                    master_log.info('[Master] From worker %d receive a App_INI msg = %s' % (recv_dict['wid'], v))
                    if v['recode'] == status.SUCCESS:
                        master_log.info('worker %d initialize successfully' % recv_dict['wid'])
                    else:
                        # initial worker failed
                        master_log.error('worker %d initialize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                        '''
                        if self.master.worker_registry.worker_reinit(recv_dict['wid']):
                            init_comm = self.master.task_scheduler.setup_worker()
                            send_dict = {'wid': recv_dict['wid'],
                                         'appid': self.master.task_scheduler.appid,
                                         'init': init_comm,
                                         'uuid': recv_dict['uuid']}
                            self.master.command_q.put({MPI_Wrapper.Tags.MPI_REGISTY_ACK: send_dict,'uuid':current_uuid})
                        '''
                        #else:
                        # terminate worker
                        master_log.info('[Master] Send Worker stop to worker %d'%recv_dict['wid'])
                        self.master.command_q.put({MPI_Wrapper.Tags.WORKER_STOP: '','uuid':current_uuid})
                if recv_dict.has_key(MPI_Wrapper.Tags.TASK_ADD):
                    v = recv_dict[MPI_Wrapper.Tags.TASK_ADD]
                    master_log.debug('[Master] From worker %s receive a TASK_ADD msg = %s' % (recv_dict['wid'], v))
                    if self.master.task_scheduler.has_more_work():
                        task_list = self.master.task_scheduler.assignTask(recv_dict['wid'])
                        if not task_list:
                            master_log.info('[Master] There are pre Chain Task need to be done, worker wait')
                        else:
                            # assign tasks to worker
                            tid_list = [task.tid for task in task_list]
                            master_log.info(
                                '[Master] Assign task %s to worker %d' % (tid_list, recv_dict['wid']))
                            self.master.worker_registry.setStatus(recv_dict['wid'], WorkerStatus.SCHEDULED)
                            self.master.worker_registry.setTask(recv_dict['wid'], tid_list[0])
                            self.master.command_q.put({MPI_Wrapper.Tags.TASK_ADD: task_list, 'uuid': current_uuid})
                    elif not self.master.task_scheduler.is_all_task_scheduled():
                        master_log.info('[Master] Has Chain task need to do, worker Halt')
                        self.master.command_q.put({MPI_Wrapper.Tags.WORKER_HALT: '', 'uuid': current_uuid})
                    else:
                        master_log.info('[Master] No more task to do')
                        if self.master.get_all_final():
                            st = self.master.worker_registry.get_worker_status(recv_dict['wid'])
                            if st == WorkerStatus.FINALIZED:
                                master_log('[Master] ReSend LOGOUT msg to worker %s'%str(recv_dict['wid']))
                                self.master.command_q.put({MPI_Wrapper.Tags.LOGOUT: '', 'uuid': recv_dict['uuid']})
                        # according to Policy ,check other worker status and idle worker
                        elif Config.getPolicyattr('WORKER_SYNC_QUIT'):
                            if self.master.worker_registry.checkIdle(exp=[recv_dict['wid']]):  # exp=[recv_dict['wid']]
                                if self.master.get_all_final():
                                    master_log.info('[Master] Have send all finalize msg, skip this')
                                else:
                                    # finalize all worker
                                    master_log.info('[Master] All worker have done, finalize all worker')
                                    fin_task = self.master.task_scheduler.uninstall_worker()
                                    self.master.command_q.put({MPI_Wrapper.Tags.APP_FIN: fin_task, 'extra': [],'uuid':current_uuid})
                                    self.master.set_all_final()
                            else:
                                master_log.info('[Master] There are still running worker, halt')
                                self.master.command_q.put({MPI_Wrapper.Tags.WORKER_HALT: '','uuid':current_uuid})
                        else:
                            master_log.info('[Master] Finalize worker %s' % recv_dict['wid'])
                            fin_task = self.master.task_scheduler.uninstall_worker()
                            self.master.command_q.put({MPI_Wrapper.Tags.APP_FIN: fin_task,'uuid':current_uuid})


                if recv_dict.has_key(MPI_Wrapper.Tags.APP_FIN):
                    v = recv_dict[MPI_Wrapper.Tags.APP_FIN]
                    master_log.info('[Master] From worker %s receive a APP_FIN msg = %s' % (recv_dict['wid'], v))
                    if v['recode'] == status.SUCCESS:
                        self.master.task_scheduler.worker_finalized(recv_dict['wid'])
                        master_log.info('[Master] Have finalized worker %s' % recv_dict['wid'])
                        #check all worker finalized
                        if self.master.worker_registry.checkError(recv_dict['wid']):
                            self.master.command_q.put({MPI_Wrapper.Tags.LOGOUT:'','uuid':recv_dict['uuid']})
                        else:
                            if self.master.appmgr.has_next_app():
                                master_log.info('[Master] New application need to be done')
                                if self.master.worker_registry.checkFinalize():
                                # after all alive worker finalized, load new app
                                # if more app need to be done, wait for old app finish and load new app
                                    #self.master.command_q.put({MPI_Wrapper.Tags.WORKER_HALT:'','uuid':current_uuid})
                                    self.master.acquire_newApp()
                            else:
                                # no more app need to do, logout all worker
                                #for uuid in self.master.worker_registry.alive_workers:
                                #    self.master.command_q.put({MPI_Wrapper.Tags.LOGOUT: '','uuid':uuid})
                                self.master.command_q.put({MPI_Wrapper.Tags.LOGOUT: '','uuid':recv_dict['uuid']})

                    else:
                        master_log.error('worker %d finalize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                        if self.master.worker_registry.worker_refin(recv_dict['wid']):
                            self.master.command_q.put({MPI_Wrapper.Tags.APP_FIN: self.master.task_scheduler.uninstall_worker(),'uuid':current_uuid})
                        else:
                            # over the refinalize times, force to shutdown
                            self.master.task_scheduler.worker_finalized(recv_dict['wid'])
                            self.master.command_q.put({MPI_Wrapper.Tags.WORKER_STOP: '','uuid':current_uuid})

                if recode_ele and recode_ele.check_integrity() and self.master.recoder:
                    self.master.recoder.set_message(recode_ele.wid,recode_ele)
        handler_log.info('Handler thread stop...')



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

        self.control_thread = WatchDogThread(self)

        self.handler_cond = threading.Condition()
        self.handler = HandlerThread(self,self.handler_cond)

        self.applications = applications  # list

        self.command_q = Queue.Queue()

        self.__wid = 1
        self.__all_final_flag = False
        # TODO(optional) load customed AppManager
        self.appmgr = SimpleAppManager(apps=self.applications)
        #master_log.debug('[Master] Appmgr has instanced')

        # load app
        self.load_app()

        self.server = MPI_Wrapper.Server(self.recv_buffer, self.svc_name)
        self.server.set_portfile(self.cfg.getCFGattr('rundir')+"/port.txt")
        ret = self.server.initialize()
        if ret != 0:
            master_log.error('[Master] Server initialize error, stop. errcode = %d' % ret)
            # TODO add error handler
            exit()
        self.recoder = None
        if self.cfg.getCFGattr('pmonitor') == 'True':
            self.recoder = BaseRecoder(self.cfg.getCFGattr('rundir'))

        self.__newApp_flag = False
        self.__stop = False
        self.__start=False
	
    def get_all_final(self):
        return self.__all_final_flag

    def set_all_final(self):
        self.__all_final_flag = True

    def setStart(self):
        self.__start=True

    def acquire_newApp(self):
        self.__newApp_flag = True

    def stop(self):
        # Recoder finalize
        if self.recoder:
            self.recoder.finalize()
        master_log.info('[Master] Recoder has finalized')
        # TaskScheduler is not a thread
        if self.control_thread and self.control_thread.isAlive():
            self.control_thread.stop()
            self.control_thread.join()
            master_log.info('[Master] Control Thread has joined')
        if self.handler and self.handler.isAlive():
            self.handler.stop()
            if self.handler.isSleep():
                self.handler_cond.acquire()
                self.handler_cond.notify()
                self.handler_cond.release()
            self.handler.join()
            master_log.info('[Master] Handler Thread has joined')

        ret = self.server.stop()
        if ret != 0:
            master_log.error('[Master] Server stop error, errcode = %d' % ret)
            # TODO add solution
        else:
            master_log.info('[Master] Server stop')
        self.__stop = True

    def register_worker(self, w_uuid, capacity=1):
        master_log.info('[Master] register worker %s' % w_uuid)
        worker = self.worker_registry.add_worker(w_uuid, capacity)
        if not worker:
            master_log.warning('[Master] The uuid=%s of worker has already registered', w_uuid)
        else:
            worker_module_path = self.appmgr.current_app.specifiedWorker
            send_dict = {'wid': worker.wid, 'appid': self.task_scheduler.appid,
                         'init': self.task_scheduler.setup_worker(), 'uuid': w_uuid, 'wmp': worker_module_path}
            self.command_q.put({MPI_Wrapper.Tags.MPI_REGISTY_ACK: send_dict,'uuid':w_uuid})
            # send_str = MSG_wrapper(wid=worker.wid,appid=self.task_scheduler.appid, init=self.task_scheduler.init_worker())
            # self.server.send_string(send_str, len(send_str), w_uuid, MPI_Wrapper.Tags.MPI_REGISTY_ACK)

    def remove_worker(self, wid):
        flag, ret = self.worker_registry.remove_worker(wid)
        if flag:
            self.task_scheduler.worker_removed(wid, time.time())
        else:
            master_log.error("[Master] remove worker error, %s"%ret)
            # TODO MPI Layer handle this problem
            pass

    def anaylize_health(self, info):
        # TODO give a threshold of the health of a node
        pass

    def startProcessing(self):
        #load app
        #self.load_app()
        self.control_thread.start()
        self.handler.start()
        try:
            while not self.__stop:
                current_uuid=None
                msg = self.recv_buffer.get()
                if msg.tag!= -1:
                    #master_log.debug('[Master] Receive msg = %s' % msg.sbuf[:msg.size])
                    if msg.tag == MPI_Wrapper.Tags.MPI_DISCONNECT:
                        master_log.info("[Master] Agent disconnect")
                        continue
                    try:
                        message = msg.sbuf[0:msg.size]
                        recv_dict = Pack.unpack_obj(Pack.unpack_from_json(message)['dict'])
                    except:
                        master_log.error("[Master] Parse msg error, errmsg=%s, "%traceback.format_exc())
                    current_uuid = recv_dict['uuid']
                    if recv_dict.has_key('wid'):
                        master_log.debug('[Master] Receive message from %s'%str(recv_dict['wid']))
                    else:
                        master_log.debug('[Master] Receive message from %s'%current_uuid)
                    # pass message to handler thread
                    self.handler.setMessage(msg,current_uuid)
                    if self.handler.isSleep():
                        self.handler_cond.acquire()
                        self.handler_cond.notify()
                        self.handler_cond.release()


                while not self.command_q.empty():
                    send_dict = self.command_q.get()
                    if len(send_dict) != 0:
                        tag = None
                        for k in send_dict.keys():
                            if k not in ['uuid','extra']:
                                tag = k
                        #tag = send_dict.keys()[0]
                        #print("%s, tag=%s"%(send_dict.keys(),tag))
                        if not send_dict.has_key('uuid'):
                            master_log.error('Send message error, uuid not found')
                            continue
                        current_uuid = send_dict['uuid']
                        del(send_dict['uuid'])

                        if send_dict.has_key('extra') and (not send_dict['extra']):
                            del (send_dict['extra'])
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send msg = %s, tag=%s, uuid=%s' % (
                            send_dict, tag, self.worker_registry.alive_workers))
                            for uuid in self.worker_registry.alive_workers:
                                send_final=Pack.pack2json({'uuid':uuid,'dict':send_str})
                                self.server.send_string(send_final, len(send_final), str(uuid), tag)
                                master_log.debug('[Master] Send completed')
                        elif send_dict.has_key('extra') and send_dict['extra']:
                            tmplist = send_dict['extra']
                            del (send_dict['extra'])
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send msg = %s' % send_dict)
                            for uuid in tmplist:
                                send_final=Pack.pack2json({'uuid':uuid,'dict':send_str})
                                self.server.send_string(send_final, len(send_final), uuid, tag)
                                master_log.debug('[Master] Send completed')
                        else:
                            send_str = Pack.pack_obj(send_dict)
                            master_log.debug('[Master] Send to worker %s msg = %s, tag=%s' % (current_uuid, send_dict,tag))
                            send_final = Pack.pack2json({'uuid':current_uuid,'dict':send_str})
                            self.server.send_string(send_final, len(send_final), current_uuid, int(tag))
                            master_log.debug('[Master] Send completed')
                # master stop condition

                # time.sleep(1)
                if not self.task_scheduler.has_more_work() and not self.task_scheduler.has_scheduled_work():
                    #TODO: app finalize/merge need to be a single module
                    #check all worker stop working
                    if not self.worker_registry.checkRunning():
                        #master_log.info('[Scheduler] Complete tasks number: %s; All task number: %s' % (self.task_scheduler.completed_queue.qsize(), len(self.task_scheduler.task_list)))
                        self.appmgr.finalize_app()
                        #check all worker finalized
                        if self.__newApp_flag and self.worker_registry.checkFinalize():
                            # has more app need to be done
                            if self.appmgr.has_next_app():
                                self.load_app(napp=True)
                                init_comm = self.task_scheduler.setup_worker()
                                worker_path = self.appmgr.current_app.specifiedWorker
                                send_dict = {'appid': self.task_scheduler.appid,
                                             'init': init_comm,
                                             'flag':'NEWAPP',
                                             'wmp':worker_path}
                                for uuid in self.worker_registry.alive_workers:
                                    send_dict['uuid'] = uuid
                                    send_dict['wid'] = self.worker_registry.get_by_uuid(uuid).wid
                                    send_str = Pack.pack_obj({MPI_Wrapper.Tags.MPI_REGISTY_ACK:send_dict})
                                    send_final = Pack.pack2json({'uuid':uuid,'dict':send_str})
                                    master_log.info('[Master] Send new App message')
                                    self.server.send_string(send_final,len(send_final),uuid,MPI_Wrapper.Tags.MPI_REGISTY_ACK)
                                self.__newApp_flag = False

                        elif not self.appmgr.has_next_app() and self.worker_registry.size() == 0:
                            master_log.info("[Master] Application done, logout workers")
                            if self.worker_registry.hasAlive():
                                #TODO logout/disconnect worker -- force stop?
                                stop_line = Pack.pack_obj({MPI_Wrapper.Tags.WORKER_STOP:''})
                                for uuid in self.worker_registry.alive_workers:
                                    self.server.send_string(stop_line,len(stop_line),uuid,MPI_Wrapper.Tags.WORKER_STOP)
                                continue
                            self.stop()
                # All worker disconnected
                if (self.__start or self.get_all_final()) and self.worker_registry.size() == 0:
                    #print "--- Master stop"
                    self.stop()


        except KeyboardInterrupt, Exception:
            self.stop()

    def load_app(self,napp=False):
        if not self.appmgr.runflag:
            # appmgr load task error, exit
            return False
        # self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        if napp:
            self.appmgr.next_app()
        if self.appmgr.get_current_app():
            self.task_scheduler = self.appmgr.get_current_app().scheduler(self, self.appmgr, self.worker_registry)
        else:
            self.task_scheduler = IScheduler.SimpleScheduler(self, self.appmgr, self.worker_registry)
        self.task_scheduler.appid = self.appmgr.get_current_appid()

    def lost_worker(self,wid):
        master_log.info("[Master] Worker %s lost, removing..."%wid)
        self.remove_worker(wid)

    def getRunFlag(self):
        return self.appmgr.runflag

    def finalize_worker(self, uuid=None):
        fin_task = self.task_scheduler.uninstall_worker()
        if not uuid:
            master_log.info('[Master] Finalize all worker')
            self.command_q.put({MPI_Wrapper.Tags.APP_FIN: fin_task, 'extra': [], 'uuid': "111"})
            self.set_all_final()
        else:
            master_log.info('[Master] Finalize worker %s' % self.worker_registry.get_by_uuid(uuid).wid)
            fin_task = self.task_scheduler.uninstall_worker()
            self.command_q.put({MPI_Wrapper.Tags.APP_FIN: fin_task, 'uuid': uuid})

    def try_pullback(self,wid, tid):
        master_log.debug('[Master] Try pull back task %s from worker %s'%(tid,wid))
        uuid = self.worker_registry.get_entry(wid).w_uuid
        send_dict = {MPI_Wrapper.Tags.TASK_REMOVE: tid, 'extra': [uuid],'uuid':uuid}
        self.command_q.put(send_dict)


