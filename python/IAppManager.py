import Queue
from Util import logger
import Task

appmgr_log = logger.getLogger('AppMgr')


class IAppManager:
    def __init__(self, apps):
        self.applist = {}  # A list of applications  id:app
        self.app_status = {}  # appid: true/false  if app finalized
        #self.task_list = {}  # tid: task
        self.app_task_list = {}  # appid: {tid:taskobj}
        self.tid = 0
        self.runflag = False
        index = 0
        for app in apps:
            if not app.checkApp():
                appmgr_log.warning('[AppMgr] APP %s is incompatible, skip' % app.name)
                continue
            self.applist[index] = app
            self.app_status[index] = False
            self.app_task_list[index]=[]
            app.set_id(index)
            index += 1
            appmgr_log.info('[AppMgr] APP %s is loaded'%app.name)
        appmgr_log.debug('[AppMgr] Load apps, the number of app = %s' % self.applist)
        if len(self.applist) > 0:
            self.current_app = self.applist[0]
            self.current_app_id = self.current_app.get_id()
            self.runflag = self.gen_task_list()

    def create_task(self,appid):
        """
        According to split function of app, split data and create small tasks, store them into task_queue
        :param app:
        :return:
        """
        raise NotImplementedError

    def setup_app(self,appid=None):
        """
        Provide app setup boot to the scheduler
        :param appid:
        :return:
        """
        raise NotImplementedError

    def uninstall_app(self,appid=None):
        """
        Provide app uninstall boot to scheduler
        :param appid:
        :return:
        """
        raise NotImplementedError

    def get_current_appid(self):
        return self.current_app_id

    def get_current_app(self):
        return self.applist[self.current_app_id]

    def finalize_app(self, app=None):
        """
        The application operations when all tasks are finished
        :return:
        """
        self.app_status[app.id] = True

    def has_next_app(self):
        return self.current_app_id != len(self.applist) - 1

    def next_app(self):
        """
        Start the next application
        :return: application
        """
        raise NotImplementedError

    def get_app_task_list(self, app=None):
        if not app:
            app = self.current_app
        if app.id not in self.app_task_list.keys():
            self.gen_task_list(app)
        return self.app_task_list[app.id]

    def get_task(self, tid, appid=None):
        if not appid:
            appid= self.current_app_id
        return self.app_task_list[appid][tid]

    def gen_task_list(self, appid=None):
        if not appid:
            appid = self.current_app_id
        if len(self.app_task_list[appid]) != 0:
            appmgr_log.warning('there are tasks in list before generate tasks, skip')
        tmp_tasklist = self.create_task(appid)
        if tmp_tasklist:
            self.app_task_list[appid] = tmp_tasklist
            appmgr_log.info('[AppMgr] App %d, Create %d tasks:%s' % (appid, len(self.app_task_list[appid]),self.app_task_list[appid]))
            return True
        else:
            return False
'''
    def get_app_init(self, appid):
        if not appid in self.applist.keys():
            appmgr_log.error('@get app initialize boot error, can not find appid=%d' % appid)
            return None
        app = self.applist[appid]
        return dict({'boot': app.app_init_boot, 'resdir': app.res_dir}, **app.app_init_extra)

    def get_app_fin(self, appid):
        if not appid in self.applist.keys():
            appmgr_log.error('@get app initialize boot error, can not find appid=%d' % appid)
            return None
        app = self.applist[appid]
        return dict({'boot': app.app_fin_boot, 'resdir': app.res_dir}, **app.app_fin_extra)
'''




class SimpleAppManager(IAppManager):
    def create_task(self, appid):
        app = self.applist[appid]
        task_list = app.split()
        data = app.split()
        app.log.debug('after split')
        self.tid = 0
        task_dict = {}
        for task in task_list:
            task.tid = self.tid
            task_dict[self.tid] = task
            self.tid+=1
        '''
        for k, v in data.items():
            for data_value in v:
            # create tasks, and store in task_queue
                task = Task.Task(self.tid)
                self.tid += 1
                task.initial(app.app_boot[k], None if not app.args.has_key(k) else app.args[k], data_value, app.res_dir)
            # self.task_queue.put(task)
                task_list[task.tid] = task
                appmgr_log.debug('[AppMgr] Create Task: %s'%task.toDict())
        '''
        if len(task_list) == 0 and len(data) == 0:
            appmgr_log.error('[AppMgr]: Create 0 task, check app split() method')
            return None
        else:
            appmgr_log.info('[AppMgr]: Create %d task'%(len(task_dict)))
            return task_dict

    def setup_app(self,appid=None):
        if not appid:
            appid=self.current_app_id
        initask = Task.Task(-1)
        initask.boot = self.applist[appid].setup()
        appmgr_log.debug('[AppMgr] Application:%s create setup command: %s'%(self.applist[appid].name,initask.boot))
        return initask
    def uninstall_app(self,appid=None):
        if not appid:
            appid=self.current_app_id
        fin_task = Task.Task(-1)
        fin_task.boot = self.applist[appid].uninstall()
        appmgr_log.debug('[AppMgr] Application:%s create uninstall command: %s'%(self.applist[appid].name,fin_task.boot))
        return fin_task

    def finalize_app(self, app=None):
        if not app:
            app = self.applist[self.current_app_id]
        if not self.app_status[app.id]:
            appmgr_log.info('[AppMgr] App %s finalizing' % app.name)
            appmgr_log.debug('[AppMgr] AppMgr merge tasks= %s' % self.get_app_task_list(app))
            app.merge(self.get_app_task_list(app))
            IAppManager.finalize_app(self, app)

    def next_app(self):
        if self.current_app_id != len(self.applist) - 1:
            self.current_app_id += 1
            self.current_app = self.applist[self.current_app_id]
            self.runflag = self.gen_task_list()

            return True
        else:
            return None
