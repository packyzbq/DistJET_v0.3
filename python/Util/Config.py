import types
import threading
import os
import ConfigParser

# os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
GlobalLock = threading.RLock()
_inipath = None


def set_inipath(inipath):
    try:
        GlobalLock.acquire()
        global _inipath
        _inipath = inipath
    finally:
        GlobalLock.release()


class Config(object):
    __global_config = {
        'svc_name':'Default',
        'log_level': 'debug',
        'health_detect_scripts': None,
        'topdir': os.environ['DistJETPATH'],
        'rundir': None,
        'logconsole': False,
        'heartbeatinterval': 0.5,
        'halt_recv_interval': 10,
		'delay_rec': False,
		'pmonitor': False
    }

    __policy = {
        'lost_worker_timeout': 60,
        'idle_worker_timeout': 0,
        'control_delay': 1,
        'task_attempt_time': 1,
        'initial_try_time': 3,
        'fin_try_time': 3,
        'worker_sync_quit': False,
        'ignore_task_fail': True
    }
    __sysconfig = os.environ['HOME']+'/.DistJET/config.ini'

    def __new__(cls, *args, **kwargs):
        global _inipath
        #check if have config file
        # check if exists tmp dir, $HOME/.diane_tmp/
        tmp_dir = os.environ['HOME'] + '/.DistJET'
        if not os.path.exists(tmp_dir+'/config.ini'):
            if not _inipath:
                set_inipath(os.environ['DistJETPATH']+'/config/config.ini')
            elif _inipath and not os.path.exists(_inipath):
                if os.path.exists(os.environ['DistJETPATH'] + '/' + _inipath):
                    set_inipath(os.environ['DistJETPATH'] + '/' + _inipath)
                else:
                    set_inipath(os.environ['DistJETPATH']+'/config/config.ini')
            try:
                GlobalLock.acquire()
                cf = ConfigParser.ConfigParser()
                cf.read(_inipath)
                if cf.has_section('GlobalCfg'):
                    for key in cf.options('GlobalCfg'):
                        cls.__global_config[key] = cf.get('GlobalCfg', key)
                if cf.has_section('Policy'):
                    for key in cf.options('Policy'):
                        try:
                            cls.__policy[key] = cf.getint('Policy', key)
                        except:
                            cls.__policy[key] = cf.getboolean('Policy',key)
                cls.__loaded = True
            finally:
                    GlobalLock.release()
            #if cls.__global_config['rundir'] is None:
            #    cls.__global_config['rundir'] = os.getcwd()+'/rundir'
            assert cls.__global_config.has_key('rundir')
            assert cls.__global_config['rundir'] is not None
            if not os.path.exists(cls.__global_config['rundir']):
                os.mkdir(cls.__global_config['rundir'])
            rundir = cls.__global_config['rundir']

            #create tmp config
            tmpconfig=open(tmp_dir+'/config.ini','w+')
            tmpconfig.write('[GlobalCfg]\n')
            for k,v in cls.__global_config.items():
                tmpconfig.write("%s=%s\n"%(k,v))
            tmpconfig.write('[Policy]\n')
            for k,v in cls.__policy.items():
                tmpconfig.write("%s=%s\n"%(k,v))
            tmpconfig.flush()
            tmpconfig.close()
        else:
            try:
                GlobalLock.acquire()
                print 'load config file'
                cf = ConfigParser.ConfigParser()
                cf.read(cls.__sysconfig)
                if cf.has_section('GlobalCfg'):
                    for key in cf.options('GlobalCfg'):
                        cls.__global_config[key] = cf.get('GlobalCfg', key)
              
                if cf.has_section('Policy'):
                    for key in cf.options('Policy'):
                        try:
                            cls.__policy[key] = cf.getint('Policy', key)
                        except:
                            cls.__policy[key] = cf.getboolean('Policy',key)
            finally:
                    GlobalLock.release()
        return object.__new__(cls)

    @classmethod
    def getCFGattr(cls, key):
        #print 'acquire key %s, keylist = %s'%(key,cls.__global_config.items())
        try:
            GlobalLock.acquire()
            if key in cls.__global_config.keys():
                return cls.__global_config[key]
            else:
                return None
        finally:
            GlobalLock.release()

    @classmethod
    def getPolicyattr(cls, key):
        try:
            GlobalLock.acquire()
            if key in cls.__policy.keys():
                return cls.__policy[key]
            else:
                return None
        finally:
            GlobalLock.release()

    @classmethod
    def getAppcfgfile(cls, name):
        try:
            GlobalLock.acquire()
            if cls.__appcfg.has_key(name):
                return cls.__appcfg[name]
            else:
                return None
        finally:
            GlobalLock.release()

    @classmethod
    def setCfg(cls, key, val):
        try:
            GlobalLock.acquire()
            cls.__global_config[key] = val
        finally:
            GlobalLock.release()

    @classmethod
    def setPolicy(cls, key, val):
        try:
            GlobalLock.acquire()
            cls.__policy[key] = val
        finally:
            GlobalLock.release()



class AppConf:
    __cfg = {
        'appname': None,
        'workdir': None,
        'topdir': os.environ['DistJETPATH']
    }

    def __init__(self, ini_path=None, app_name=None):
        self.config = dict([(k, v) for (k, v) in AppConf.__cfg.items()])
        self.other_cfg = {}  # other section: options
        self.app_name = app_name
        if not self.app_name:
            self.app_name = 'AppConfig'
        self.lock = threading.RLock()
        if ini_path:
            if os.path.exists(ini_path):
                pass
            elif os.path.exists(os.environ['DistJETPATH'] + '/' + ini_path):
                ini_path = os.environ['DistJETPATH'] + '/' + ini_path
            else:
                return
            try:
                self.lock.acquire()
                cf = ConfigParser.ConfigParser()
                cf.read(ini_path)
                if cf.has_section(self.app_name):
                    main_sec = self.app_name
                else:
                    main_sec = cf.sections()[0]
                for key in cf.options(main_sec):
                    self.config[key] = cf.get(main_sec, key)
                for sec in cf.sections()[1:]:
                    self.other_cfg[sec] = {}
                    for opt in cf.options(sec):
                        self.other_cfg[sec][opt] = cf.get(sec, opt)

            finally:
                self.lock.release()

    # def getattr(self,item):
    #    assert type(item) == types.StringType, "ERROR: attribute must be of String type!"
    ##    if self.config.has_key(item):
    #        return self.config[item]
    #    else:
    #        return None

    def get(self, item, sec=None):
        if type(item) != types.StringType or (sec is not None and type(sec) != types.StringType):
            print('@Config:[WARN] item or sec cannot be recognized')
            return None

        if sec is not None:
            if self.other_cfg.has_key(sec) and self.other_cfg[sec].has_key(item):
                return self.other_cfg[sec][item]
        elif self.config.has_key(item):
            return self.config[item]
        else:
            return None

    def getSections(self):
        if self.other_cfg:
            return self.other_cfg.keys()
        else:
            return None

    def setOptions(self, item, value, section=None):
        '''
        :param item: key
        :param value: value
        :param section: other sub config
        :return: bool,errmsg
        '''
        warnmsg = None
        if type(item) != types.StringType or (section is not None and type(section) != types.StringType):
            print('@Config:[WARN] item or sec cannot be recognized')
            return False, 'Input type of key/sample error'
        if section is not None and self.other_cfg.has_key(section):
            if self.other_cfg[section].has_key(item):
                warnmsg = '@Config:[WARN] Detect <%s,%s> in cfg, overwrite it' % (
                item, self.other_cfg[section].get(item))
            self.other_cfg[section][item] = value
            return True, warnmsg
        elif section is None:
            if self.config.has_key(item):
                warnmsg = '@Config:[WARN] Detect <%s,%s> in cfg, overwrite it' % (item, self.config.get(item))
            self.config[item] = value
            return True, warnmsg
        else:
            print '@Config:[ERROR] appcfg has no key:%s' % item
            return False, 'appcfg can not find key %s' % item


            # def __setattr__(self,key, value):
            #    assert type(key) == types.StringType, "ERROR: attribute must be of String type!"
            #    self.config[key] = value


if __name__ == '__main__':
    set_inipath(os.environ['DistJETPATH']+'/config/config.ini')
    Config()
