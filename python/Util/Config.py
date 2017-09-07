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
        'Log_Level': 'debug',
        'health_detect_scripts': None,
        'topDir': os.environ['DistJETPATH'],
        'Rundir': None,
        'LogConsole': True,
        'HeartBeatInterval': 0.5,
        'Halt_Recv_Interval': 10
    }

    __policy = {
        'LOST_WORKER_TIMEOUT': 60,
        'IDLE_WORKER_TIMEOUT': 0,
        'CONTROL_DELAY': 10,
        'TASK_ATTEMPT_TIME': 2,
        'INITIAL_TRY_TIME': 3,
        'FIN_TRY_TIME': 3,
        'WORKER_SYNC_QUIT': True
    }

    __loaded = False

    def __new__(cls, *args, **kwargs):
        global _inipath
        if not cls.__loaded:
            if _inipath:
                if os.path.exists(_inipath):
                    pass
                elif os.path.exists(os.environ['DistJETPATH'] + '/' + _inipath):
                    set_inipath(os.environ['DistJETPATH'] + '/' + _inipath)
                else:
                    return object.__new__(cls)
                try:
                    GlobalLock.acquire()
                    cf = ConfigParser.ConfigParser()
                    cf.read(_inipath)
                    if cf.has_section('GlobalCfg'):
                        for key in cf.options('GlobalCfg'):
                            cls.__global_config[key] = cf.get('GlobalCfg', key)
                    if cf.has_section('Policy'):
                        for key in cf.options('Policy'):
                            cls.__policy[key] = cf.getint('Policy', key)
                    cls.__loaded = True
                finally:
                    GlobalLock.release()
        return object.__new__(cls)

    @classmethod
    def getCFGattr(cls, key):
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

    @classmethod
    def isload(cls):
        return cls.__loaded


class AppConf:
    __cfg = {
        'appName': None,
        'workDir': None,
        'topDir': os.environ['DistJETPATH']
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
    set_inipath('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/config/default.cfg')
    config = Config()
    if config.__class__.isload():
        print 'config file loaded'
    print config.__class__.getCFGattr('svc_name')
