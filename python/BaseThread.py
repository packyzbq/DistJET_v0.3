import threading
from Util import logger

base_thread_log = logger.getLogger("BaseThread")

class BaseThread(threading.Thread):
    """
    Base Thread class for this framework
    """
    def __init__(self, name=""):
        name = '.'.join(['DistJET.BaseThread',name])
        print("[BaseThread]: create new thread : %s" % name)
        super(BaseThread, self).__init__(name=name)
        self.setDaemon(1)
        self.lock = threading.RLock()
        self.__should_stop = False
        base_thread_log.debug('BaseThread object created:%s', self.__class__.__name__)

    def get_stop_flag(self):
        return self.__should_stop

    def stop(self):
        self.lock.acquire()
        if not self.__should_stop:
            base_thread_log.debug('Stopping: %s', self.__class__.__name__)
            self.__should_stop = True
        self.lock.release()
