import os
import sys
import types
class Backend:
    '''
    Backend module
    '''
    def __init__(self,):
        # scan Backend dir
        self.backend_list={}
        self.backend=""
        self.backend_obj = None


    def _loadBackend(self):
        if not self.backend:
            return False
        dir = os.listdir(os.environ['DistJET_PATH'] + '/Backend')
        for d in dir:
            if os.path.isdir(d):
                self.backend_list[d] = os.path.abspath(d)
        if self.backend_list.has_key(self.backend):
            # load backend script
            sys.path.append(dir+'/%s'%self.backend)
            module = __import__('script')
            if not module.__dict__.has_key("backend") or not callable(module.__dict__['backend']):
                return False
            self.backend_obj = module.backend()
            return True

    def setBackend(self,backend):
        self.backend = backend.lower()
        return self._loadBackend()

    def apply(self, work_num):
        # return host list
        return self.backend_obj.apply(work_num)

    def release(self, **kwargs):
        # return true/false
        return self.backend_obj.release(**kwargs)
