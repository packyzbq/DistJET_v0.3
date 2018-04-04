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
        backdir_list = os.listdir(os.environ['DistJETPATH'] + '/Backend')
        backdir = os.environ['DistJETPATH']+'/Backend'
        for d in backdir_list:
            if os.path.isdir(backdir+'/'+d):
                self.backend_list[d] = os.path.abspath(backdir+'/'+d)
        if self.backend_list.has_key(self.backend):
            # load backend script
            sys.path.append(os.environ['DistJETPATH'] + '/Backend/%s'%self.backend)
            module = __import__('script')
            if not module.__dict__.has_key("backend") or not callable(module.__dict__['backend']):
                return False
            self.backend_obj = module.backend()
            return True
        else:
            print("Can't find backend %s, backend list is %s"%(self.backend, [back for back in self.backend_list.keys()]))
            return False

    def setBackend(self,backend):
        self.backend = backend.upper()
        return self._loadBackend()

    def apply(self, work_num):
        # return host list
        return self.backend_obj.apply(work_num)

    def release(self, **kwargs):
        # return true/false
        return self.backend_obj.release(**kwargs)
