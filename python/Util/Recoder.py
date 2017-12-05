import os,types,pickle

class BaseElement(object):
    def __init__(self,wid):
        self.wid = wid
        self.running_task = None
        self.cpuid = None
        self.cpurate = None
        self.mem = None

    def check_integrity(self):
        if self.cpurate and self.cpuid and self.mem:
            return True
        else:
            return False

class BaseRecoder(object):
    def __init__(self,basepath):
        self.basepath = basepath+'/tmp'
        os.mkdir(self.basepath)
        self.recode_file = {} # worker_id: file

    def set_message(self,wid,message):
        if not self.recode_file.has_key(wid):
            self.recode_file[wid] = os.path.abspath(self.basepath)+'/'+str(wid)+'.tmp'
        tmpfile = open(self.recode_file[wid])
        try:
            if type(message) == types.StringType:
                tmpfile.write(message+'\n')
            else:
                tmpfile.write(pickle.dumps(message)+' ')
        finally:
            tmpfile.flush()
            tmpfile.close()

    def handle_message(self):
        pass

    def finalize(self, cleanup=False):
        self.handle_message()
        if cleanup:
            for file in self.recode_file.values():
                os.remove(file)
