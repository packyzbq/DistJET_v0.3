import os,types,json

class BaseElement(object):
    def __init__(self,wid):
        self.wid = wid
        self.running_task = None
        self.cpuid = None
        self.cpurate = None
        self.mem = None
        self.delay=None
        self.extra=None

    def check_integrity(self):
        if self.cpurate and self.cpuid and self.mem:
            return True
        else:
            return False
	
    def toDict(self):
        return self.__dict__

class BaseRecoder(object):
    def __init__(self,basepath):
        if basepath and not os.path.exists(basepath):
            os.mkdir(basepath)
        self.basepath = basepath+'/monitor'
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)
        self.recode_file = {} # worker_id: file

    def set_message(self,wid,message):
        if not self.recode_file.has_key(wid):
            self.recode_file[wid] = os.path.abspath(self.basepath)+'/'+str(wid)+'.tmp'
        tmpfile = open(self.recode_file[wid],'a')
        try:
            if type(message) == types.StringType:
                tmpfile.write(message+'\n')
            elif isinstance(message,BaseElement):
                tmpfile.writelines(json.dumps(message.toDict())+'\n')
            else:
                tmpfile.write(json.dumps(message.__dict__)+'\n')
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
