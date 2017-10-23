from IApplication import IApplication
from python.Util import Config
import os
junodir = '/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6'
class JunoApp(IApplication):
    def __init__(self,rootdir, name, config_path=None):
        super(JunoApp,self).__init__(rootdir,name,config_path)
        self.JUNOTOP = self.app_config.get('JunoTop')
        if self.JUNOTOP is None:
            self.JUNOTOP = self.app_config.get('JunoVer')
            if 'Pre' in self.JUNOTOP:
                self.JUNOTOP = junodir+'/Pre-Release/'+self.JUNOTOP
            else:
                self.JUNOTOP = junodir + '/Release/' + self.JUNOTOP
        if not os.path.exists(self.JUNOTOP):
            self.log.warning('[JUNOAPP] Cannot find JUNOTOP dir %s, using latest version.'%self.JUNOTOP)
            self.JUNOTOP = self._findJunoTop()

    def setup(self):
        if self.JUNOTOP and os.path.exists(self.JUNOTOP):
            return ['source %s'%self.JUNOTOP]
        else:
            self._findJunoTop()

    def _setJunoTop(self,path):
        if os.path.exists(path):
            self.JUNOTOP = path

    def _findJunoTop(self):
        os.chdir(junodir)
        output = os.popen('ls -rt *Release|tail -1')
        return junodir+'/'+output.read()[:-1]


