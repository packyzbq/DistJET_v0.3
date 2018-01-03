from IApplication import IApplication
from python.Util import Config
import os
junodir = '/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6'
class JunoApp(IApplication):
    def __init__(self,rootdir, name, config_path=None):
        super(JunoApp,self).__init__(rootdir,name,config_path)
        self.JUNOTOP=os.environ['JUNOTOP']
        '''
        if self.app_config is not None:
            self.JUNOTOP = self.app_config.get('JunoTop')
        if self.app_config and self.JUNOTOP is None:
            self.JUNOTOP = self.app_config.get('JunoTop')
            if self.JUNOTOP:
                if 'Pre' in self.JUNOTOP:
                    self.JUNOTOP = junodir+'/Pre-Release/'+self.JUNOTOP
                else:
                    self.JUNOTOP = junodir + '/Release/' + self.JUNOTOP
        if not self.JUNOTOP or( self.JUNOTOP and not os.path.exists(self.JUNOTOP)):
            self.log.warning('[JUNOAPP] Cannot find JUNOTOP dir %s, using latest version.'%self.JUNOTOP)
            self.JUNOTOP = self._findJunoTop()
        '''
        self.log.info('[JunoAPP] Set JunoTop = %s'%self.JUNOTOP)

    def setup(self):
        if not self.JUNOTOP or not os.path.exists(self.JUNOTOP):
            self.log.info('[JunoAPP] Cannot find JUNOTOP:%s ,use default'%self.JUNOTOP)
            self.JUNOTOP = self._findJunoTop()
        return ['. %s/setup.sh'%self.JUNOTOP]

    def _setJunoTop(self,path):
        if os.path.exists(path):
            self.JUNOTOP = path

    def _findJunoTop(self):
        back_dir = os.getcwd()
        print "current dir = %s"%back_dir
        os.chdir(junodir)
        output = os.popen('ls -rt *Release|tail -n 4')
        out = output.read()[:-1]
        out = out.split('\n')
        out.reverse()
        path=None
        for basename in out:
            if 'branch' not in basename:
                if 'Pre' in basename:
                    path = junodir+'/Pre-Release/'+basename
                else:
                    path = junodir+'/Release/'+basename
		os.chdir(back_dir)
        return path


