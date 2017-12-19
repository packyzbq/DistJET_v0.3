import sys,os
sys.path.append(os.environ['DistJETPATH'])
from python.IApplication.JunoApp import JunoApp
from python.Task import ChainTask,Task
import python.Util.Config as Config

class AnaApp(JunoApp):
    def __init__(self,rootdir, name, config_path=None):
        if not config_path or not os.path.exists(os.path.abspath(config_path)):
            config_path=os.environ['DistJETPATH']+'/Application/ProdApp/config.ini'
        super(AnaApp, self).__init__(rootdir,name,config_path=config_path)

        self.anascp={}


    def _find_scrip(self):
        if not os.environ['TUTORIALROOT']:
            self.log.warning('[Analysis App] Can not find analysis script, exit')
            return None
        file_list =