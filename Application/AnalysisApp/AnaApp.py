import sys,os
sys.path.append(os.environ['DistJETPATH'])
from python.IApplication.JunoApp import JunoApp
from python.Task import ChainTask,Task
import python.Util.Config as Config

script_dict={
    'detsim': os.environ['TUTORIALROOT']+"/share/SimAnalysis/drawDetSim.C+",
    'elecsim': os.environ['TUTORIALROOT']+"/share/ElecAnalysis/ElecAnalysis.c+",
    'calib': os.environ['TUTORIALROOT']+"/share/CalibAnalysis/runValidation.sh",
    'rec': os.environ['TUTORIALROOT']+"/share/RecAnalysis/RecAnalysis.c+"
}

class AnaApp(JunoApp):
    def __init__(self,rootdir, name, config_path=None):
        if not config_path or not os.path.exists(os.path.abspath(config_path)):
            config_path=os.environ['DistJETPATH']+'/Application/AnaApp/config.ini'
        super(AnaApp, self).__init__(rootdir,name,config_path=config_path)


        self.step_script=script_dict
        self.app_config = Config.AppConf(config_path)
        self.anascp={}
        self.data_path = self.app_config.get('input_dir')
        self.workflow = self.app_config.get('workflow')

        for step in self.workflow:
            if self.app_config.get(step) is not None:
                self.step_script[step] = self.app_config.get(step)

        if not os.path.exists(self.data_path):
            print "[Analysis App] Data path not found, failed"


    def _find_scrip(self):
        if not os.environ['TUTORIALROOT']:
            self.log.warning('[Analysis App] Can not find analysis script, exit')
            return None


    def _find_data(self):
        '''
        find the simulation data, get a list of different energy
        :return: sample:subdir:energy = path
        '''

        sample_list = self.app_config.get('sample_list')
        sample_dict={}
        energy_path = []
        if sample_list is None:
            sample_list=[]
            for dir in os.listdir(self.data_path):
                if os.path.isdir(dir):
                    sample_list.append(dir)
        for sample in sample_list:
            sample_dict[sample] = {}
            for subdir in os.listdir(self.data_path+'/'+sample):
                if os.path.isdir(subdir):
                    sample_dict[sample][subdir]={}
                    for energy in os.listdir(self.data_path+'/'+sample+'/'+subdir):
                        if os.path.isdir(energy):
                            sample_dict[sample][subdir][energy] = os.path.abspath(energy)
                            energy_path.append(os.path.abspath(energy))
                            self._gen_list(os.path.abspath(energy))
        return energy_path

    def _gen_list(self, energy_path):
        # generate input list and result directory
        for step in self.workflow:
            if step in os.listdir(energy_path):
                with open(energy_path+'/'+step+'/'+'lists_%s.txt'%step,'w+') as list_file:
                    for data_list in os.listdir(energy_path+'/'+step):
                        if data_list.startswith('user') and data_list.endswith('.root'):
                            list_file.write(os.path.abspath(data_list)+'\n')
            os.mkdir(energy_path+'/'+step+'ana')

    def split(self):
        task_list=[]
        energy_path = self._find_data()
        for step in self.workflow:
            for energy in energy_path:
                if step in os.listdir(energy):
                    task = Task()
                    task.boot.append("cd %s"%energy)
                    if self.step_script[step].endswith('.sh'):
                        task.boot.append("bash %s"%self.step_script[step])
                    else:
                        task.boot.append("root -b -l -q %s"%self.step_script[step])
                    task.res_dir = energy_path
                    task_list.append(task)
        return task_list



if __name__ == "__main__":
    app = AnaApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/AnalysisApp/", 'AnalysisApp')
    app.res_dir = (os.environ['DistJETPATH']+'/AnalysisApp/test')
    task_list = app.split()
    for task in task_list:
        print "task id: %d, boot:%s \ndir=%s\n"%(task.tid,task.boot,task.res_dir)