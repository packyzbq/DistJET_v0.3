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
            config_path=os.environ['DistJETPATH']+'/Application/AnalysisApp/config.ini'
        super(AnaApp, self).__init__(rootdir,name,config_path=config_path)


        self.step_script=script_dict
        self.app_config = Config.AppConf(config_path)
        self.anascp={}
        self.data_path = self.app_config.get('input_dir')
        self.workflow = self.app_config.get('workflow').split()

        self.__load = True

        for step in self.workflow:
            if self.app_config.get(step) is not None:
                self.step_script[step] = self.app_config.get(step)
        if not os.path.exists(self.data_path):
            self.log.error("[Analysis App] Data path not found, failed")
            self.__load = False
        else:
            self.setStatus('boot')
            self.setStatus('data')


    #def _find_scrip(self):
    #    if not os.environ['TUTORIALROOT']:
    #        self.log.warning('[Analysis App] Can not find analysis script, exit')
    #        return None


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
            for d in os.listdir(self.data_path):
                d = self.data_path+'/'+d
                if os.path.isdir(d):
                    sample_list.append(d)
        else:
            sample_list = sample_list.split()
        for sample in sample_list:
            sample_dict[os.path.basename(sample)] = {}
            for subdir in os.listdir(self.data_path+'/'+sample):
                subdir=self.data_path+'/'+sample+'/'+subdir
                if os.path.isdir(subdir):
                    #sample_dict[os.path.basename(sample)][os.path.subdir]={}
                    for energy in os.listdir(subdir):
                        energy = subdir+'/'+energy
                        if os.path.isdir(energy):
                            #sample_dict[sample][subdir][energy] = os.path.abspath(energy)
                            energy_path.append(os.path.abspath(energy))
                            self._gen_list(os.path.abspath(energy))
        return energy_path

    def _gen_list(self, energy_path):
        # generate input list and result directory
        for step in self.workflow:
            if step in os.listdir(energy_path):
                with open(energy_path+'/'+'lists_%s.txt'%step,'w+') as list_file:
                    for data_list in os.listdir(energy_path+'/'+step):
                        if (data_list.startswith('user') and data_list.endswith('.root')) or (step=='rec' and data_list.endswith('.root')):
                            list_file.write(energy_path+'/'+step+'/'+data_list+'\n')
            if not os.path.exists(energy_path+'/'+step+'_ana'):
                os.mkdir(energy_path+'/'+step+'_ana')
        #set calib output
        #os.environ['JUNOTEST_CALIB_ANA_OUTPUT']=energy_path+'/calib_ana'

    def split(self):
        if not self.__load:
            return None
        task_list=[]
        energy_path = self._find_data()
        for step in self.workflow:
            for energy in energy_path:
                if step in os.listdir(energy):
                    task = Task()
                    task.boot.append("cd %s"%energy)
                    if step == 'calib':
                        task.boot.append('export JUNOTEST_CALIB_ANA_OUTPUT=%s'%energy+'/calib_ana')
                    task.boot.append('pwd')
                    if self.step_script[step].endswith('.sh'):
                        task.boot.append("bash %s"%self.step_script[step])
                    else:
                        task.boot.append("root -b -l -q %s"%self.step_script[step])
                    task.res_dir = energy_path
                    task_list.append(task)
        return task_list



if __name__ == "__main__":
    app = AnaApp(os.environ['DistJETPATH']+"/Application/AnalysisApp/", 'AnalysisApp',sys.argv[1])
    app.res_dir = (os.environ['DistJETPATH']+'/Application/AnalysisApp/test')
    task_list = app.split()
    if task_list is None:
        exit()
    for task in task_list:
        print "task id: %d, boot:%s \ndir=%s\n"%(task.tid,task.boot,task.res_dir)
