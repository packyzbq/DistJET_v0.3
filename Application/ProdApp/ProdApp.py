import sys,os
sys.path.append(os.environ['DistJETPATH'])
from python.IApplication.JunoApp import JunoApp
from python.Task import ChainTask,Task
import python.Util.Config as Config
import subprocess

def MakeandCD(path):
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

class ProdApp(JunoApp):
    def __init__(self,rootdir, name, config_path=None):
        if not config_path or not os.path.exists(os.path.abspath(config_path)):
            #self.log.warning('[ProdApp] Cannot find config file = %s, use default'%config_path)
            config_path = os.environ['DistJETPATH']+'/Application/ProdApp/config.ini' 
        super(ProdApp,self).__init__(rootdir,name,config_path)
        self.task_reslist={}
        self.workflow=[]
        self.driver_dir = []
        self.driver={} # driver_name: scripts_list
        self.sample_list=[]

        self.setStatus('boot')
        self.setStatus('data')
	    
        self.cfg = Config.AppConf(config_path,'ProdApp')
        self.njob = self.cfg.get('njobs')
        if self.cfg.get('sample_list') is not None:
            self.sample_list.extend(self.cfg.get('sample_list').strip().split(' '))
        else:
            self.sample_list.extend(self.cfg.getSections())

        self.tags={}
        for sample in self.sample_list:
            self.tags[sample] = []
            self.tags[sample].extend(self.cfg.get('tags',sample).strip().split(' '))



    def split(self):
        task_list = []
        for sample in self.sample_list:
            driver = self.cfg.get('driver',sec=sample)
            # gen boot command
            chain_script = self._find_driver_script(driver_name=driver)
            if not chain_script:
                self.log.warning('WARN: Can not find specify driver: %s for sample:%s, skip simaple' % (
                self.cfg.get('driver', sec=sample), sample))
                continue
            scripts = self.cfg.get('scripts',sec=sample)
            if scripts:
                script_list = scripts.strip().split()
                scripts = {}
                for scp in script_list:
                    if scp in chain_script.keys():
                        scripts[scp] = chain_script[scp]
       
            if not scripts:
                scripts = chain_script
            elif not set(chain_script.keys()) > set(scripts):
                self.log.warning("WARN: Can not find specified scripts: %s in driver: %s, skip" % (
                scripts, self.cfg.get('driver', sec=sample)))

            topdir = self.cfg.get('topdir',sec=sample)
            if not topdir:
                rundir = Config.Config.getCFGattr('Rundir')
                if not rundir:
                    rundir = os.getcwd()
                    print 'rundir = %s'%rundir
                topdir = rundir+'/'+sample
                self.log.warning('WARN: Top dir is None, use default: %s'%(topdir))
            #if not os.path.exists(topdir):
            #    os.mkdir(topdir)

            sample_dir = self.res_dir+'/'+sample
            MakeandCD(sample_dir)

            tags = self.cfg.get('tags',sec=sample).strip().split(' ')
            workflow = self.cfg.get('workflow',sec=sample).strip().split(' ')

            seed_base = int(self.cfg.get('seed',sec=sample))
            maxevt = self.cfg.get('evtmax',sec=sample)
            assert seed_base is not None or maxevt is not None
            task_njob = int(maxevt)/int(self.njob)
            rest_evt = int(maxevt)%task_njob
            print "scripts = %s"%scripts
            for spt in scripts.keys():
                evt_count = 0
                worksubdir = None
                if 'uniform' in spt:
                    worksubdir = 'uniform'
                elif 'center' in spt:
                    worksubdir = 'center'
                task_resdir = sample_dir+'/'+worksubdir
                if not os.path.exists(task_resdir):
                    os.mkdir(task_resdir)
                MakeandCD(sample_dir+'/'+worksubdir)
                pre_task = None
                for tag in tags:
                    # Add detsim 0
                    detsim0 = ChainTask()
                    detsim0.boot.append("bash %s/run-%s-%s.sh" % (os.getcwd() + '/' + tag + '/detsim', 'detsim', '0'))
                    detsim0.res_dir = task_resdir
                    task_list.append(detsim0)
                    while True:
                        #if (rest_evt != 0 and evt_count >= int(maxevt)+task_njob) or (rest_evt == 0 and evt_count >= int(maxevt)):
                        if evt_count >= int(maxevt):
                            break
                        for wf in workflow:
                            if evt_count+task_njob < maxevt:
                                args = self._gen_args(sample,str(seed_base),str(task_njob),tag,wf,worksubdir)
                            else:
                                args = self._gen_args(sample,str(seed_base),str(rest_evt),tag,wf,worksubdir)
                            #self.log.info('bash %s %s'%(spt,args))
                            os.system('bash %s %s'%(chain_script[spt],args))
                            tmp_task = ChainTask()
                            tmp_task.boot.append("bash %s/run-%s-%s.sh"%(os.getcwd()+"/"+tag+'/'+wf,wf,seed_base))
                            #tmp_task.boot = "bash %s/run-%s-%s.sh"%(os.getcwd()+"/"+tag+'/'+wf,wf,seed_base+seed_offset)
                            tmp_task.res_dir = task_resdir
                            if wf != 'detsim':
                                tmp_task.set_father(pre_task)
                            if wf == 'rec':
                                detsim0.set_child(tmp_task)
                                tmp_task.set_father(detsim0)
                            if pre_task:
                                pre_task.set_child(tmp_task)
                            pre_task = tmp_task
                            self.log.debug('[ProdApp] Creat task : %s'%tmp_task.toDict())
                            task_list.append(tmp_task)
                        seed_base += 1
                        evt_count+=task_njob
                        pre_task = None

        os.chdir(self.res_dir)
        return task_list






    def _find_driver_script(self,driver_name=None):
        """
        :param driver_name:
        :return: self.driver[driver_name] = {scp1: scp1_path, scp2:scp2_path...}
        """
        # position the dirver dir
        top_driver_dir = self.JUNOTOP+'/offline/Validation/JunoTest/production'
        user_extra_dir=[]
        user_extra_dir_str = os.getenv("JUNOTESTDRIVERPATH")
        if user_extra_dir_str:
            user_extra_dir.extend(user_extra_dir_str.split(":"))
        user_extra_dir.append(top_driver_dir)
        # get driver list
        for dd in user_extra_dir:
            # check directory exists or not
            if not os.path.exists(dd):
                self.log.warning("WARN: %s does not exist" % dd)
                continue
            if not os.path.isdir(dd):
                self.log.warning("WARN: %s is not a directory" % dd)
                continue
            # get the contents of dd
            for d in os.listdir(dd):
                path = os.path.join(dd, d)
                if not os.path.isdir(path): continue
                # d is driver
                if self.driver.has_key(d):
                    self.log.warning("WARN: %s (%s) already exists, skip %s" % (d, str(self.driver[d]), path))
                    continue

                # if the script is already in PathA, use it.
                scripts = self.driver.get(d, [])
                scripts_base = {}
                for f in scripts:
                    scripts_base[os.path.basename(f)] = f
                for f in os.listdir(path):
                    # only match prod_*
                    if not f.startswith('gen'): continue
                    # if the script is already added, skip it.
                    if scripts_base.has_key(f): continue
                    scripts_base[f] =os.path.join(path,f)
                    #scripts.append(os.path.join(path, f))
                    # print('element:%s add script %s'%(d,os.path.join(path,f)))
                if len(scripts_base):
                    self.driver[d] = scripts_base
        if driver_name:
            return self.driver.get(driver_name)

    def _gen_args(self, sample, seed, evtnum, tags, workflow,worksubdir=None,njob=1):
        args = ''
        arg_neg_list = ['driver', 'scripts', 'workflow', 'evtmax', 'seed', 'tags']
        #args += ' --setup "$JUNOTOP/setup.sh"'
        args += ' --%s "%s"' %('seed',seed)
        args += ' --%s "%s"' % ('evtmax', evtnum)
        args += ' --%s "%s"' % ('njobs', njob)
        args += ' --%s "%s"' % ('tags',tags)
        for k, v in self.cfg.other_cfg[sample].items():
            if k not in arg_neg_list:
                print '%s:%s' % (k, v)
                args += ' --%s "%s"' % (k, v)
        if not self.cfg.get('worksubdir', sample) and worksubdir:
            args += ' --worksubdir "%s"' % worksubdir
        args+=' %s'%workflow
        return args


if __name__ == '__main__':
    app = ProdApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/ProdApp/",'ProdApp')
    app.res_dir = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/ProdApp/test"
    tasklist = app.split()
    print len(tasklist)
    for task in tasklist:
        print('%s - child: %s, father: %s\n'%(task.tid, [child for child in task.get_child_list()],[father for father in task.get_father_list()]))
