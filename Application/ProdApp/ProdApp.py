from python.IApplication.JunoApp import JunoApp
from python.Task import TaskStatus
import python.Util.Config as Config
import os
import subprocess

def MakeandCD(path):
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

class ProdApp(JunoApp):
    def __init__(self,rootdir, name, config_path=None):
        super(ProdApp,self).__init__(rootdir,name,config_path)
        self.task_reslist={}
        self.workflow=[]
        self.driver_dir = []
        self.driver={}
        self.sample_list=[]

        if not config_path or not os.path.exists(os.path.abspath(config_path)):
            self.log.warning('[ProdApp] Cannot find config file = %s, use default'%config_path)
            config_path = os.environ['DistJETPATH']+'/Application/ProdApp/config.ini'
        self.cfg = Config.AppConf(config_path,'ProdApp')
        if self.cfg.get('sample_list') is not None:
            self.sample_list.extend(self.cfg.get('sample_list').strip().split(' '))
        else:
            self.sample_list.extend(self.cfg.getSections())

        self.tags={}
        for sample in self.sample_list:
            self.tags[sample] = []
            self.tags[sample].extend(self.cfg.get('tags',sample).strip().split(' '))
        self.seed = self.cfg.get('seed')
        self.njobs=self.cfg.get('njobs')


    def split(self):
        #TODO
        task_list = []
        for sample in self.sample_list:
            chain_script = self.driver.get(self.cfg.get('driver',sec=sample))
            if not chain_script:
                self.log.warning('WARN: Can not find specify driver: %s for sample:%s, skip simaple' % (
                self.cfg.get('driver', sec=sample), sample))
                continue
            scripts = self.cfg.get('scripts',sec=sample).strip().split()
            if not scripts:
                scripts = chain_script
            elif not set(chain_script.keys()) > set(scripts):
                self.log.warning("WARN: Can not find specified scripts: %s in driver: %s, skip" % (
                scripts, self.cfg.get('driver', sec=sample)))

            topdir = self.cfg.get('topdir',sec=sample)
            if not topdir:
                topdir = Config.Config.getCFGattr('Rundir')+'/'+sample
                self.log.warning('WARN: Top dir is None, use default: %s'%(topdir))
            if not os.path.exists(topdir):
                os.mkdir(topdir)

            tags = self.cfg.get('tags',sec=sample)
            workflow = self.cfg.get('workflow',sec=sample)



        pass

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
            print "searching on %s"%dd
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


    def _gen_job_bash(self):