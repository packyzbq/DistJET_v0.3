from python.IScheduler import SimpleTaskScheduler
import os
import UnitTestApp

def run(app_config_path):
    app = UnitTestApp.UnitTestApp(os.environ['DistJETPATH']+"/Application/UnitTest/",'UnitTest')
    app.set_resdir(os.environ['DistJETPATH']+"/UnitTest")
    #app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/UnitTest/res")
    app.set_scheduler(SimpleTaskScheduler)
    #app2 = UnitTestApp.UnitTestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/",'UnitTest')
    #app2.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/res2")
    #app2.set_scheduler(SimpleTaskScheduler)
    return [app]

