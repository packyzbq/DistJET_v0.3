from python.IScheduler import SimpleTaskScheduler
import UnitTestApp

def run(app_config_path):
    app = UnitTestApp.UnitTestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/",'UnitTest')
    app.set_resdir("/junofs/users/zhaobq/UnitTest/res")
    #app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/UnitTest/res")
    app.set_scheduler(SimpleTaskScheduler)
    app2 = UnitTestApp.UnitTestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/",'UnitTest')
    app2.set_resdir("/junofs/users/zhaobq/UnitTest/res2")
    app2.set_scheduler(SimpleTaskScheduler)
    return [app,app2]

