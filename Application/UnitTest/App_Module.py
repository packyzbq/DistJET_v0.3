from python.IScheduler import SimpleTaskScheduler
import UnitTestApp

def run():
    app = UnitTestApp.UnitTestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/",'UnitTest')
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/python/Test/UnitTest")
    app.set_scheduler(SimpleTaskScheduler)
    return [app]

