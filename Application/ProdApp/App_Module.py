from python.IScheduler import SimpleTaskScheduler
import ProdApp

def run():
    app = ProdApp.ProdApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/UnitTest/","ProdApp",config_path="config.ini")
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/ProdAppTest/res")
    app.set_scheduler(SimpleTaskScheduler)
    return [app]