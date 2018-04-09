from python.IScheduler import SimpleTaskScheduler
import ProdApp

def run(app_config_path):
    app = ProdApp.ProdApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v0.3/Application/ProdApp/","ProdApp",config_path="config.ini")
    app.set_resdir("/junofs/users/zhaobq/ProdTest/res")
    app.set_scheduler(SimpleTaskScheduler)
    return [app]
