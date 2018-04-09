from python.IScheduler import SimpleTaskScheduler
import ProdApp

def run(app_config_path):
    app = ProdApp.ProdApp("/junofs/users/zhaobq/DistJET_v0.3/Application/ProdApp/","ProdApp",config_path="config.ini")
    app.set_resdir("/junofs/users/zhaobq/DistJET_v0.3/ProdAppTest/res")
    app.set_scheduler(SimpleTaskScheduler)
    return [app]
