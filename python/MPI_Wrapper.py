import Server_Module as SM
import Client_Module as CM
#Waring remember to add exception handler: try catch
MPI_log = None

class Tags:
    MPI_PING = 6

    MPI_REGISTY = 1
    MPI_REGISTY_ACK = 12
    MPI_DISCONNECT = 2
###### NUM <100 for mpich ; NUM>= 100 for python
    WORKER_STOP     =100
    WORKER_INFO     = 101   #w->m sync worker info (uuid, capacity)
    WORKER_HALT     = 102

    TASK_FIN        = 110      #w->m   worker notify completed tasks,

    TASK_SYNC       = 111     #m<->w   master ask for work info
                        # worker->master: wid,tid
    TASK_ADD        = 112  # m->w   (tid, task_boot, task_data, res_dir)
    TASK_REMOVE     = 113  # m->w   remove worker task, maybe give it to another worker (tid)

    APP_INI         = 120   #m->w   master schedule app and transfer the init data  (app_ini_boot, app_ini_data, res_dir)
                    #w->m   init result                                     (wid, res_dir)
    APP_INI_ASK     = 121   #w->m ask for app ini boot and data
    APP_FIN         = 122       #m->w   master tell worker how to finalize
                        #W->M   worker ask for finalize operation
    LOGOUT          = 130
    LOGOUT_ACK      = 131    #m->w ack for worker logout requirement

    NEW_APP         = 140   # master tell worker has a new app, and restart


class Server:
    """
    Set up a server using C++ lib
    """
    def __init__(self, recv_buffer, svcname):
        self.server = SM.MPI_Server(recv_buffer, svcname)
        self.svcname = svcname
    def initialize(self):
        ret = self.server.initialize()
        if ret != 0:
            # log init error, boost return error need to be handled
            MPI_log.error('[MPI server]: Initialize server error, error code=%d',ret)
            return ret
        else:
            MPI_log.info('[MPI server]: start server with service_name=%s', self.svcname)
            return ret

    def send_int(self, int_data, msgsize, dest, tags):
        self.server.send_int(int_data, msgsize, dest, tags)

    def send_string(self, strs ,msgsize, dest, tag):
        ret = self.server.send_string(strs, msgsize, str(dest), tag)
        return ret

    def run(self):
        self.server.run()

    def command_analyze(self, command):
        pass

    def stop(self):
        ret = self.server.stop()
        return ret

class Client:
    """
    Set up a client(workerAgent) using C++ lib
    """
    def __init__(self, recv_buffer, svcname, uuid):
        self.client = CM.MPI_Client(recv_buffer, svcname, uuid)
        pass

    def ping(self, uuid):
        self.send_string(uuid, len(uuid), 0, Tags.MPI_PING)

    def initial(self):
        return self.client.initialize()

    def run(self):
        self.client.run()

    def send_int(self, int_data, msgsize, dest, tags):
        print('[Python-Client]: send string=%s, tag=%d' % (str, tags))
        self.client.send_int(int_data, msgsize, dest, tags)

    def send_string(self, strs ,msgsize, dest, tags):
        return self.client.send_string(strs, msgsize, dest, tags)

    def stop(self):
        return self.client.stop()

class MSG:
    def __init__(self, tag, pack):
        self.tag = tag
        self.pack = pack
