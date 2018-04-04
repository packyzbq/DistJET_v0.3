import Client_Module as CM
import IR_Buffer_Module as IM
import time
import uuid

recver = IM.IRecv_buffer()
uid = str(uuid.uuid4())
client=CM.MPI_Client(recver, "Default",uid)
client.set_portfile("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/MPI_Connect_v2/bin/port.txt")
print "client uuid = %s"%uid
ret = client.initialize()
if ret != 0:
    print "client error"
    exit()
time.sleep(3)
client.send_string(uid,len(uid),0,1)
'''
try:
    while True:
        time.sleep(1)
        #print('buffer size=%d'%recver.size())
        if not recver.empty():
            msg_t = recver.get()
            if msg_t.tag == -1:
                continue
            if msg_t.tag == 12:
                send_str = 'app_ini'
                client.send_string(send_str, len(send_str), 0, 12)
                break;
except KeyboardInterrupt:
    time.sleep(1)
    client.stop(False)
'''
time.sleep(2)
client.stop(False)

