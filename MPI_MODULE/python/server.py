import Server_Module as SM
import IR_Buffer_Module as IM
import time

recv_buffer = IM.IRecv_buffer()
server = SM.MPI_Server(recv_buffer, 'Default')
server.set_portfile("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/MPI_Connect_v2/bin/port.txt")
ret = server.initialize()
tag = 0
print 'init ret = %d'%ret
#server.test_hi()
try:
    #while server.get_stop_permit() and server.get_Commlist_size() > 0:
    while True:
        #print('buffer size=%d'%recv_buffer.size())
        if not recv_buffer.empty():
            pack = recv_buffer.get()
            tag = pack.tag
            print 'tag=%d, sbuf=%s'%(tag,pack.sbuf)
            #if tag == 1:
            #    server.send_string('76',2,'test',12)
except:
    pass
finally:
    ret = server.stop()
    if ret:
        print "stop ret = %d"%ret
