#include "Include/MPI_Server.h"
#include "Include/IRecv_buffer.h"
#include <unistd.h>
#include <iostream>
using namespace std;
//class test_msg_handler: public IRecv_handler{
//public:
//	void handler_recv_int(int mpi_tags, Pack_Int pack){
//		cout << "I get a int msg, tag=" << mpi_tags << endl;
//	};
//	void handler_recv_str(int mpi_tags, Pack_Str pack){
//		cout << "I get a str msg, tag=" << mpi_tags << endl;
//	};
//
//};

int main(){
    bool f = false;
	IRecv_buffer rv_buf = IRecv_buffer();
	MPI_Server server(&rv_buf, "Test");
	server.set_portfile("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/MPI_Connect_v2/bin/port.txt");
	int ret = server.initialize();
	cout << "@main, server init code = " << ret << endl;
	while(!(server.get_stop_permit()) || server.get_Commlist_size() > 0){
		if(!rv_buf.empty()){
			Pack p = rv_buf.get();
			cout << "@main, tag=" << p.tag_ << endl;
		}
		if(!server.get_stop_permit())
			cout << "@main not allow stop" << endl;
		else
			cout << "@main allow stop, commlist size=" << server.get_Commlist_size() << endl;
		sleep(3);
	}
	ret = server.stop();
	cout << "@main, server stop code = " << ret << endl;

	return 0;

}
