#include "Include/MPI_Client.h"
#include <iostream>
#include <unistd.h>
#include "Include/MPI_Util.h"
#include <uuid/uuid.h>
#include <string.h>
using namespace std;

//class TMH: public IRecv_handler{
///public:
//	void handler_recv_int(int mpi_tags, Pack_Int pack){
//		cout << "I get a int msg, tag=" << mpi_tags << endl;
//	};
//	void handler_recv_str(int mpi_tags, Pack_Str pack){
//		cout << "I get a str msg, tag=" << mpi_tags << endl;
//	};
//};

int main(int argc, char** argv)
{
	uuid_t uu;
	uuid_generate(uu);
	char *uuid = (char*)uu;
	//strcpy(uu,uuid);
	IRecv_buffer rv_buf = IRecv_buffer();
	MPI_Client *client;
	if(argc > 1)
		client = new MPI_Client(&rv_buf, "Test",uuid, argv[1]);
	else
		client = new MPI_Client(&rv_buf, "Test",uuid);
	client->set_portfile("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/MPI_Connect_v2/bin/port.txt");
	client->initialize();
	client->send_string(uuid,strlen(uuid),0,MPI_REGISTER);
	sleep(5);
	client->stop(false);
	return 0;
}
