//
// Created by zhaobq on 2017-4-24.
//

#ifndef MPI_CONNECT_MPI_SERVER_H
#define MPI_CONNECT_MPI_SERVER_H

#include "MPI_Base.h"
#include "MPI_Util.h"
#include <list>
#include <map>
#include <string.h>


class MPI_Server : public MPI_Base{
private:
    char* svc_name_;
    map<string ,MPI_Comm> comm_map;
    pthread_mutex_t comm_list_mutex;
    char port[MPI_MAX_PORT_NAME];

	MPI_Errhandler eh;
    char port_file[1024];

    pthread_t pth_accept;
    pthread_mutex_t accept_flag_mutex;
	pthread_mutex_t dummy_f_mutex;
    bool accept_f = true;       // true->stop; false->running
    bool allowstop = false;
    bool dummy_conn = false;    // flag for dummy connection has established



public:
    MPI_Server(IRecv_buffer* rh, char* svc_name);

    ~MPI_Server();

    //void run();
    int initialize();
    int stop();
    int finalize();

    bool new_msg_come(ARGS *args);
    void recv_handle(ARGS args, void* buf);

    int send_string(char *buf, int msgsize, string dest_uuid, int tag);
    void errhandler(MPI_Comm *comm, int* errcode,...);
    //int send_int(int buf, int msgsize, string dest_uuid, int tag);
    static void* accept_conn_thread(void* ptr);

    //bool disconnect_client(int w_uuid);
    //void bcast(void *buf, int msgsz, MPI_Datatype datatype, int tags);
    void set_accept_f_stop(){
        pthread_mutex_lock(&accept_flag_mutex);
        accept_f = true;
        pthread_mutex_unlock(&accept_flag_mutex);
    };
    int get_Commlist_size(){
        return comm_map.size();
    };
    bool get_stop_permit(){
        return allowstop;
    };
    void print_Commlist(){
        map<string, MPI_Comm>::iterator iter;
        pthread_mutex_lock(&comm_list_mutex);
        for(iter = comm_map.begin(); iter != comm_map.end(); iter++){
            cout << iter->first <<" :: " << iter->second << endl;
        }
        pthread_mutex_unlock(&comm_list_mutex);
    };
	
	void set_portfile(char* port_path){
		strcpy(port_file, port_path);
	};
};

#endif //MPI_CONNECT_MPI_SERVER_H
