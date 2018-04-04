//
// Created by zhaobq on 2017-4-24.
//

#ifndef MPI_CONNECT_V2_MPI_BASE_H
#define MPI_CONNECT_V2_MPI_BASE_H

#include "mpi.h"
#include "pthread.h"
#include "IRecv_buffer.h"
#include <iostream>

using namespace std;

struct ARGS{    //用于 new_msg_come 向 recv传递参数
    MPI_Comm newcomm;
    int source_rank;
    MPI_Datatype datatype;
    MPI_Status arg_stat;

    void print(){
        cout << "<args_info>: newcomm=" << newcomm << "; source=" << source_rank << "; datatype=" << datatype << "; status:{" << arg_stat.MPI_TAG << ";" <<arg_stat.count_lo << "}" << endl;
    }
};

class MPI_Base{
protected:
    IRecv_buffer* rv_buf;
    pthread_t recv_t;

    int myrank;
    int w_size;
    char hostname[MPI_MAX_PROCESSOR_NAME];

    pthread_mutex_t recv_flag_mutex;

    bool recv_f = true; //true = stop false = running

    bool Recv_Probe(MPI_Comm comm, MPI_Status* stat);

public:
    MPI_Base(IRecv_buffer* rbuf):rv_buf(rbuf){};
    virtual ~MPI_Base(){
        //TODO free memory
    };
    virtual int initialize(){cout << "[Error] father init..." << endl; return 0;};
    virtual void run(){cout << "[Error] father run..." << endl;};
    virtual int stop(){cout << "[Error] father stop..." << endl; return 0;};
    virtual int finalize(){ return 0;};

    static void* recv_thread(void* ptr);
    virtual int send_int(int buf, int msgsize, int dest, int tag){};
    virtual int send_string(char* buf, int msgsize, int dest, int tag){};
    virtual void errhandler(MPI_Comm *comm, int* errcode,...){};

    virtual bool new_msg_come(ARGS * args);

    void set_recv_stop();

    virtual void recv_handle(ARGS args, void* buf){cout << "[Error] father recv handler" << endl;}; //


};

#endif //MPI_CONNECT_V2_MPI_BASE_H
