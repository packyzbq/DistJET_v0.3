//
// Created by zhaobq on 2017-4-24.
//

//#define DEBUG

#include <pthread.h>
#include "Include/MPI_Base.h"

using namespace std;

bool MPI_Base::Recv_Probe(MPI_Comm comm, MPI_Status *stat) {
    if(comm == 0x0)
        return false;
    int merr = 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];
    int flag = 0;
    merr = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &flag, stat);
    if (merr) {
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Error]: Iprobe message error :" << errmsg << endl;
        return false;
    }
    return true;
}

void* MPI_Base::recv_thread(void *ptr) {
    int msgsz, merr, msglen;
    double starttime, endtime;
    void *rb = NULL;
    char errmsg[MPI_MAX_ERROR_STRING];

    pthread_t pid;
    pid = pthread_self();
    ARGS args;
    MPI_Status recv_st;

    MPI_Comm_rank(MPI_COMM_WORLD, &(((MPI_Base*)ptr)->myrank));
    MPI_Comm_size(MPI_COMM_WORLD, &(((MPI_Base*)ptr)->w_size));
	
	pthread_mutex_lock(&(((MPI_Base*)ptr)->recv_flag_mutex));
    ((MPI_Base*)ptr)->recv_f = false;
    bool recv_f_dup = ((MPI_Base*)ptr)->recv_f;
    pthread_mutex_unlock(&(((MPI_Base*)ptr)->recv_flag_mutex));

#ifdef DEBUG
    cout <<"<recv thread>: Proc: "<< ((MPI_Base*)ptr)->myrank << ", Pid: " << pid << ", receive thread start...  "<<endl;
#endif
    
    // TODO add exception handler -> OR add return code
    while(!recv_f_dup){
        if(((MPI_Base*)ptr)->new_msg_come(&args)){
            MPI_Get_count(&args.arg_stat, args.datatype, &msgsz);
#ifdef DEBUG
            cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<" recv thread >: detect message length=" << msgsz << endl;
#endif
            switch (args.datatype)
            {
                case MPI_INT:
                    rb = new int[msgsz];
                    break;
                case MPI_CHAR:
                    rb = new char[msgsz];
                    break;
                default:
                    rb = new char[msgsz];
                    break;
            }
            starttime = MPI_Wtime();
            merr = MPI_Recv(rb, msgsz, args.datatype, args.arg_stat.MPI_SOURCE, args.arg_stat.MPI_TAG, args.newcomm, &recv_st);
            endtime = MPI_Wtime();
            if(merr){
                MPI_Error_string(merr, errmsg, &msglen);
                cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: receive error: " << errmsg << endl;
                //TODO error handle return code
            }
#ifdef DEBUG
            cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: receive a message from <"<< args.arg_stat.MPI_SOURCE<<"> <== <" << (char*)rb << ", msgsize=" << msgsz<< ">" << endl;
            cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: start recv Barrier" << endl;
#endif
            merr = MPI_Barrier(args.newcomm);
#ifdef DEBUG
            cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: end recv Barrier" << endl;
#endif
            if(merr){
                MPI_Error_string(merr, errmsg, &msglen);
                cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: barrier fail...error: " << errmsg << endl;
                //TODO Add error handle
            }
#ifdef DEBUG
            cout << "<Rank "<<((MPI_Base*)ptr)->myrank <<"recv thread>: handled by recv_handler" << endl;
#endif
            ((MPI_Base*)ptr)->recv_handle(args,rb);
            Pack p = Pack(args.arg_stat.MPI_TAG,msgsz);
            if(args.datatype == MPI_INT){
                p.ibuf_ = (*(int*)rb);
#ifdef DEBUG
                cout << "pack creat, ibuf = " << p.ibuf_ << endl;
#endif
            }
            if(args.datatype == MPI_CHAR) {
                p.sbuf_ = (char *) rb;
#ifdef DEBUG
                cout << "pack creat, sbuf = " << p.sbuf_ <<" size = " << p.size_<< endl;
#endif
            }
            ((MPI_Base*)ptr)->rv_buf->put(p);
        }
        if(!rb)
            delete(rb);

        pthread_mutex_lock(&(((MPI_Base*)ptr)->recv_flag_mutex));
        recv_f_dup = ((MPI_Base*)ptr)->recv_f;
        pthread_mutex_unlock(&(((MPI_Base*)ptr)->recv_flag_mutex));
    }

    return 0;
}

bool MPI_Base::new_msg_come(ARGS *args) {
    cout << "[Error]: father function, error to reach" << endl;
    return false;
}

void MPI_Base::set_recv_stop() {
    pthread_mutex_lock(&recv_flag_mutex);
    recv_f = true;
    pthread_mutex_unlock(&recv_flag_mutex);
}
