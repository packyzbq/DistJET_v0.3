//
// Created by zhaobq on 2017-4-25.
//

#include "Include/MPI_Client.h"
#include <string.h>
#include <fstream>

//#define DEBUG

MPI_Client::MPI_Client(IRecv_buffer *mh, char *svc_name, char *uuid):MPI_Base(mh),svc_name_(svc_name),uuid_(uuid) {
    recv_flag_mutex = PTHREAD_MUTEX_INITIALIZER;
	//strcpy(portname,port);
}

MPI_Client::MPI_Client(IRecv_buffer *mh, char *svc_name, char *uuid, char *port): MPI_Base(mh),svc_name_(svc_name),uuid_(uuid){
    recv_flag_mutex = PTHREAD_MUTEX_INITIALIZER;
    //check if port is right
    if(strlen(port) > 24 && port[strlen(port)-1] == '$'){
        strcpy(portname,port);
        port_f = true;
    }


}

MPI_Client::~MPI_Client() {
    //stop(recv_f);
    //if(!recv_f)
    //    stop();
}

int MPI_Client::initialize() {
    cout << "--------------------Client init start--------------------" << endl;
    cout << "[Client]: client initail..." << endl;
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];

    int provide;
    MPI_Init_thread(0,0, MPI_THREAD_MULTIPLE, &provide);
	MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
    cout << "[Client_"<< myrank <<"]: support thread level= " << provide << endl;
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    //MPI_Comm_create_errhandler(MPI_Client::errhandler, &eh);
    // read port from file

    ifstream in;
	if(strlen(portfile) <= 1){
		strcpy(portfile,"port.txt");	
	}
    cout << "[Client] Find port file in path = " << portfile << endl; 
	in.open(portfile,ios::in);
    if(in.is_open()) {
        in.getline(portname, MPI_MAX_PORT_NAME);
        in.close();
        port_f = true;
    }
    else{
        cout << "[Client-Error] Open port file error, lookup for server port" << endl;
    }

    int attemp = 0;
    while(!port_f) {
		attemp+=1;
        cout << "[Client_" << myrank << "]: finding service name <" << svc_name_ << "> ..." << endl;
        merr = MPI_Lookup_name(svc_name_, MPI_INFO_NULL, portname);
		cout << "port = " << portname << endl;
        if (merr) {
            MPI_Error_string(merr, errmsg, &msglen);
            cout << "[Client-" << myrank << "-error]: Lookup service name error, msg: " << errmsg << endl;
			if(attemp < 10)
            	continue;
			else
				return MPI_ERR_CODE::LOOKUP_SVC_ERR;
            //TODO Add error handle
        }

        // check port is in right format
        int port_len = strlen(portname);
        while(portname[port_len-1] != '$'){
            portname[port_len-1] = '\0';
            port_len-=1;
        }
        if (port_len < 24 && attemp <= 5)
            continue;
        else
            port_f = true;
    }
    cout << "[Client_" << myrank << "]: service found on port:<" << portname << ">" << endl;
	//check portname format
	if(strlen(portname) < 24 || portname[strlen(portname)-1] != '$'){
		cout << "[Client_" << myrank << "]: server port error:<" << portname << ">; exit" << endl;
		return MPI_ERR_CODE::LOOKUP_SVC_ERR;
	}

    merr = MPI_Comm_connect(portname, MPI_INFO_NULL,0, MPI_COMM_SELF, &sc_comm_);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Client-"<<myrank<<"-error]: Connect to Server error, msg: " << errmsg << endl;
        return MPI_ERR_CODE::CONNECT_ERR;
        //TODO Add error handle
    }
    cout << "[Client_"<< myrank <<"]: client connect to server, comm = " << sc_comm_ << endl;
    //MPI_Barrier(sc_comm_);

    //set error handler
    //cout << "[Client_"<< myrank <<"]: client set error handler"<< endl;
    //MPI_Comm_set_errhandler(sc_comm_,eh);

    pthread_create(&recv_t, NULL, MPI_Base::recv_thread, this);
    while(true){
		pthread_mutex_lock(&recv_flag_mutex);
		if(!recv_f){
			pthread_mutex_unlock(&recv_flag_mutex);
			break;
		}
		pthread_mutex_unlock(&recv_flag_mutex);
	}
    cout << "[Client_"<< myrank <<"]: recv thread start...." << endl;
    cout << "--------------------Client "<< myrank <<" init finish--------------------" << endl;


    return MPI_ERR_CODE::SUCCESS;
}

int MPI_Client::stop(bool flag) {
    cout << "--------------------stop Client "<< myrank<<"--------------------" << endl;
	if(flag)
		MPI_Finalize();
	else{
    //cout << "[Client_"<< myrank <<"]: stop Client..." << endl;
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];
    set_recv_stop();
    // add disconnect send
    char* tmp = (char *) uuid_.data();
    if(send_string(tmp, strlen(tmp), 0, MPI_DISCONNECT) == MPI_ERR_CODE::SUCCESS)
        cout <<"[Client_"<< myrank <<"]: send complete..." << endl;
    //MPI_Barrier(sc_comm_);
	
	// wait for recv join
	int ret = pthread_join(recv_t, NULL);
    cout <<"[Client_"<< myrank <<"]: recv thread stop, exit code=" << ret << endl;

    merr = MPI_Comm_disconnect(&sc_comm_);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Client-"<<myrank<<"-Error]: disconnect error :" << errmsg << endl;
        return MPI_ERR_CODE::DISCONN_ERR;
    }
    MPI_Barrier(sc_comm_);
    cout << "[Client_"<< myrank <<"]: disconnected..." << endl;
    finalize();
	}
    cout << "--------------------Client "<< myrank <<" stop finish--------------------" << endl;
    return MPI_ERR_CODE::SUCCESS;
}

int MPI_Client::finalize() {
    int ret;
    //ret = pthread_join(recv_t, NULL);
    //MPI_Errhandler_free(&eh);
    //cout <<"[Client_"<< myrank <<"]: recv thread stop, exit code=" << ret << endl;
    MPI_Finalize();
    return 0;
}

bool MPI_Client::new_msg_come(ARGS *args) {
    if(sc_comm_ == 0x0)
        return false;
    int merr = 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];

    MPI_Status *stat = new MPI_Status;
    int flag = 0;
    merr = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, sc_comm_, &flag, stat);
    if (merr) {
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Client-Error]: Iprobe message error :" << errmsg << endl;
        delete(stat);
        return false;
    }
    if (flag) {
#ifdef DEBUG
        cout << "[Client_"<< myrank <<"]: dectect a new msg <source=" << stat->MPI_SOURCE << ";tag=" << stat->MPI_TAG << ">" <<endl;
#endif
        args->arg_stat = *stat;
        args->datatype = MPI_CHAR;
        args->source_rank = stat->MPI_SOURCE;
        args->newcomm = sc_comm_;
        flag = 0;
        delete (stat);
        return true;
    } else {
        delete(stat);
        return false;
    }
}

int MPI_Client::send_string(char* buf, int msgsize, int dest, int tag){
#ifdef DEBUG
    cout << "[Client_"<< myrank <<"]: send message...<" << buf << ",msgsize="<< msgsize <<",dest="<<dest <<",tag=" <<tag  << ">"<< endl;
#endif
    int merr = 0;
    int msglen = msgsize;
    char errmsg[MPI_MAX_ERROR_STRING];
    merr = MPI_Send(buf, msgsize, MPI_CHAR, 0,  tag, sc_comm_);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Client-Error]: send fail...error: " << errmsg << endl;
        return MPI_ERR_CODE::SEND_FAIL;
    }
#ifdef DEBUG
    cout << "[Client_"<< myrank <<"]: start send barrier..." << endl;
#endif
    merr = MPI_Barrier(sc_comm_);
#ifdef DEBUG
	cout << "[Client_"<< myrank <<"]: end start  barrier..." << endl;	
#endif
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Client-Error]: barrier fail...error: " << errmsg << endl;
        return MPI_ERR_CODE::BARRIER_FAIL;
    }
#ifdef DEBUG
    cout << "[Client_"<< myrank <<"]: end barrier..." << endl;
#endif
    return MPI_ERR_CODE::SUCCESS;
}

void MPI_Client::recv_handle(ARGS args, void *buf) {
    int merr, msglen;
    char errmsg[MPI_MAX_ERROR_STRING];
    switch (args.arg_stat.MPI_TAG){
        case MPI_DISCONNECT:{
            if(args.newcomm != sc_comm_) {
#ifdef DEBUG
                cout << "[Client-"<<myrank<<"-Error]: disconnect error: MPI_Comm is not matching" << endl;
#endif
                //TODO error handle
            }
            merr = MPI_Comm_disconnect(&sc_comm_);
            if(merr){
                MPI_Error_string(merr, errmsg, &msglen);
                cout << "[Client-"<<myrank<<"-Error]: disconnect error: " << errmsg << endl;
                //TODO Add error handle
            }
            MPI_Barrier(sc_comm_);
        }
            break;
        default:
            //cout << "[Client-Error]: Unrecognized type" << endl;
            break;
    }
}

int MPI_Client::exit(){
	MPI_Finalize();
	return 0;
}

void MPI_Client::errhandler(MPI_Comm *comm, int *errcode,...) {
    int reslen;
    char errstr[MPI_MAX_ERROR_STRING];
    if(*errcode != MPI_ERR_OTHER) {
        MPI_Error_string(*errcode, errstr, &reslen);
        Pack pack = Pack(-1, 0);
        pack.sbuf_ = errstr;
        rv_buf->put(pack);
    }
}
