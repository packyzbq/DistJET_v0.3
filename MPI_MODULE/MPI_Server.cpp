//
// Created by zhaobq on 2017-4-24.
//

#include "Include/MPI_Server.h"
#include "Include/ErrorHandler.h"
#include <fstream>
#include "map"
#include <string>
#include <signal.h>
#include <sys/dir.h>
#include <unistd.h>

//#define DEBUG

MPI_Server::MPI_Server(IRecv_buffer* rh, char *svc_name) : MPI_Base(rh) {
    svc_name_ = svc_name;
    recv_flag_mutex = PTHREAD_MUTEX_INITIALIZER;
    accept_flag_mutex = PTHREAD_MUTEX_INITIALIZER;
    comm_list_mutex = PTHREAD_MUTEX_INITIALIZER;
	dummy_f_mutex = PTHREAD_MUTEX_INITIALIZER;
};

MPI_Server::~MPI_Server() {
    if(!recv_f && !accept_f)
        stop();
    cout << "[Server] end..." << endl;
}

int MPI_Server::initialize() {
    cout << "--------------------Server init start-------------------" << endl;
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];

    int provided;
    MPI_Init_thread(0,0,MPI_THREAD_MULTIPLE, &provided);
    cout << "[Server]: support thread level= " << provided << endl;
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Get_processor_name(hostname, &msglen);
    //TODO set self costume handler, here set MPI_ERRORS_RETURN in temp
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    //MPI_Comm_create_errhandler(MPI_Server::errhandler, &eh);
    cout << "[Server]: Host: " << hostname << ",Proc: "<< myrank << ", Server initialize..." << endl;
    merr = MPI_Open_port(MPI_INFO_NULL, port);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server]: Error in Open port :" << errmsg<<endl;
        return MPI_ERR_CODE::OPEN_PORT_ERR;
    }

    cout << "[Server]: Host: " << hostname << ",Proc: "<< myrank << ",Server opening port on <" << port <<">" << endl;
    // write port into files
#ifdef DEBUG
    cout << "[Server]: Open file " << port_file <<" , write port : " << port << endl;
#endif
    if(strlen(port_file) <= 1){
        getcwd(port_file,1024);
        sprintf(port_file,"%s/port.txt",port_file);
	}
    ofstream out(port_file);
    if(out.is_open()) {
        out << port;
        out.close();
        cout << "[Server]: Write port into file finished, path = " << port_file <<  endl;
    }
    else{
        cout << "[Server-Error]: Write port error" << endl;
    }

    merr = MPI_Publish_name(svc_name_, MPI_INFO_NULL, port);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server]: Error in publish_name :" << errmsg<<endl;
        return MPI_ERR_CODE::PUBLISH_SVC_ERR;
    }
    cout << "[Server]: publish service <" << svc_name_ << ">" << endl;

    //start recv thread
    pthread_create(&recv_t ,NULL, MPI_Base::recv_thread, this);
    while(true){
		pthread_mutex_lock(&recv_flag_mutex);
		if(!recv_f){
			pthread_mutex_unlock(&recv_flag_mutex);
			break;
		}
		pthread_mutex_unlock(&recv_flag_mutex);
	}
    cout << "[Server]: receive thread start..." << endl;

    //start accept thread
    pthread_create(&pth_accept, NULL, MPI_Server::accept_conn_thread, this);
    while(true){
		pthread_mutex_lock(&accept_flag_mutex);
		if(!accept_f){
			pthread_mutex_unlock(&accept_flag_mutex);
			break;
		}
		pthread_mutex_unlock(&accept_flag_mutex);
	}
    cout << "[Server]: accept thread start..." << endl;
    cout << "--------------------Server init finish--------------------" << endl;
    return MPI_ERR_CODE::SUCCESS;
}

int MPI_Server::stop() {
    cout << "--------------------Server stop start--------------------"<< endl;
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];
    cout << "[Server]: Ready to stop..." << endl;
    cout << "[Server]: Unpublish service name..." << endl;
    merr = MPI_Unpublish_name(svc_name_, MPI_INFO_NULL, port);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server]: Unpublish service name error: "<< errmsg << endl;
        return MPI_ERR_CODE::UNPUBLISH_ERR;
    }

    if(!comm_map.empty()){
        cout <<"[Server]: client still working ,cannot stop server..." << endl;
		cout << "[Server]: Running client:" << endl;
		print_Commlist();
        //return MPI_ERR_CODE::STOP_ERR;
    }
    // TODO Add force stop
    //stop threads
    set_recv_stop();
    if(finalize() != MPI_ERR_CODE::SUCCESS){
        //TODO Do something for finalize error
        return MPI_ERR_CODE::JOIN_THREAD_ERROR;
    }

    //delete port file
    if(remove(port_file) != 0)
        cout << "[Server-Error]: Remove port tmp file fail" << endl;

    cout << "--------------------Server stop finish--------------------"  << endl;

    return MPI_ERR_CODE::SUCCESS;
}

int MPI_Server::finalize() {
    // set up a dummy connection to jump out of accept loop
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];
    MPI_Comm tmp_comm;
	pthread_mutex_lock(&dummy_f_mutex);
    dummy_conn = true;
	pthread_mutex_unlock(&dummy_f_mutex);
    merr = MPI_Comm_connect(port, MPI_INFO_NULL, 0, MPI_COMM_SELF, &tmp_comm);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server-ERR]: Setup dummy connection error, msg: " << errmsg << endl;
        return MPI_ERR_CODE::CONNECT_ERR;
    }
	cout << "tmp_comm = " << tmp_comm << endl;
    //MPI_Barrier(tmp_comm);
    //merr = MPI_Comm_disconnect(&tmp_comm);
	//if(merr){
	//	MPI_Error_string(merr, errmsg, &msglen);
    //    cout << "[Server-ERR]: Disconnect dummy connection error, msg: " << errmsg << endl;
    //    return MPI_ERR_CODE::DISCONN_ERR;
	//}
#ifdef DEBUG
    cout << "[Server]: Disconnect dummy connection, wait for accept thread stop" << endl;
#endif
	//MPI_Barrier(tmp_comm);
    int ret;
    ret = pthread_join(pth_accept, NULL);
#ifdef DEBUG
    cout << "[Server]: accept thread stop, exit code=" << ret << endl;
#endif
    if(ret != 0){
        cout << "[Server-ERR]: Join **accept** thread error, code = " << ret << endl;
        //TODO Do something
        return MPI_ERR_CODE::JOIN_THREAD_ERROR;
    }
    ret = pthread_join(recv_t, NULL);
#ifdef DEBUG
    cout << "[Server]: recv_thread stop, exit code=" << ret << endl;
#endif
    if(ret != 0){
        cout << "[Server-ERR]: Join **receive** thread error, code = " << ret << endl;
        //TODO Do something
        return MPI_ERR_CODE::JOIN_THREAD_ERROR;
    }
    //MPI_Errhandler_free(&eh);
    MPI_Finalize();
    return MPI_ERR_CODE::SUCCESS;
}

bool MPI_Server::new_msg_come(ARGS *args) {
    //FIXME Can not fairly detect the last-half queue
    if(comm_map.empty())
        return false;
	int merr=0;
	int msglen = 0;
	char errmsg[MPI_MAX_ERROR_STRING];
    MPI_Status stat;
	int flag;
    map<string, MPI_Comm>::iterator iter;
    for(iter = comm_map.begin(); iter != comm_map.end(); iter++){
		if(iter->second == 0x0){
			//cout << "[Server-ERR]: Invalid communicator" << endl;
			return false;
		}
		merr = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, iter->second, &flag, &stat);
		if(merr){
			MPI_Error_string(merr, errmsg, &msglen);
			cout << "[Server-Error]: " << errmsg << endl;
			//TODO Add error handle
		}
        if(flag) {
#ifdef DEBUG
            cout << "[Server]: dectect a new msg <source=" << stat.MPI_SOURCE << ";tag=" << stat.MPI_TAG << ">" <<endl;
#endif
            args->newcomm = (iter->second);
            args->arg_stat = stat;
            args->datatype = MPI_CHAR;
            args->source_rank = stat.MPI_SOURCE;
#ifdef DEBUG
            args->print();
#endif
            return true;
        }
    }
    return false;
}

void* MPI_Server::accept_conn_thread(void *ptr) {
    //pthread_t mypid = pthread_self();
    int merr= 0;
    int msglen = 0;
    char errmsg[MPI_MAX_ERROR_STRING];
    pthread_mutex_lock(&(((MPI_Server*)ptr)->accept_flag_mutex));
    ((MPI_Server*)ptr)->accept_f = false;
    pthread_mutex_unlock(&(((MPI_Server*)ptr)->accept_flag_mutex));
    cout << "[Server] host: "<< ((MPI_Server*)ptr)->hostname <<", accept connection thread start..." << endl;
    int tmpkey = 0;
	MPI_Comm newcomm;
    while(!((MPI_Server*)ptr)->accept_f) {
        //MPI_Comm newcomm;
        merr = MPI_Comm_accept(((MPI_Server*)ptr)->port, MPI_INFO_NULL, 0, MPI_COMM_SELF, &newcomm);
        if(merr){
            MPI_Error_string(merr, errmsg, &msglen);
            cout << "[Server-Error]: accept client error, msg: " << errmsg << endl;
        }
		#ifdef DEBUG
		cout << "[Server]: Add a new communicator=" << newcomm << endl;
		#endif
        //MPI_Barrier(newcomm);
		pthread_mutex_lock(&(((MPI_Server*)ptr)->dummy_f_mutex));
        if(((MPI_Server*)ptr)->dummy_conn){
		#ifdef DEBUG
		cout << "[Server]: Accept a dummy connection, ready to stop" << endl;
		#endif
            //merr = MPI_Comm_disconnect(&newcomm);
            //if(merr){
            //    MPI_Error_string(merr, errmsg, &msglen);
            //    cout << "[Server-ERR]: <accept-thread> Disconnect dummy connection error, msg: " << errmsg << endl;
            //}
			pthread_mutex_unlock(&(((MPI_Server*)ptr)->dummy_f_mutex));
            break;
        }
		pthread_mutex_unlock(&(((MPI_Server*)ptr)->dummy_f_mutex));
        pthread_mutex_lock(&(((MPI_Server*)ptr)->comm_list_mutex));
        ((MPI_Server*)ptr)->allowstop = true;
        while(((MPI_Server*)ptr)->comm_map[""+tmpkey] != NULL){
            tmpkey++;
        }
        //((MPI_Server*)ptr)->comm_map.insert(pair<string, MPI_Comm>(""+tmpkey,newcomm));
		((MPI_Server*)ptr)->comm_map[""+tmpkey] = newcomm;
		//cout << "comm = " << newcomm << endl;
		tmpkey++;
        pthread_mutex_unlock(&(((MPI_Server*)ptr)->comm_list_mutex));
        //add comm errhandler
        //MPI_Comm_set_errhandler(newcomm,(MPI_Errhandler)&eh);


#ifdef DEBUG
        cout << "[Server]:Host: " << ((MPI_Server*)ptr)->hostname << ", Proc: "<< ((MPI_Server*)ptr)->myrank << ", receive new connection...; MPI_COMM="<< newcomm << endl;
#endif


    }
    cout << "[Server] host: "<< ((MPI_Server*)ptr)->hostname << ", accept connection thread stop..." << endl;
    return 0;
}

void MPI_Server::recv_handle(ARGS args, void* buf) {
    //TODO set different conditions
    int merr, msglen;
#ifdef DEBUG
    cout << "[Server-DEBUG]: ARGS = " << endl;
    args.print();
#endif
    string msg_tmp = (char *)buf;
    string msg = msg_tmp.substr(0, (unsigned int) args.arg_stat.count_lo);
    char errmsg[MPI_MAX_ERROR_STRING];
    map<string, MPI_Comm>::iterator iter;
    switch(args.arg_stat.MPI_TAG){
        case MPI_REGISTER: {
#ifdef DEBUG
            cout << "get a registery from worker:" << msg << endl;
#endif
            if(comm_map.size() == 0)
                cout << "[Server-Error]: comm_list has no MPI_Comm" << endl;
            int size = 0;
            pthread_mutex_lock(&comm_list_mutex);
            //modify comm_map
			size_t pos = msg.find("\"uuid\":");
			string uuid = "";
			if(pos != -1)
            	uuid = msg.substr(pos+9,36);
			else
				uuid = msg;
            for(iter = comm_map.begin(); iter != comm_map.end(); iter++){
                if(iter->second == args.newcomm && comm_map[uuid] == NULL) {
                    comm_map[uuid] = iter->second;
                    comm_map.erase(iter->first);

#ifdef DEBUG
                    cout << "[Server]: register worker " << uuid << " success" << endl;
#endif
                    break;
                }
				else if(comm_map[uuid] != NULL){
               	cout << "[Server-ERROR]: This uuid=" << uuid << "has already registered" << endl;
            	}

            }

            if(iter == comm_map.end()){
                cout << "[Server-Error]: register error, no compatible MPI_COMM" << endl;
                //TODO Add error handle
            }
            pthread_mutex_unlock(&comm_list_mutex);
		    //print_Commlist();
        }
            break;
        case MPI_DISCONNECT:{
            size_t pos = msg.find("\"uuid\":");
			string uuid = "";
			if(pos != -1)
            	uuid = msg.substr(pos+9,36);
			else
				uuid = msg;
            cout << "[Server] worker :" << uuid<< " require disconnect" << endl;
            pthread_mutex_lock(&comm_list_mutex);
            if(comm_map[uuid] != NULL){
                if(comm_map[uuid] == args.newcomm) {
                    merr = MPI_Comm_disconnect(&comm_map[uuid]);
                    if (merr) {
                        MPI_Error_string(merr, errmsg, &msglen);
                        cout << "[Server-Error]: disconnect error: " << errmsg << endl;
                    }
                    MPI_Barrier(comm_map[uuid]);
                    comm_map.erase(uuid);
#ifdef DEBUG
                    cout << "[Server]: find MPI_Comm and wid, removing worker..., commlist size = " << comm_map.size() << endl;
#endif
                }
                else{
                    cout << "[Server-ERR]: MPI_Comm can not match with MPI_Comm: <stored:" << comm_map[msg] << "> | <your:" << args.newcomm <<">" <<endl;
                }
            }
            else{
                // not found
#ifdef DEBUG
                cout << "[Server-Error]: can't find correspond MPI_Comm and wid" << endl;
                cout << "Here is the comm list" << endl;
                for(iter = comm_map.begin(); iter != comm_map.end(); iter++){
                    cout << iter->first <<" :: " << iter->second << endl;
                }
#endif
            }
            pthread_mutex_unlock(&comm_list_mutex);
        }
            break;
        default: {
            //cout << "[Server-Error]: unrecorgnized type" << endl;
            break;
        }
    }
}

int MPI_Server::send_string(char *buf, int msgsize, string dest_uuid, int tag) {
    double starttime,endtime;
#ifdef DEBUG
    cout << "[Server]: send message...<" << buf << ",msgsize="<< msgsize <<",dest="<<dest_uuid <<",tag=" <<tag  << ">"<< endl;
#endif
    int merr = 0;
    int msglen = msgsize;
    char errmsg[MPI_MAX_ERROR_STRING];
    MPI_Comm send_comm = comm_map[dest_uuid];
    if(send_comm == NULL) {
#ifdef  DEBUG
        cout << "[Server-Error]: can't find send comm" << endl;
#endif
        //TODO add error handler
		return MPI_ERR_CODE::SEND_FAIL;
    }
    starttime = MPI_Wtime();
    merr = MPI_Send(buf, msgsize, MPI_CHAR, 0, tag, send_comm);
    endtime = MPI_Wtime();
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server-Error]: send fail...error: " << errmsg << endl;
        return MPI_ERR_CODE::SEND_FAIL;
    }
#ifdef DEBUG
    cout << "[Server]: start barrier..." << endl;
#endif
    merr = MPI_Barrier(send_comm);
    if(merr){
        MPI_Error_string(merr, errmsg, &msglen);
        cout << "[Server-Error]: barrier fail...error: " << errmsg << endl;
        return MPI_ERR_CODE::BARRIER_FAIL;
    }
#ifdef DEBUG
    cout << "[Server]: end barrier..." << endl;
#endif
    return MPI_ERR_CODE::SUCCESS;
}

void MPI_Server::errhandler(MPI_Comm *comm, int *errcode, ...) {
    int reslen;
    char errstr[MPI_MAX_ERROR_STRING];
    if(*errcode != MPI_ERR_OTHER) {
        MPI_Error_string(*errcode, errstr, &reslen);
        Pack pack = Pack(-1, 0);
        string st = "";
        for(map<string, MPI_Comm>::iterator i = comm_map.begin(); i != comm_map.end();i++){
            if(i->second == *comm) {
                st += i->first;
                break;
            }
        }
        st =st+ " "+ errstr;
        pack.sbuf_=st;
        rv_buf->put(pack);
    }
}
