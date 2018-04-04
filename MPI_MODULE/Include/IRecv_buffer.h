//
// Created by zhaobq on 2017-4-24.
//

#ifndef MPI_CONNECT_V2_IRECV_BUFFER_H
#define MPI_CONNECT_V2_IRECV_BUFFER_H

//#define DEBUG

#include <string>
#include <pthread.h>
#include <queue>
#include <cstdlib>
#include <iostream>
using namespace std;

struct Pack{
    int tag_;
    int ibuf_=0;
    string sbuf_="";
    int size_ = 0;

    Pack(int tag, int size):tag_(tag), size_(size){};
};

struct IRecv_buffer{
    queue<Pack> buffer;
    pthread_mutex_t mutex;

    IRecv_buffer(){
        mutex = PTHREAD_MUTEX_INITIALIZER;
    };

	int size(){
		return buffer.size();
	};

    void put(Pack p){
        pthread_mutex_lock(&mutex);
        buffer.push(p);
        pthread_mutex_unlock(&mutex);
#ifdef DEBUG
        std::cout << "<IRecv_buffer> Put pack into buffer" << std::endl;
#endif
    };

    Pack get(){
        pthread_mutex_lock(&mutex);
        if(buffer.empty()){
            pthread_mutex_unlock(&mutex);
            Pack p = Pack(-1,0);
            return p;
        }
        else {
            Pack p = buffer.front();
            buffer.pop();
            pthread_mutex_unlock(&mutex);
            return p;
        }
    };

    bool empty(){
        return buffer.empty();
    };
};

#endif //MPI_CONNECT_V2_IRECV_BUFFER_H
