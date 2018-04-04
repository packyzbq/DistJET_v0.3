//
// Created by zhaobq on 2017-4-24.
//

#ifndef MPI_CONNECT_ERRORHANDLER_H
#define MPI_CONNECT_ERRORHANDLER_H

class MPI_ERR_CODE{
public:
    const static int SUCCESS                = 0;
    const static int OPEN_PORT_ERR          = 1;
    const static int PUBLISH_SVC_ERR        = 2;
    const static int UNPUBLISH_ERR          = 3;
    const static int DISCONN_ERR            = 4;
    const static int STOP_ERR               = 5;
    const static int LOOKUP_SVC_ERR         = 6;
    const static int CONNECT_ERR            = 7;
    const static int SEND_FAIL              = 8;
    const static int RECV_FAIL              = 9;
    const static int BARRIER_FAIL           =10;
    const static int JOIN_THREAD_ERROR      =11;

    const static int UNEXPECT_ERR           = 100;
};
#endif //MPI_CONNECT_ERRORHANDLER_H
