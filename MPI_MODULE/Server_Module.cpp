//
// Created by zhaobq on 2017-4-25.
//

#include <boost/python.hpp>
#include "MPI_Server.cpp"
#include "MPI_Base.cpp"
#include "Include/IRecv_buffer.h"
using namespace boost::python;

BOOST_PYTHON_MODULE(Server_Module)
{
        class_<MPI_Server>("MPI_Server", init<IRecv_buffer*, char*>())
                //.def("run", &MPI_Server::run)
                .def("initialize", &MPI_Server::initialize)
                .def("stop", &MPI_Server::stop)
                .def("finalize", &MPI_Server::finalize)
                //.def("recv_handle", &MPI_Server::recv_handle)
                //.def("send_int", &MPI_Server::send_int)
                .def("send_string", &MPI_Server::send_string)
                //.def("disconnect_client", &MPI_Server::disconnect_client)
                //.def("bcast", &MPI_Server::bcast)
                .def("get_Commlist_size", &MPI_Server::get_Commlist_size)
                .def("get_stop_permit",&MPI_Server::get_stop_permit)
                .def("print_Commlist", &MPI_Server::print_Commlist)
				.def("set_portfile", &MPI_Server::set_portfile)
                ;

}
