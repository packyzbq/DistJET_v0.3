//
// Created by zhaobq on 2017-4-25.
//

#include <boost/python.hpp>
#include "MPI_Client.cpp"
#include "MPI_Base.cpp"
#include "Include/IRecv_buffer.h"

using namespace boost::python;

BOOST_PYTHON_MODULE(Client_Module)
{
        class_<MPI_Client>("MPI_Client", init<IRecv_buffer*, char* ,char*>())
                //.def(init<IRecv_buffer*, char*, char*, char*>())
                .def("initialize", &MPI_Client::initialize)
                //.def("run", &MPI_Client::run)
				.def("set_portfile", &MPI_Client::set_portfile)
                .def("stop", &MPI_Client::stop)
                .def("finalize", &MPI_Client::finalize)
                .def("send_string", &MPI_Client::send_string)
				.def("exit", &MPI_Client::exit)
                //.def("send_int", &MPI_Client::send_int)
        ;
}
