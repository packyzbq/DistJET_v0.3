//
// Created by zhaobq on 2017-4-25.
//

#include <boost/python.hpp>
#include "Include/IRecv_buffer.h"

using namespace boost::python;

BOOST_PYTHON_MODULE(IR_Buffer_Module){
        class_<Pack>("Pack", init<int, int>())
                .def_readonly("tag", &Pack::tag_)
                .def_readonly("ibuf", &Pack::ibuf_)
                .def_readonly("sbuf", &Pack::sbuf_)
                .def_readonly("size", &Pack::size_)
        ;

        class_<IRecv_buffer>("IRecv_buffer")
            .def("get", &IRecv_buffer::get)
            .def("put", &IRecv_buffer::put)
            .def("empty", &IRecv_buffer::empty)
			.def("size", &IRecv_buffer::size)
        ;
}
