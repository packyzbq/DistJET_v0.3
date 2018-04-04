#!/bin/bash

mpic++ -g -c -o MPI_Client.o MPI_Client.cpp -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -c -o MPI_Base.o MPI_Base.cpp -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -c -o MPI_Server.o MPI_Server.cpp -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -c -o ServerTest.o ServerTest.cpp -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -o bin/server MPI_Client.o MPI_Base.o MPI_Server.o ServerTest.o -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -c -o ClientTest.o ClientTest.cpp -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread

mpic++ -g -o bin/client MPI_Client.o MPI_Base.o MPI_Server.o ClientTest.o -gstabs+ -std=c++11 -I/usr/include/mpich2-x86_64 -lpthread -luuid
