all: myavg

myavg:myavg.cpp
	g++ -D HAVE_LONG_INT_64  -I /opt/vertica/sdk/include -Wall -shared -Wno-unused-value -fPIC -o myavg.so myavg.cpp /opt/vertica/sdk/include/Vertica.cpp