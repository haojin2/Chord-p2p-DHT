EXENAME = chord
OBJS = main.o message.o node.o

CXX = clang++
CXXFLAGS = -std=c++1y -stdlib=libc++ -c -g -O0 -Wall -Wextra -pedantic
LD = clang++
LDFLAGS = -std=c++1y -stdlib=libc++ -lc++abi

all : $(EXENAME)

$(EXENAME) : $(OBJS)
	$(LD) $(OBJS) $(LDFLAGS) -o $(EXENAME)

main.o : main.cpp message.cpp node.cpp
	$(CXX) $(CXXFLAGS) main.cpp

node.o : node.cpp
	$(CXX) $(CXXFLAGS) node.cpp

message.o : message.cpp
	$(CXX) $(CXXFLAGS) message.cpp

clean :
	-rm -f *.o $(EXENAME)
