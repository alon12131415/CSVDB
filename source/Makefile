CPP_SRC:= $(wildcard csvdbdll/*.cpp)
HDRS:=$(wildcard csvdbdll/*.hpp)
OBJ:= $(patsubst %.cpp,%.o,$(CPP_SRC))

all: csvdb.dll

csvdb.dll: $(OBJ) $(HDRS)
	g++ -shared -std=c++17 $(OBJ) -o $@

csvdbdll/%.o: csvdbdll/%.cpp $(HDRS)
	g++ -c -fPIC -m64 -Wall -std=c++17 $< -o $@

clean:
	rm -f $(OBJ) csvdb.dll *~
