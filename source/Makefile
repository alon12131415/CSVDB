CPP_SRC:= $(wildcard csvdbdll/*.cpp)
HDRS:=$(wildcard csvdbdll/*.hpp)
OBJ:= $(patsubst %.cpp,%.o,$(CPP_SRC))

all: csvdb.so

csvdb.so: $(OBJ) $(HDRS)
	g++ -shared $(OBJ) -o $@

csvdbdll/%.o: csvdbdll/%.cpp $(HDRS)
	g++ -c -fPIC -m64 $< -o $@

clean:
	rm -f $(OBJ) csvdb.so *~
