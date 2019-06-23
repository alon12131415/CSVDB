TABLE_VALUES_CPP := $(wildcard csvdbdll/tableValues/*.cpp)
TABLE_VALUES_HPP := $(wildcard csvdbdll/tableValues/*.cpp)

CPP_SRC := $(TABLE_VALUES_CPP) $(wildcard csvdbdll/*.cpp)
HDRS := $(TABLE_VALUES_HPP) $(wildcard csvdbdll/*.hpp)
OBJ := $(patsubst %.cpp,%.o,$(CPP_SRC))

UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
TARGET = csvdb.so
else
TARGET = csvdb.dll
endif


all: $(TARGET)

$(TARGET): $(OBJ) $(HDRS)
	g++ -fopenmp -D_GLIBCXX_PARALLEL -std=c++17 -o3 -shared  $(OBJ) -o $@

csvdbdll/%.o: csvdbdll/%.cpp $(HDRS)
	g++ -fopenmp -D_GLIBCXX_PARALLEL -std=c++17 -o3 -c -fPIC -m64  $< -o $@

clean:
	rm -f $(OBJ) $(TARGET) *~
