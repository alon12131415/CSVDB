DIRECTORY=`dirname $0`
g++ -c -fPIC $DIRECTORY/*.cpp -m64
g++ *.o -shared -o $DIRECTORY/../column.so
rm *.o
