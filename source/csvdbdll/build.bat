@echo off
echo Starting to build
cls
g++ -c -fPIC %~dp0\*.cpp -m64
g++ *.o -shared -o %~dp0\..\column.dll
del *.o
