Team Members
=
 Alon Sukonik, Guy Shefy

Project Structure
=
## Code Structure
The project is split into two parts, the python part and the c++ part, the python part is responsible for parsing, setup and execution of all commands except the select command.
The c++ part is responsible for performing the select command itself and thus is called only by Table.select.
When the program receives a command it parses it, and the parser returns the function responsible for execution of the command and the arguments it should receive.
The source/main.py file can receive multiple parameters from the commandline:
--rootdir - determines the directory where all of the tables will be stored at. default: "../"
--run - receives a file and executes all commands in that file.
--verbose - prints information useful for debugging and time testing.
--unit - applies all unit tests that have been written to the project using varying file sizes to make sure it always works.
--test - applies unit tests in operational scenario. mainly used to time big tests.
--filesizes - sets the file sizes of the inner files of the table, mainly used when handling files that do not fit in the ram. default: 2**31 - 1
--ascii - prints and ascii art indicating a success or a failure of --unit.
## File Structure
Tables are stored inside rootdir/table_name and have a table.json file which stores various information about the table itself.
Each column is stored separated into files with each file holding [filesizes] amount of lines and each file is called "{file_index}.ga". During select if an order is needed we first sort each file index individually and store the output as a "tmp{file_index}" in csv format (not column separated anymore) and then all the files are merged and stored back into files until one file is left and it is renamed to the requested output file.
Each type is stored differently:
Int - binary packed. Prepended by a NullByte to indicate is not null, if is null, prepended by a "\x01".
Timestamp - binary packed (unsigned). Prepended by a NullByte to indicate is not null, if is null, prepended by a "\x01".
Float - Binary packed. Null is stored as a negative zero.
Varchar - NullByte separated. No null for varchars.

