import consts
import ctypes
import sys
import os

if sys.platform == 'win32':
	csvdbLib = ctypes.cdll.LoadLibrary(os.path.join(consts.SOURCE_DIR, "csvdb.dll"))
else:
	csvdbLib = ctypes.cdll.LoadLibrary(os.path.join(consts.SOURCE_DIR, "csvdb.so"))

csvdbLib.Column_new.argstypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int, ctypes.c_int]
csvdbLib.Column_new.restype = ctypes.c_void_p

csvdbLib.Column_setFP.argstypes = [ctypes.c_void_p, ctypes.c_int]
csvdbLib.Column_setFP.restype = None

csvdbLib.Column_setOp.argstypes = [ctypes.c_void_p, ctypes.c_int]
csvdbLib.Column_setOp.restype = None

csvdbLib.Column_setWhereConst.argstypes = [ctypes.c_void_p, ctypes.c_void_p]
csvdbLib.Column_setWhereConst.restype = None

csvdbLib.Column_getRow.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getRow.restype = None

csvdbLib.Column_getIntVal.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getIntVal.restype = ctypes.c_int64

csvdbLib.Column_getTimestampVal.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getTimestampVal.restype = ctypes.c_uint64

csvdbLib.Column_getVarcharVal.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getVarcharVal.restype = ctypes.c_void_p#not c_char_p because c_char_p will be converted to string, and we will not be able to pass it to deleteVarChar.

csvdbLib.DeleteVarchar.argstypes = [ctypes.c_char_p]
csvdbLib.DeleteVarchar.restype = None

csvdbLib.Column_getFloatVal.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getFloatVal.restype = ctypes.c_double

csvdbLib.Column_getFinished.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getFinished.restype = ctypes.c_bool

csvdbLib.Column_getPassedTheWhere.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getPassedTheWhere.restype = ctypes.c_bool

csvdbLib.Column_getIsNull.argstypes = [ctypes.c_void_p]
csvdbLib.Column_getIsNull.restype = ctypes.c_bool

csvdbLib.Column_delete.argstypes = [ctypes.c_void_p]
csvdbLib.Column_delete.restype = None

csvdbLib.CreateIntWhereConst.argstypes = [ctypes.c_int64]
csvdbLib.CreateIntWhereConst.restype = ctypes.c_void_p

csvdbLib.CreateTimestampWhereConst.argstypes = [ctypes.c_uint64]
csvdbLib.CreateTimestampWhereConst.restype = ctypes.c_void_p

csvdbLib.CreateVarcharWhereConst.argstypes = [ctypes.c_char_p]
csvdbLib.CreateVarcharWhereConst.restype = ctypes.c_void_p

csvdbLib.CreateFloatWhereConst.argstypes = [ctypes.c_double]
csvdbLib.CreateFloatWhereConst.restype = ctypes.c_void_p

csvdbLib.Table_Create.argstypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_int), \
									ctypes.c_int, ctypes.POINTER(ctypes.c_int), \
									ctypes.c_int, ctypes.c_int, ctypes.c_void_p,
									ctypes.c_int, ctypes.c_int,
									ctypes.c_int, ctypes.POINTER(ctypes.c_int),
									ctypes.POINTER(ctypes.c_int), ctypes.c_void_p]
csvdbLib.Table_Create.restype = ctypes.c_void_p

csvdbLib.Table_select.argstypes = [ctypes.c_void_p, ctypes.c_char_p]
csvdbLib.Table_select.restype = None

csvdbLib.Table_delete.argstypes = [ctypes.c_void_p]
csvdbLib.Table_delete.restype = None
