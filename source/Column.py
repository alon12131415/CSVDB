from null import NULL
import consts
import os
import struct
import ctypes

columnLib = ctypes.cdll.LoadLibrary(os.path.join(consts.SOURCE_DIR, "column.so"))

columnLib.Column_new.argstypes = [ctypes.c_char_p, ctypes.c_int]
columnLib.Column_new.restype = ctypes.c_void_p

columnLib.Column_setFP.argstypes = [ctypes.c_void_p, ctypes.c_char_p]
columnLib.Column_setFP.restype = None

columnLib.Column_setOp.argstypes = [ctypes.c_void_p, ctypes.c_int]
columnLib.Column_setOp.restype = None

columnLib.Column_setWhereConst.argstypes = [ctypes.c_void_p, ctypes.c_void_p]
columnLib.Column_setWhereConst.restype = None

columnLib.Column_getRow.argstypes = [ctypes.c_void_p]
columnLib.Column_getRow.restype = None

columnLib.Column_getIntVal.argstypes = [ctypes.c_void_p]
columnLib.Column_getIntVal.restype = ctypes.c_int64

columnLib.Column_getTimestampVal.argstypes = [ctypes.c_void_p]
columnLib.Column_getTimestampVal.restype = ctypes.c_uint64

columnLib.Column_getVarcharVal.argstypes = [ctypes.c_void_p]
columnLib.Column_getVarcharVal.restype = ctypes.c_void_p#not c_char_p because c_char_p will be converted to string, and we will not be able to pass it to deleteVarChar. 

columnLib.DeleteVarchar.argstypes = [ctypes.c_char_p]
columnLib.DeleteVarchar.restype = None

columnLib.Column_getFloatVal.argstypes = [ctypes.c_void_p]
columnLib.Column_getFloatVal.restype = ctypes.c_double

columnLib.Column_getFinished.argstypes = [ctypes.c_void_p]
columnLib.Column_getFinished.restype = ctypes.c_bool

columnLib.Column_getPassedTheWhere.argstypes = [ctypes.c_void_p]
columnLib.Column_getPassedTheWhere.restype = ctypes.c_bool

columnLib.Column_getIsNull.argstypes = [ctypes.c_void_p]
columnLib.Column_getIsNull.restype = ctypes.c_bool

columnLib.Column_delete.argstypes = [ctypes.c_void_p]
columnLib.Column_delete.restype = None

columnLib.CreateIntWhereConst.argstypes = [ctypes.c_int64]
columnLib.CreateIntWhereConst.restype = ctypes.c_void_p

columnLib.CreateTimestampWhereConst.argstypes = [ctypes.c_uint64]
columnLib.CreateTimestampWhereConst.restype = ctypes.c_void_p

columnLib.CreateVarcharWhereConst.argstypes = [ctypes.c_char_p]
columnLib.CreateVarcharWhereConst.restype = ctypes.c_void_p

columnLib.CreateFloatWhereConst.argstypes = [ctypes.c_double]
columnLib.CreateFloatWhereConst.restype = ctypes.c_void_p

class Column:

	def __init__(self, table_name, field_name, field_type, file_sizes, file_num):
		self.obj = ctypes.c_void_p(columnLib.Column_new(bytes(os.path.join(table_name, field_name + "0" + '.ga'), encoding = "ascii"),
		{"int" : 0, "varchar" : 1, "float" : 2, "timestamp" : 3}[field_type]))
		
		self.table_name = table_name
		self.field_name = field_name
		self.field_type = field_type
		self.where_set_up = False
		self.file_sizes = file_sizes
		self.file_num = file_num
		self.current_batch = 0
		self.current_fp_index = 0
		
	def __del__(self):
		columnLib.Column_delete(self.obj)
	
	def setFP(self, index):
		columnLib.Column_setFP(self.obj, bytes(os.path.join(self.table_name, self.field_name + str(index) + '.ga'), encoding = "ascii"))

	def getRow(self, where):  # return: finished, valid(where), val
		if self.current_batch > self.file_sizes - 1:
				self.current_batch = 0
				self.current_fp_index += 1
				if (self.current_fp_index >= self.file_num):
					return True, None, None
				self.setFP(self.current_fp_index)
		if not self.where_set_up:
			if where != {} and where["field"] == self.field_name:	
				columnLib.Column_setOp(self.obj, {"<" : 0, "<=" : 1, ">" : 2, ">=" : 3, "==" : 4, "<>" : 5, "is" : 6, "is not" : 7}[where["op"]])
				if(self.field_type == "int"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateIntWhereConst(where["const"])))
				if(self.field_type == "timestamp"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateTimestampWhereConst(where["const"])))
				if(self.field_type == "varchar"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateVarcharWhereConst(where["const"])))
				if(self.field_type == "float"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateFloatWhereConst(where["const"])))
			self.where_set_up = True
			
		columnLib.Column_getRow(self.obj)
		finished = columnLib.Column_getFinished(self.obj)
		passedTheWhere = columnLib.Column_getPassedTheWhere(self.obj)
		if(self.field_type == "int"): 
			value = columnLib.Column_getIntVal(self.obj) if not columnLib.Column_getIsNull(self.obj) else ""
		if(self.field_type == "timestamp"): 
			value = columnLib.Column_getTimestampVal(self.obj) if not columnLib.Column_getIsNull(self.obj) else ""
		if(self.field_type == "varchar"):
			ca = columnLib.Column_getVarcharVal(self.obj)
			value = ctypes.c_char_p(ca).value.decode("ascii")
			columnLib.DeleteVarchar(ctypes.c_char_p(ca))#release the memory
		if(self.field_type == "float"):
			value = columnLib.Column_getFloatVal(self.obj) if not columnLib.Column_getIsNull(self.obj) else ""
		# self.current_batch += 1
		return finished, passedTheWhere, value
		