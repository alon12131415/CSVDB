from null import NULL
import consts
import os
import sys
import struct
import ctypes
from library_manager import csvdbLib

class Column:

	def __init__(self, table_name, field_name, field_type, file_sizes, file_num):
		self.obj = ctypes.c_void_p(csvdbLib.Column_new(bytes(os.path.join(table_name, field_name), encoding = "ascii"),
		{"int" : 0, "varchar" : 1, "float" : 2, "timestamp" : 3}[field_type], file_sizes, file_num))

		self.table_name = table_name
		self.field_name = field_name
		self.field_type = field_type
		self.where_set_up = False
		self.file_sizes = file_sizes
		self.file_num = file_num
		self.current_batch = 0
		self.current_fp_index = 0

	def __del__(self):
		csvdbLib.Column_delete(self.obj)

	def setFP(self, index):
		csvdbLib.Column_setFP(self.obj, index)

	def getRow(self, where):  # return: finished, valid(where), val
		# if self.current_batch > self.file_sizes - 1:
		# 	self.current_batch = 0
		# 	self.current_fp_index += 1
		# 	if (self.current_fp_index >= self.file_num):
		# 		return True, True, None
		# 	self.setFP(self.current_fp_index)
		if not self.where_set_up:
			if where != {} and where["field"] == self.field_name:
				csvdbLib.Column_setOp(self.obj, {"<" : 0, "<=" : 1, ">" : 2, ">=" : 3, "==" : 4, "<>" : 5, "is" : 6, "is not" : 7}[where["op"]])
				if(self.field_type == "int"):	csvdbLib.Column_setWhereConst(self.obj, ctypes.c_void_p(csvdbLib.CreateIntWhereConst(where["const"])))
				if(self.field_type == "timestamp"):	csvdbLib.Column_setWhereConst(self.obj, ctypes.c_void_p(csvdbLib.CreateTimestampWhereConst(where["const"])))
				if(self.field_type == "varchar"):	csvdbLib.Column_setWhereConst(self.obj, ctypes.c_void_p(csvdbLib.CreateVarcharWhereConst(bytes(where["const"], encoding = "ascii"))))
				if(self.field_type == "float"):	csvdbLib.Column_setWhereConst(self.obj, ctypes.c_void_p(csvdbLib.CreateFloatWhereConst(where["const"])))
			self.where_set_up = True

		csvdbLib.Column_getRow(self.obj)
		finished = csvdbLib.Column_getFinished(self.obj)
		passedTheWhere = csvdbLib.Column_getPassedTheWhere(self.obj)
		if(self.field_type == "int"):
			value = csvdbLib.Column_getIntVal(self.obj) if not csvdbLib.Column_getIsNull(self.obj) else ""
		if(self.field_type == "timestamp"):
			value = csvdbLib.Column_getTimestampVal(self.obj) if not csvdbLib.Column_getIsNull(self.obj) else ""
		if(self.field_type == "varchar"):
			ca = csvdbLib.Column_getVarcharVal(self.obj)
			value = ctypes.c_char_p(ca).value.decode("ascii")
			csvdbLib.DeleteVarchar(ctypes.c_char_p(ca))#release the memory
		if(self.field_type == "float"):
			value = csvdbLib.Column_getFloatVal(self.obj) if not csvdbLib.Column_getIsNull(self.obj) else ""
		self.current_batch += 1
		return finished, passedTheWhere, value
