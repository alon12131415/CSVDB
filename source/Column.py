from null import NULL
import consts
import os
import struct
import ctypes

columnLib = ctypes.cdll.LoadLibrary(os.path.join(consts.SOURCE_DIR, "column.dll"))

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

columnLib.CreateIntWhereConst.argstypes = [ctypes.c_int]
columnLib.CreateIntWhereConst.restype = ctypes.c_void_p

columnLib.CreateVarcharWhereConst.argstypes = [ctypes.c_char_p]
columnLib.CreateVarcharWhereConst.restype = ctypes.c_void_p

columnLib.CreateFloatWhereConst.argstypes = [ctypes.c_double]
columnLib.CreateFloatWhereConst.restype = ctypes.c_void_p

class Column:

	def __init__(self, table_name, field_name, field_type, mode = "rb"):
		self.use_column_dll = field_type in ["int", "varchar", "float"] and mode == "rb" 
		if self.use_column_dll:
			self.obj = ctypes.c_void_p(columnLib.Column_new(bytes(os.path.join(table_name, field_name + "0" + '.ga'), encoding = "ascii"),
			{"int" : 0, "varchar" : 1, "float" : 2, "timestamp" : 3}[field_type]))
			
			self.table_name = table_name
			self.field_name = field_name
			self.field_type = field_type
			self.where_set_up = False;
			
			return
			
		self.mode = mode
		self.table_name = table_name
		self.field_name = field_name
		self.fp = open(
			os.path.join(
				self.table_name,
				self.field_name +
				"0" +
				'.ga'),
			self.mode)
		self.type = field_type
	
	def __del__(self):
		if self.use_column_dll:
			columnLib.Column_delete(self.obj)
	
	def setFP(self, index):
		if self.use_column_dll:
			columnLib.Column_setFP(self.obj, bytes(os.path.join(self.table_name, self.field_name + str(index) + '.ga'), encoding = "ascii"))
			return
		self.fp.close()
		self.fp = open(
			os.path.join(
				self.table_name,
				self.field_name +
				str(index) +
				'.ga'),
			self.mode)

	def getRow(self, where):  # return: finished, valid(where), val
		if self.use_column_dll:
			if not self.where_set_up:
				if where != {} and where["field"] == self.field_name:	
					columnLib.Column_setOp(self.obj, {"<" : 0, "<=" : 1, ">" : 2, ">=" : 3, "==" : 4, "<>" : 5, "is" : 6, "is not" : 7}[where["op"]])
					if(self.field_type == "int"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateIntWhereConst(where["const"])))
					if(self.field_type == "varchar"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateVarcharWhereConst(where["const"])))
					if(self.field_type == "float"):	columnLib.Column_setWhereConst(self.obj, ctypes.c_void_p(columnLib.CreateFloatWhereConst(where["const"])))
				self.where_set_up = True
			
			columnLib.Column_getRow(self.obj)
			finished = columnLib.Column_getFinished(self.obj)
			passedTheWhere = columnLib.Column_getPassedTheWhere(self.obj)
			if(self.field_type == "int"): value = columnLib.Column_getIntVal(self.obj) if not columnLib.Column_getIsNull(self.obj) else ""
			if(self.field_type == "varchar"):
				ca = columnLib.Column_getVarcharVal(self.obj)
				value = ctypes.c_char_p(ca).value.decode("ascii")
				columnLib.DeleteVarchar(ctypes.c_char_p(ca))#release the memory
			if(self.field_type == "float"): value = columnLib.Column_getFloatVal(self.obj) if not columnLib.Column_getIsNull(self.obj) else ""
			return finished, passedTheWhere, value
		
		def satisfiesWhere(x):  # better solution would be eval but fine - shut up
			if where['field'] != self.field_name:
				return True
			op = where["op"]
			const = where["const"]
			if x is None:
				return (op == "is")

			if op == "<":
				return x < const
			if op == "<=":
				return x <= const
			if op == "<>":
				return x != const
			if op == "==":
				return x == const
			if op == ">=":
				return x >= const
			if op == ">":
				return x > const
			if op == "is":
				return False
			if op == "is not":
				return True
			raise NotImplementedError(x, op, const)

		if where == {}:
			def satisfiesWhere(x): return True
		if self.type == "varchar":
			val = self.fp.readline().decode(encoding="utf8")
			if not val:
				return True, True, None
			return False, satisfiesWhere(val[:-1]), val[:-1]

		elif self.type == "int":
			val = self.fp.read(9)
			if not val:
				return True, True, None
			if val[0] != 0:
				val = struct.unpack(">q", val[1:])[0]
				return False, satisfiesWhere(val), str(val)
			else:
				return False, satisfiesWhere(None), NULL()

		elif self.type == "float":
			val = self.fp.read(8)
			if not val:
				return True, True, None
			print("should read: {}".format(struct.unpack(">Q", val)[0]))
			if struct.unpack(">Q", val)[0] == 2**63:
				return False, satisfiesWhere(None), NULL()
			val = struct.unpack(">d", val)[0]
			return False, satisfiesWhere(val), str(val)

		elif self.type == "timestamp":
			val = self.fp.read(9)
			if not val:
				return True, True, None
			if val[0] != "\x00":
				val = struct.unpack(">Q", val[1:])[0]
				return False, satisfiesWhere(val), str(val)
			else:
				return False, satisfiesWhere(None), NULL()
