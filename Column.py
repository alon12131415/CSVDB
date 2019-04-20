from consts import *
from null import NULL
import os
import struct

class Column:

	def __init__(self, table_name, field_name, field_type, mode):
		self.mode = mode
		self.table_name = table_name
		self.field_name = field_name
		self.fp = open(os.path.join(self.table_name, self.field_name + "0" + '.ga'), self.mode)
		self.type = field_type
		
	def setFP(self, index):
		self.fp.close()
		self.fp = open(os.path.join(self.table_name, self.field_name + str(index) + '.ga'), self.mode)

	def getRow(self, where):#return: finished, valid(where), val
		
		def satisfiesWhere(x):#better solution would be eval but fine - shut up 
							  #TODO:        NULL stuff(smaller the everything, is, is not)
			if where['field'] != self.field_name:
				return True
			op = where["op"]
			const = where["const"]
			if x == None:
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

		if where == {}: satisfiesWhere = lambda x: True
		if self.type == "varchar":
			val = self.fp.readline().decode(encoding = "utf8")
			if not val: return True, True, None
			return False, satisfiesWhere(val[:-1]), val[:-1]
        
		elif self.type == "int":
			val = self.fp.read(9)
			if not val: return True, True, None
			if val[0] != 0:
				val = struct.unpack(">q", val[1:])[0]
				return False, satisfiesWhere(val), str(val)
			else:
				return False, satisfiesWhere(None), NULL()
	    
		elif self.type == "float":
			val = self.fp.read(8)
			if not val: return True, True, None
			if struct.unpack(">Q", val)[0] == 2**63:
				return False, satisfiesWhere(None), NULL()
			val = struct.unpack(">d", val)[0]
			return False, satisfiesWhere(val), str(val)
	    
	    
		elif self.type == "timestamp":
			val = self.fp.read(9)
			if not val: return True, True, None
			if val[0] != "\x00":
				val = struct.unpack(">Q", val[1:])[0]
				return False, satisfiesWhere(val), str(val)
			else:
				return False, satisfiesWhere(None), NULL()
				
	
	