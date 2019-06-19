import json
import os
import consts
import csv
import ctypes
import ZipManager

from library_manager import csvdbLib

def line_joiner(line): return ",".join(line)


class Table:

	def __init__(self, table_name):
		ZipManager.unzipTable(table_name)
		scheme_path = os.path.join(table_name, "table.json")
		if not os.path.isfile(scheme_path):
			raise FileNotFoundError(
				"Table '{}' does not exists".format(table_name))

		schema_file = open(scheme_path, "r")
		full_file = json.load(schema_file)
		schema_file.close()

		self.line_batches = full_file["file_sizes"]
		self.file_num = full_file["file_num"] + 1
		self.last_i = full_file["last_i"]
		consts.FILE_SIZES = self.line_batches
		schema = full_file['schema']
		# define local objects that help us get through the rough command ;-)

		self.name = table_name
		self.field_names = []
		self.name2type = {}
		for x in schema:
			field_name = x["field"]
			field_type = x["type"]
			self.field_names.append(field_name)
			self.name2type[field_name] = field_type

	def type_from_name(self, field_name):
		if not isinstance(field_name, tuple):
			return self.name2type[field_name]
		agg_type = field_name[0]
		field_type = self.name2type[field_name[1]]
		if agg_type in ['min', 'max', 'sum']:
			return field_type
		elif agg_type == 'count':
			return 'int'
		elif agg_type == 'avg':
			return 'float'

	def get_final_field(self, source):
		"""
		returns a list of the columns/aggs needed(if column so name if agg so a tuple),
		a dictionary of the nicknames of each of the out fields, will smash any duplicates so only the last one counts
		a set of the fields we need to read
		"""
		needed_fields = set()
		out = []
		nicknames = {}
		for x in source:
			if isinstance(x, tuple):  # aggregator
				needed_fields.add(x[1])
				nicknames[x] = x
				out.append(x)
			elif isinstance(x, list):  # nickname
				if x[1] in nicknames:
					raise NameError(x[1])
				else:
					nicknames[x[1]] = x[0]
					out.append(x[1])
					needed_fields.add(x[0] if not isinstance(x[0], tuple) else x[0][1])
			elif x in self.field_names:
				needed_fields.add(x)
				nicknames[x] = x
				out.append(x)
			elif x == "*":
				for y in self.field_names:
					needed_fields.add(y)
					nicknames[y] = y
				out.extend(self.field_names)
			else:
				raise NameError(x)
		return out, nicknames, needed_fields

	def select(self, out, _fields, where, group, having, order):
		"""
		out - name of outfile name or None in case of no outfile
		_fields - field names as supplied by the client(list)
		field is either a string for literal field, tuple for aggregator or list for nickname
		nickname is: [field, nickname]
		aggregator is: (field, aggergation)
		where - dictionary of op, const, field
		group - list of fields to group by
		having - like where
		order - list of: [(field, ASC/DESC)*]
		"""
		fields, nicknames, needed_fields = self.get_final_field(_fields)

		print_to_screen = False
		if out is None:
			out = "tmpOutput.csv"
			print_to_screen = True

		fields_list = list(needed_fields)
		if where:
			if not where["field"] in fields_list:
				fields_list.append(where["field"])

		def c_list(lst, varType):
			c_lst = (varType * len(lst))()
			c_lst[:] = lst
			return c_lst

		paths = [os.path.join(self.name, field).encode("ascii") for field in fields_list]
		c_paths = c_list(paths, ctypes.c_char_p)

		col_types = [{"int" : 0, "varchar" : 1, "float" : 2, "timestamp" : 3}[self.type_from_name(field)] for field in fields_list]
		c_col_types = c_list(col_types, ctypes.c_int)

		out_fields = [fields_list.index(nicknames[field] if not isinstance(nicknames[field], tuple) else nicknames[field][1]) for field in fields]
		c_out_fields = c_list(out_fields, ctypes.c_int)

		#order - list of: [(field, ASC/DESC)*]
		order_indeces = [fields.index(nicknames[x[0]] if not isinstance(nicknames[x[0]], tuple) else x[0]) for x in order]
		c_order_indeces = c_list(order_indeces, ctypes.c_int)

		order_directions = [{"asc": 0, "desc": 1}[x[1]] for x in order]
		c_order_directions = c_list(order_directions, ctypes.c_int)

		group_indeces = [fields.index(nicknames[x]) for x in group]
		c_group_indeces = c_list(group_indeces, ctypes.c_int)

		agg_types = {'min': 0, 'max': 1, 'sum': 2, 'count': 3, 'avg': 4}
		aggs_list = [agg_types[nicknames[field][0]] for field in fields if isinstance(nicknames[field], tuple)]
		c_aggs_list = c_list(aggs_list, ctypes.c_int)

		aggs_fields = [fields.index(nicknames[field] if nicknames[field] in fields else field) for field in fields if isinstance(nicknames[field], tuple)]
		c_aggs_fields = c_list(aggs_fields, ctypes.c_int)

		where_op = -1	#no where
		where_const = ctypes.c_void_p(0) #nullptr
		where_field = -1
		if(where):
			where_op = {"<" : 0, "<=" : 1, ">" : 2, ">=" : 3, "==" : 4, "<>" : 5, "is" : 6, "is not" : 7}[where["op"]]
			where_field = fields_list.index(where["field"])
			where_type = self.type_from_name(where["field"])
			if(where_type == "int"):	where_const = ctypes.c_void_p(csvdbLib.CreateIntWhereConst(where["const"]))
			if(where_type == "timestamp"):	where_const = ctypes.c_void_p(csvdbLib.CreateTimestampWhereConst(where["const"]))
			if(where_type == "varchar"):	where_const = ctypes.c_void_p(csvdbLib.CreateVarcharWhereConst(where["const"].encode("ascii")))
			if(where_type == "float"):	where_const = ctypes.c_void_p(csvdbLib.CreateFloatWhereConst(where["const"]))

		having_op = -1	#no having
		having_const = ctypes.c_void_p(0) #nullptr
		having_field = -1
		if(having):
			having_op = {"<" : 0, "<=" : 1, ">" : 2, ">=" : 3, "==" : 4, "<>" : 5, "is" : 6, "is not" : 7}[having["op"]]
			having_field = fields.index(having["field"])
			having_type = self.type_from_name(nicknames[having["field"]])
			if(having_type == "int"):	having_const = ctypes.c_void_p(csvdbLib.CreateIntWhereConst(having["const"]))
			if(having_type == "timestamp"):	having_const = ctypes.c_void_p(csvdbLib.CreateTimestampWhereConst(having["const"]))
			if(having_type == "varchar"):	having_const = ctypes.c_void_p(csvdbLib.CreateVarcharWhereConst(having["const"].encode("ascii")))
			if(having_type == "float"):	having_const = ctypes.c_void_p(csvdbLib.CreateFloatWhereConst(having["const"]))

		table = ctypes.c_void_p(csvdbLib.Table_Create(len(c_paths), c_paths, c_col_types, \
								len(c_out_fields), c_out_fields, \
								where_field, where_op, where_const, \
								self.line_batches, self.file_num,
								len(order), c_order_indeces,
								c_order_directions, (self.name + os.sep).encode("ascii"),
								len(group), c_group_indeces,
								len(aggs_fields), c_aggs_list, c_aggs_fields,
								having_field, having_op, having_const))
		csvdbLib.Table_select(table, out.encode("ascii"))
		csvdbLib.Table_delete(table)

		if print_to_screen:
			with open("tmpOutput.csv", "r") as csvfile:
				file_reader = csv.reader(csvfile)
				for row, _ in zip(file_reader, range(consts.MAX_PRINT_IN_SELECT)):
					print(line_joiner(row))
			os.remove("tmpOutput.csv")
		ZipManager.cleanTable(self.name)
