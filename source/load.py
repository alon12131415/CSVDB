import consts
import json
import csv
import struct
import datetime
import Table
import os

class write_column(object):

	def __init__(self, table_name, field_name, col_type, index):
		self.fp = open_file(table_name, field_name, index)
		self.type = col_type


def open_file(table_name, field_name, index):
	return open(os.path.join(table_name,field_name + str(index) + '.ga'),"ab")

def load(file_name, table_name, ignored):
	# table = Table.Table(table_name, "ab")
	fp_list = {}
	field_names = []
	scheme_path = os.path.join(table_name, "table.json")
	schema_file = open(scheme_path, "r")
	full_file = json.load(schema_file)
	schema_file.close()
	file_num = full_file["file_num"]
	last_i = full_file["last_i"]
	schema = full_file['schema']

	with open(file_name, "r") as csvfile:
		file_reader = csv.reader(csvfile)
		i = 0
		current_batch = last_i
		max_size = consts.FILE_SIZES
		current_fp_index = file_num
		for x in schema:
			field_name = x["field"]
			field_type = x["type"]
			field_names.append(field_name)
			fp_list[field_name] = write_column(table_name, field_name, field_type, current_fp_index)
		for row in file_reader:
			if current_batch > max_size - 1:
				current_batch = 0
				current_fp_index += 1
				for field_name in field_names:
					fp_list[field_name].fp.close()
					fp_list[field_name].fp = open_file(table_name, field_name, current_fp_index)
			if i < ignored:
				i += 1
				continue
			for val, field_name in zip(row, field_names):
				col = fp_list[field_name]

				if col.type == "varchar":
					col.fp.write(bytes(val + "\x00", encoding='utf8'))

				elif col.type == "int":
					col.fp.write((b"\x00" + struct.pack(">q",0)) \
							 if val in ["","NULL"] else (
							b"\x01" + struct.pack(">q",int(val))))

				elif col.type == "float":
					if val in ["", "NULL"]:
						# negative zero(-0.0) , NULL
						out = struct.pack(">Q", 2**63)
					else:
						out = struct.pack(">d", float(val))
					col.fp.write(out)

				elif col.type == "timestamp":
					col.fp.write(
						(b"\x00" +
						 struct.pack(
							 ">Q",
							 0)) if val in [
							"",
							"NULL"] else (
							b"\x01" +
							struct.pack(
								">Q",
								int(val))))
			current_batch += 1
		scheme_path = os.path.join(table_name, "table.json")
		schema_file = open(scheme_path, "r+")
		full_file = json.load(schema_file)
		schema_file.seek(0)
		full_file["file_num"] = current_fp_index
		full_file["last_i"] = current_batch
		json.dump(full_file, schema_file, indent=True)
		schema_file.close()
