from consts import *
import json
import csv
import struct
import datetime
import Table
import os

def load(file_name, table_name, ignored):
	table = Table.Table(table_name, "wb")
	with open(file_name, "r") as csvfile:
		file_reader = csv.reader(csvfile)
		
		i = 0
		current_batch = 0
		max_size = FILE_SIZES()
		current_fp_index = 0
		for row in file_reader:	
			if current_batch > max_size - 1:
				current_batch = 0
				current_fp_index += 1
				for field_name in table.field_names: table.columns[field_name].setFP(current_fp_index)
				
			if i < ignored:
				i += 1
				continue
			for val, field_name in zip(row, table.field_names):
				col = table.columns[field_name]
				
				if col.type == "varchar":
					col.fp.write(bytes(val + "\n", encoding = 'utf8'))

				elif col.type == "int":
					col.fp.write((b"\x00" + struct.pack(">q", 0)) if val in ["", "NULL"] else (b"\x01" + struct.pack(">q", int(val))))

				elif col.type == "float":
					if val in ["", "NULL"]:
						out = struct.pack(">Q", 2**63) #negative zero(-0.0) , NULL
					else: 
						out = struct.pack(">d", float(val))
					col.fp.write(out)

				elif col.type == "timestamp":
					col.fp.write((b"\x00" + struct.pack(">Q", 0)) if val in ["", "NULL"] else (b"\x01" + struct.pack(">Q", int(val))))
			current_batch += 1