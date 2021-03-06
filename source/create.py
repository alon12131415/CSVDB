import consts
import json
import os
import ZipManager

def create(table_name, fields, check_ex):
	table_path = table_name
	if os.path.exists(table_path):
		if check_ex:
			return
		else:
			raise ValueError("table already exists")
	os.makedirs(table_path)
	# print(table_name, fields, check_ex)
	schema = open(os.path.join(table_path, "table.json"), "w+")
	# print(fields)
	l = []
	for x, y in fields:
		os.makedirs(os.path.join(table_name, x))
		l.append({"field": x, "type": y})
	out = {'schema': l, 'file_sizes': consts.FILE_SIZES, 'file_num': 0, 'last_i': 0}
	# print(out)
	json.dump(out, schema, indent=True)
	schema.close()
	ZipManager.zipTable(table_name)


