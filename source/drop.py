import os
import shutil


def drop(check_ex, table_name):
	table_path = table_name
	if not os.path.exists(table_path):
		if check_ex:
			return
		else:
			raise FileNotFoundError("table does not exist")

	shutil.rmtree(table_path)
