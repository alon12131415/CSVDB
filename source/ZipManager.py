import zipfile, os

def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			if f[-4:] == ".zip":
				continue
			fp = os.path.join(dirpath, f)
			# skip if it is symbolic link
			if not os.path.islink(fp):
				total_size += os.path.getsize(fp)
	return total_size


def zipTable(table_name):
	if os.path.isfile(os.path.join( table_name, table_name + ".zip")):
		os.remove(os.path.join( table_name, table_name + ".zip"))
	with zipfile.ZipFile(os.path.join( table_name, table_name + ".zip"), "w", zipfile.ZIP_DEFLATED, 1) as myzip:
		for root, dirs, files in os.walk(table_name):
			for file in files:
				if (file[-4:] == ".zip"):
					continue
				myzip.write(os.path.join(root, file))
	print()
	if get_size(table_name) < os.path.getsize(os.path.join(table_name, table_name + ".zip")):
		os.remove(os.path.join(table_name, table_name + ".zip"))
	else:
		for root, dirs, files in os.walk(table_name):
			for file in files:
				if (file[-4:] == ".zip"):
					continue
				os.remove(os.path.join(root, file))
	# os.remove(os.path.join(root, file))

def unzipTable(table_name):
	if not os.path.isfile(os.path.join( table_name, table_name + ".zip")):
		return
	with zipfile.ZipFile(os.path.join( table_name, table_name + ".zip"), "r") as myzip:
		myzip.extractall(".")


def cleanTable(table_name):
	if not os.path.isfile(os.path.join( table_name, table_name + ".zip")):
		return
	for root, dirs, files in os.walk(table_name):
		for file in files:
			if (file[-3:] != ".ga"):
				continue
			os.remove(os.path.join(root, file))
if __name__ == '__main__':
	zipTable("test")
