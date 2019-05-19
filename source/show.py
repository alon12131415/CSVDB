from Table import Table


def show(table_name, boop):
	t = Table(table_name)
	print("col_name, col_type\n")
	for n, y in t.name2type.items():
		print(n, y)
