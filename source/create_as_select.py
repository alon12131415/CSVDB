from create import create
from Table import Table
from select_from import select_from
from load import load
import os


def create_as_select(table_name, select_fields):
	# out_file, fields, original_table_name, where, group, having, order = select_fields
	assert select_fields[0] is None
	original_table = Table(select_fields[2])
	# print(original_table.get_final_field(select_fields[1]))
	final_fields = []
	fields, nicknames, needed_fields = original_table.get_final_field(
		select_fields[1])
	for field in fields:
		final_fields.append(
			(field, original_table.type_from_name(nicknames[field])))
	# final_fields = [(x, original_table.name2type[x]) for x in original_table.get_final_field(select_fields[1])[0]]
	# print(final_fields)

	create(table_name, final_fields, False)
	select_fields = list(select_fields)
	select_fields[0] = "tmp.txt"
	select_fields = tuple(select_fields)
	select_from(*select_fields)
	load("tmp.txt", table_name, 0)
	# os.remove("tmp.txt")
