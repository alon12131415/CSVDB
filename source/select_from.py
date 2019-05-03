import Table


def select_from(out_file, fields, table_name, where, group, having, order):
    table = Table.Table(table_name)
    table.select(out_file, fields, where, group, having, order)
