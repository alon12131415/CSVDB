from quicksort import merge, get_compare
from consts import *
from writer import writer
from null import NULL
import functools
import operator
import glob
import os
import json
import Column
import csv
import time

line_joiner = lambda line: ",".join(line)



class Table:
	
	def __init__(self, table_name, mode = "rb"):
		scheme_path = os.path.join(table_name, "table.json")
		if not os.path.isfile(scheme_path): raise FileNotFoundError("Table '{}' does not exists".format(table_name))
		
		schema_file = open(scheme_path, "r")
		full_file = json.load(schema_file)
		schema_file.close()

		self.line_batches = full_file["file_sizes"]
		set_const("FILE_SIZES", self.line_batches)
		schema = full_file['schema']
		#define local objects that help us get through the rough command ;-)

		self.name = table_name
		self.field_names = []
		self.columns = {}
		self.name2type = {}
		for x in schema:
			field_name = x["field"]
			field_type = x["type"]
			self.columns[field_name] = Column.Column(table_name, field_name, field_type, mode)
			self.field_names.append(field_name)
			self.name2type[field_name] = field_type
			

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
			if isinstance(x, tuple):		#aggregator
				needed_fields.add(x[1])
				nicknames[x] = x
				out.append(x)
			elif isinstance(x, list):		#nickname
				if x[1] in nicknames:
					raise NameError(x[1])
				else:
					nicknames[x[1]] = x[0]
					out.append(x[1])
					needed_fields.add(x[0])
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



	def clean_up(self):
		for filename in glob.glob(os.path.join(self.name, "tmp*")):			#remove all tmp file
			os.remove(filename)

	def update_aggs(self, agg_vals, row, field_list):
		for agg in agg_vals:
			agg_type, field_name = agg
			field_value = row[field_list.index(field_name)]
			field_type = self.name2type[field_name]
			
			if field_value == ' ' and field_type != 'varchar':	continue#null value
			
			if(field_type == 'int'):	field_value = int(field_value)
			if(field_type == 'float'):	field_value = float(field_value)
			if(field_type == 'timestamp'):	field_value = int(field_value)
			
			if agg_type == 'min':
				if field_type == 'int' or field_type == 'float' or field_type == 'timestamp':
					agg_vals[agg] = min(field_value, agg_vals[agg])
				else:
					raise NotImplementedError('min is not supported for {} type fields'.format(field_name))
			elif agg_type == 'max':
				if field_type == 'int' or field_type == 'float' or field_type == 'timestamp':
					agg_vals[agg] = max(field_value, agg_vals[agg])
				else:
					raise NotImplementedError('max is not supported for {} type fields'.format(field_name))		
			elif agg_type == 'sum':
				if field_type == 'int' or field_type == 'float':
					agg_vals[agg] += field_value
				else:
					raise NotImplementedError('sum is not supported for {} type fields'.format(field_name))
			elif agg_type == 'count':
				if field_type == 'int' or field_type == 'float' or field_type == 'timestamp' or field_type == 'varchar':
					agg_vals[agg] += 1
				else:
					raise NotImplementedError('count is not supported for {} type fields'.format(field_name))
			else:#no need to handle avg because we will calculate sum and count instead(and divide). 
				raise NotImplementedError('unimplemented agg type: {}'.format(agg_type))

	def write_group_line(self, out_file, fields, agg_vals, row, row_fields):
		line = []
		for field in fields:
			if isinstance(field, tuple):#agg
				if(field[0] != 'avg'):	line.append(agg_vals[field])
				if(field[0] == 'avg'):	line.append(agg_vals[('sum', field[1])] / agg_vals[('count', field[1])])
			else:
				line.append(row[row_fields.index(field)])
		out_file.add_line(line)
	
	def set_up_agg_vals(fields):
		agg_vals = {}
		for field in fields:
			if not isinstance(field, tuple):	continue
			if field[0] == 'min':
				agg_vals[field] = 2**63 - 1
			elif field[0] == 'max':
				agg_vals[field] = -2**63
			elif field[0] == 'sum':
				agg_vals[field] = 0
			elif field[0] == 'count':
				agg_vals[field] = 0
			elif field[0] == 'avg':
				agg_vals[('sum', field[1])] = 0#calculate avg based on count and sum
				agg_vals[('count', field[1])] = 0
			else:
				raise NotImplementedError('unimplemented agg type: {}'.format(field[0]))
	
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
		#print(order)
		start_time = time.time()
		fields, nicknames, needed_fields = self.get_final_field(_fields)
		#print("fields:", fields)
		#print("nicknames:", nicknames)
		#print("needed_fields:", needed_fields)
		
		if any((isinstance(field, tuple) for field in fields)):#any aggs
			needed_fields_list = list(needed_fields)
			out_file_writer = writer(out)
			agg_vals = {field : 0 for field in fields if isinstance(field, tuple)}
			self.select('tmp.csv', needed_fields_list, where, [], {}, [(field, 'asc') for field in group])
			with open(os.path.join('tmp.csv'), "r") as csvfile:
				csv_reader = csv.reader(csvfile)
				prev_group = None
				prev_row = []
				for row in csv_reader:
					curr_group = [row[needed_fields_list.index(field)] for field in group]
					if prev_group == None: prev_group = curr_group
					if curr_group != prev_group:
						self.write_group_line(out_file_writer, fields, agg_vals, prev_row, needed_fields_list)
						agg_vals = {field : 0 for field in fields if isinstance(field, tuple)}#reset agg vals
					self.update_aggs(agg_vals, row, needed_fields_list)
					prev_group = curr_group
					prev_row = row
				self.write_group_line(out_file_writer, fields, agg_vals, prev_row, needed_fields_list)
				out_file_writer.done()
			os.remove(os.path.join('tmp.csv'))
			return
		
		if order:
			final = []
			fp_list = []
			file_num = self.line_batches
			order_mapped = list(map(lambda x: (fields.index(x[0]), x[1]), order))
			while True:														#sort files seperately
				try:
					a = self.sort_internally(order_mapped, ind, fields, nicknames, needed_fields, where)
				except FileNotFoundError:
					break
				fp_list.append(a)
			final_file = self.merge_files(fp_list, order_mapped, 0)			#merge sorted files
			if out is not None:																#no_print
				os.rename(os.path.join(self.name, final_file), os.path.join(out))
			else:
				csvfile = open(os.path.join(self.name, final_file), "r")
				csvreader = csv.reader(csvfile)
				for line, i in zip(csvreader, range(200)):
					print(line_joiner(line))

				csvfile.close()

			self.clean_up()								#delete all tmp files :)

			if VERBOSE(): print("took {} seconds".format(time.time() - start_time))
			return

		if out is not None:
			csvwriter = writer(out)
		printCount = 0
		current_batch = 0
		max_size = FILE_SIZES()
		current_fp_index = 0		
		while printCount < MAX_PRINT_IN_SELECT():
			if current_batch > max_size - 1:
				current_batch = 0
				current_fp_index += 1
				try:
					for field_name in self.field_names: self.columns[field_name].setFP(current_fp_index)
				except FileNotFoundError:
					break
			current_batch += 1
			field_res = {field:self.columns[field].getRow(where) for field in needed_fields}
			line = []
			for field in fields:
				if not isinstance(field, tuple):
					line.append(field_res[nicknames[field]])
				else:
					raise NotImplementedError
			if line[0][0]:#is col 0 finished
				break
			if not all([x[1] for x in line]):#didn't pass the where
				continue
			line = [x[2] for x in line]
			if out is not None:
				csvwriter.add_line(line)
			else:
				print(line_joiner(str(x) for x in line))
				printCount += 1
		if out is not None: csvwriter.done()
		if VERBOSE(): print("took {} seconds".format(time.time() - start_time))

	def sort_internally(self, order, index, fields, nicknames, needed_fields, where):
		"""
		sorts the files of index $index according to the fields. 
		writes the output to a csv and returns the filename.
		"""
		for field_name in self.field_names: self.columns[field_name].setFP(index)
		
		out = "tmp{}".format(index)
		csvwriter = writer(os.path.join(self.name, out))

		field_types = []
		for field in fields:
			field_types.append(self.name2type[nicknames[field]])
		line_cmp = get_compare(order, field_types)
		csvwriter.lines = [field_types]
		csvwriter.flush()
		rows = []
		for _ in range(FILE_SIZES()):
			row = [self.columns[field].getRow(where) for field in fields]
			if row[0][0]:
				break
			if not all([x[1] for x in row]):#didn't pass the where
				continue
			rows.append([x[2] for x in row])
		# csvwriter.lines = sorted(rows, key=operator.itemgetter(*sort_indexes))
		csvwriter.lines = sorted(rows, key=functools.cmp_to_key(line_cmp))
		csvwriter.done()
		return out

	def merge_files(self, file_lst, ind, it):
		"""
		merges files recursivly where ind is a list of indexes which are needed to be sorted by
		(to sort multiple fields)
		returns filename of the output file.
		"""
		if len(file_lst) == 1:
			return file_lst
		elif len(file_lst) == 2:
			return merge(file_lst[0], file_lst[1],self.name, it + 1, ind)
		else:
			file1 = merge_files(file_lst[:len(file_lst)//2], ind, it)
			file2 = merge_files(file_lst[len(file_lst)//2:], ind, it)
			return merge(file1, file2,self.name, it + 1, ind)
