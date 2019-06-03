import consts
import os
import csv


class writer(object):

	def __init__(self, out, batching = False):
		self.base_file_name = out
		if batching:
			self.current_index = 0
			out += str(self.current_index)
		self.batching = batching
		self.csvfile = open(os.path.join(out), 'w+', newline='')
		self.csvwriter = csv.writer(self.csvfile)
		self.lines = []

	def flush(self):
		self.csvwriter.writerows(self.lines)
		self.lines = []

	def add_line(self, line):
		self.lines.append(line)
		if len(self.lines) >= consts.FILE_SIZES:
			self.flush()
			if self.batching:
				self.current_index += 1
				self.csvfile.close()
				out = self.base_file_name + str(self.current_index)
				self.csvfile = open(os.path.join(out), 'w+', newline='')
				self.csvwriter = csv.writer(self.csvfile)
				return out



	def done(self):
		x = len(self.lines) == 0
		self.flush()
		self.csvfile.close()
		return x