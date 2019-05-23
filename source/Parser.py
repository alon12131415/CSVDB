#! python3
from sqltokenizer import *
from create import create
from load import load
from create_as_select import create_as_select
from select_from import select_from
from drop import drop
from show import show
import os
import json
# import readline


class Parser():
	def __init__(self, in_text):
		self.tokenizer = SqlTokenizer(in_text)
		self._tok = ''
		self._val = ''

	def next_tokens(self):
		self._tok, self._val = self.tokenizer.next_token()
		# print("DEBUG1", self._tok, self._val)

	def expect(self, token, value, skip=False, ass=False):
		# print("expecting:", token, value)
		if not skip:
			self.next_tokens()
		if self._tok != token or self._val != value:
			if ass:
				raise SyntaxError("unexpected token: {}".format(self._val))
			return False
		return True

	def execute(self):
		self.next_tokens()
		if self._val == "create":
			x = self.parse_create()
			if x[0] == 0:
				return create, x[1:]
			else:
				return create_as_select, x[1:]
		elif self._val == "drop":
			return drop, self.parse_drop()
		elif self._val == "load":
			return load, self.parse_load()
		elif self._val == "select":
			return select_from, self.parse_select()
		elif self._val == "show":
			return show, self.parse_show()
		else:
			raise SyntaxError("'{}' Unexpected Command".format(self._val))

	def create_check_existing(self):
		self.expect(SqlTokenKind.KEYWORD, "table", ass=True)

		self.next_tokens()
		# determine if whe need to check for the existence of the table
		if self._tok == SqlTokenKind.KEYWORD and self._val == 'if':
			self.expect(SqlTokenKind.KEYWORD, "not", ass=True)
			self.expect(SqlTokenKind.KEYWORD, "exists", ass=True)
			self.next_tokens()
			return True
		return False

	def create_get_fields(self):

		fields = []
		while True:
			if self._tok != SqlTokenKind.IDENTIFIER:
				raise SyntaxError
			field = self._val
			self.next_tokens()
			if self._tok != SqlTokenKind.KEYWORD:
				raise SyntaxError
			fields.append((field, self._val))
			if (not self.expect(SqlTokenKind.OPERATOR, ",")):
				if self._tok == SqlTokenKind.OPERATOR and self._val == ")":
					break
				else:
					raise SyntaxError("Expected ',' Between columns")
			self.next_tokens()
		return fields

	def parse_create(self):

		check_ex = self.create_check_existing()
		table_name = self._val

		if not self.expect(SqlTokenKind.OPERATOR, "("):
			self.expect(SqlTokenKind.KEYWORD, "as", skip=True, ass=True)
			self.expect(SqlTokenKind.KEYWORD, "select", ass=True)
			select_out = self.parse_select()
			# print(select_out)
			return 1, table_name, select_out

		self.next_tokens()
		fields = self.create_get_fields()
		self.expect(SqlTokenKind.OPERATOR, ";", ass=True)
		return 0, table_name, fields, check_ex

	def select_get_fields(self):
		"""
		returns a list of the fields where every element of the list is:
		str: simply a column
		tuple: aggregator
		list: one of the above but with a nickname
		"""
		# print("yo")
		fields = []
		while True:
			field = self._val
			self.next_tokens()
			if self._val == "(":
				self.next_tokens()
				field = (field, self._val)
				# aggregators are TUPLE, ass = TrueS
				self.expect(SqlTokenKind.OPERATOR, ")")
				self.next_tokens()
			if self.expect(SqlTokenKind.KEYWORD, "as", skip=True):
				self.next_tokens()
				nickname = self._val
				field = [field, nickname]  # nicknames are LIST
				self.next_tokens()
			fields.append(field)
			if not self.expect(SqlTokenKind.OPERATOR, ",", skip=True):
				return fields
			self.next_tokens()

	def select_where(self):
		where = {}
		if self._val == "where":
			self.next_tokens()
			where["field"] = self._val
			self.next_tokens()
			where["op"] = self._val
			self.next_tokens()
			if where["op"] == "is":
				if self._val == "not":
					where["op"] = "is not"
					self.next_tokens()
				self.expect(SqlTokenKind.KEYWORD, "null", skip=True, ass=True)
			where["const"] = self._val
			self.next_tokens()
		if "op" in where and where["op"] == "=":
			where["op"] = "=="
		return where

	def select_group(self):
		group = []
		if self._val == "group":
			self.expect(SqlTokenKind.KEYWORD, "by", ass=True)
			while True:
				self.next_tokens()
				group.append(self._val)
				if not self.expect(SqlTokenKind.OPERATOR, ","):
					# self.next_tokens()
					break
		return group

	def select_having(self):
		having = {}
		if self._val == "having":
			self.next_tokens()
			having["field"] = self._val
			self.next_tokens()
			having["op"] = self._val
			self.next_tokens()
			having["const"] = self._val
			self.next_tokens()
		return having

	def select_order(self):
		order = []
		if self._val == "order":
			self.expect(SqlTokenKind.KEYWORD, "by", ass=True)
			while True:
				order_way = "asc"
				self.next_tokens()
				f = self._val
				self.next_tokens()
				if self._val in ["asc",
								 "desc"] and self._tok == SqlTokenKind.KEYWORD:
					order_way = self._val
					self.next_tokens()
				order.append((f, order_way))
				if (not self.expect(SqlTokenKind.OPERATOR, ",", skip=True)):
					# self.next_tokens()
					break
		return order

	def parse_select(self):
		self.next_tokens()

		fields = self.select_get_fields()
		# print(fields)
		file_name = None
		if self._val == "into":
			self.expect(SqlTokenKind.KEYWORD, "outfile", ass=True)
			self.next_tokens()
			file_name = self._val
			self.next_tokens()
		self.expect(SqlTokenKind.KEYWORD, "from", ass=True, skip=True)

		self.next_tokens()
		table = self._val
		self.next_tokens()

		where = self.select_where()
		group = self.select_group()
		having = self.select_having()
		order = self.select_order()

		self.expect(SqlTokenKind.OPERATOR, ";", skip=True, ass=True)
		return file_name, fields, table, where, group, having, order

	def drop_check_ex(self):
		self.expect(SqlTokenKind.KEYWORD, "table", ass=True)

		self.next_tokens()
		# determine if whe need to check for the existence of the table
		if self._tok == SqlTokenKind.KEYWORD and self._val == 'if':
			self.expect(SqlTokenKind.KEYWORD, "exists", ass=True)
			self.next_tokens()
			return True
		return False

	def parse_drop(self):
		check_ex = self.drop_check_ex()
		table_name = self._val
		self.expect(SqlTokenKind.OPERATOR, ";", ass=True)
		return check_ex, table_name

	def parse_load(self):
		# self.next_tokens()
		self.expect(SqlTokenKind.KEYWORD, "data", ass=True)
		self.expect(SqlTokenKind.KEYWORD, "infile", ass=True)
		self.next_tokens()
		file_name = self._val
		self.expect(SqlTokenKind.KEYWORD, "into", ass=True)
		self.expect(SqlTokenKind.KEYWORD, "table", ass=True)
		self.next_tokens()
		table_name = self._val
		# print(table_name)
		ignored = 0
		if not self.expect(SqlTokenKind.OPERATOR, ";"):
			self.expect(SqlTokenKind.KEYWORD, "ignore", skip=True, ass=True)
			self.next_tokens()
			ignored = self._val
			self.expect(SqlTokenKind.KEYWORD, "lines", ass=True)
		# else:
		# print(ignored)
		return file_name, table_name, ignored

	def parse_show(self):
		self.expect(SqlTokenKind.KEYWORD, "table", ass=True)
		self.next_tokens()
		table_name = self._val
		self.expect(SqlTokenKind.OPERATOR, ";", ass=True)
		return table_name, 1
