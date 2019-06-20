import os
import consts
consts.SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
from Parser import Parser
from sqltokenizer import *
import time
import argparse
import signal
import sys
import traceback
import filecmp
import inspect
import itertools
import csv
tests = [
	"age_null",
	"cmp_null",
	"ctas",
	"name_semi",
	"noa17",
	"age_ord",
	"age_ord_asc",
	"order_where",
	"small_order",
	# "big_order",
	"group1",
	"group_order",
	"float_null",
	"escaping",
	"timestamp_test",
	"sum_no_gb",
	"group_having"
	# "clicks"
	]

def signal_handler(sig, frame):
	print()
	# if consts.VERBOSE:
	print("good bye")
	sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def execute_command(input_text):
	if consts.VERBOSE:
		print("Executing Command:", input_text.replace("\n", " "))
		input()
		start_time = time.time()
	p = Parser(input_text)
	try:
		# exc_info = sys.exc_info()
		a = p.execute()
		a[0](*a[1]) #( ͡° ͜ʖ ͡°)
		if consts.VERBOSE:
			print("Execution Success!, took: {}".format(time.time() - start_time))
	except Exception as err:
		# if not isinstance(err, FileNotFoundError):
		traceback.print_tb(err.__traceback__)
		print("{}: {}".format(type(err).__name__, err))
		input()


def confirm_command(text):
	"""
	private feature, not intended for commercial use!
	"""
	tokenizer = SqlTokenizer(text)
	token, value = tokenizer.next_token()
	while token != SqlTokenKind.EOF:
		if token == SqlTokenKind.OPERATOR and value == ";":
			return True
		token, value = tokenizer.next_token()
	return False


def input_from_keyboard():
	input_text = ''
	new_row = ''
	while new_row != ';':
		new_row = input("csvdb>")
		input_text += new_row + " "
		if confirm_command(input_text):
			break
	return input_text


def compare_files(out, good):
	errs = 0
	if not os.path.isfile(out):
		print("Program Did NOT Generate an output.csv!!!")
		return False
	if os.stat(out).st_size == 0 and os.stat(out).st_size != 0:
		print("OUT FILE IS EMPTY!!!")
		return False
	with open(out) as o:
		with open(good) as g:
			for i, (lo, lg) in enumerate(itertools.zip_longest(csv.reader(o), csv.reader(g), fillvalue="")):
				if lo != lg:
					print(f"DIFF[{'%4d'% i}]: {lo}\t{lg}")
					input()
					print("error")
					errs += 1
	if errs:
		return False
	return True


def main_loop():
	print("""Welcome to the best csvdb program
Please enter a command according to the negotiated syntax
please finish a command with a ; IN A NEW LINE.""")
	while True:
		input_text = input_from_keyboard()
		execute_command(input_text)

def clear():
	if os.name == 'nt': os.system('cls') # for windows
	else: os.system('clear') # for mac and linux(here, os.name is 'posix')

def progress_bar(curr, full_val, length = 20):
	part = curr / full_val
	color_codes = {'black': 40,
					'red': 41,
					'green': 42,
					'yellow': 43,
					'blue': 44,
					'purple': 45,
					'cyan': 46,
					'white': 47}
	print_color = lambda code, s: "\033[1;1;{}m{}".format(code, s)
	switch_to_green = print_color(color_codes['green'], "")
	switch_to_black = print_color(color_codes['black'], "")
	switch_to_red = print_color(color_codes['red'], "")
	switch_to_yellow = print_color(color_codes['yellow'], "")
	colored_num = part * length
	# colored_spaces = switch_to_green +  " " * int(colored_num) + switch_to_black + " " * (length - int(colored_num))
	colored_spaces = switch_to_red
	colored_spaces += int(min(colored_num, length / 3)) * " "
	if colored_num > length / 3:
		colored_spaces += switch_to_yellow
		colored_spaces += int(min(colored_num - length / 3, length / 3)) * " "
		if colored_num > 2 * length / 3:
			colored_spaces += switch_to_green
			colored_spaces += int(colored_num - 2 * length / 3) * " "
	colored_spaces += switch_to_black + " " * (length - int(colored_num))
	return "Progress: " + colored_spaces + " " + str(int(part * 100)) + "%"

def perform_tests():
	failed = False
	os.system("color")
	debug = False
	if debug: tests.append("clicks")
	for test_num, test in enumerate(tests):
		for file_size in range(2, 10):
			if not debug: consts.FILE_SIZES = file_size
			print(progress_bar(test_num + 1, len(tests)) + " " + test)
			print("using file sizes of: ", consts.FILE_SIZES)
			with open(os.path.join(consts.SOURCE_DIR, "unittests", test, "test.sql")) as infile:
				currentPath = os.path.abspath(os.getcwd())
				os.chdir(os.path.join(consts.SOURCE_DIR, "unittests", test))
				input_text = infile.read()
				commands = get_commands(input_text)
				for command in commands:
					execute_command(command)
				if compare_files("output.csv", "good_output.csv"):
					if consts.VERBOSE: print("test passed!")
					else:
						clear()
				else:
					failed = True
				if consts.VERBOSE: print("------")
				if os.path.exists("output.csv"):
					os.remove("output.csv")
				if os.path.exists("tmp.txt"):
					os.remove("tmp.txt")
				os.chdir(currentPath)
				if failed:
					break
				if debug: break
		if failed:
			break
	if consts.ASCII:
		import print_pikachu
		if not failed:
			print_pikachu.lol(print_pikachu.bart)
		else:
			print_pikachu.lol(print_pikachu.pikachu)
	word = "test passed!"

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"--rootdir",
		help="Directory under which all table directories reside.",
		default="../")
	parser.add_argument(
		"--run",
		help="execute all SQL commands inside FILENMAE. If one fails - stop execution")
	parser.add_argument(
		"--verbose",
		help="print log messages that helps debug",
		action='store_true')
	parser.add_argument(
		"--test",
		help="print log messages that helps debug",
		action='store_true')
	parser.add_argument(
		"--unit",
		help="apply unittests",
		action='store_true')
	parser.add_argument(
		"--filesizes",
		help="set filesizes(the maximal amount of lines that fits in the RAM(random access memory))",
		defaults=2**31-1)
	parser.add_argument(
		"--ascii",
		help="displays ascii art",
		action='store_true')
	args = parser.parse_args()
	consts.VERBOSE = args.verbose
	consts.ROOT_DIR = args.rootdir
	consts.ASCII = args.ascii
	if(args.ram):
		consts.FILE_SIZES = 2**31 - 1
	# set_const("VERBOSE", args.verbose)
	# set_const("ROOT_DIR", args.rootdir)
	currentPath = os.getcwd()
	os.chdir(args.rootdir)
	if args.unit:
		perform_tests()
		os.chdir(currentPath)
		return
	if args.run:
		with open(os.path.join(consts.SOURCE_DIR, args.run)) as infile:
			input_text = infile.read()
			for command in get_commands(input_text):
				execute_command(command)
				# input()
		os.chdir(currentPath)
		return

	main_loop()

def get_commands(text):
	tokenizer = SqlTokenizer(text)
	commands = []
	prev_i = 0
	tok, val = tokenizer.next_token()
	while tok != SqlTokenKind.EOF:
		if tok == SqlTokenKind.OPERATOR and val == ";":
			commands.append(text[prev_i: tokenizer._i_next])
			prev_i = tokenizer._i_next
		tok, val = tokenizer.next_token()
	return commands

if __name__ == '__main__':
	main()
