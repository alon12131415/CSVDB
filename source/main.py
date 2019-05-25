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
tests = [
	"age_null",
	"cmp_null",
	"ctas",
	"name_semi",
	"noa17",
	"age_ord",
	"age_ord_asc",
	"order_where",
	"big_order",
	"group1",
	"group_order",
	"float_null",
	"timestamp_test"]


def signal_handler(sig, frame):
	print()
	if consts.VERBOSE:
		print("good bye")
	sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def execute_command(input_text):
	if consts.VERBOSE:
		print("Executing Command:", input_text.replace("\n", " "))
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
	if os.stat(out).st_size == 0 and os.stat(out).st_size != 0:
		print("OUT FILE IS EMPTY!!!")
		return False
	with open(out) as o:
		with open(good) as g:
			for i, (lo, lg) in enumerate(itertools.zip_longest(o, g, fillvalue="")):
				if lo != lg:
					print(f"DIFF[{'%4d'% i}]: {repr(lo)}\t{repr(lg)}")
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

def perform_tests():
	failed = False
	for test_num, test in enumerate(tests):
		for file_size in range(2, 10):
			consts.FILE_SIZES = file_size
			print("[running test {}/{}]: {}".format(test_num, len(tests), test))
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
					else: clear()
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
		if failed:
			break
	if consts.ASCII:
		import print_pikachu
		if not failed:
			print_pikachu.lol(print_pikachu.bart)
		else:
			print_pikachu.lol(print_pikachu.pikachu)
	word = "test passed!"
	# # DO NOT TRY TO UNDERSTAND THIS CODE, it is the code from last year's printing sentences as big letters ;)
	# for a,b in [(-130,0),(-130,1),(-130,2),(-130,3),(-129,0),(-129,1),(-129,2),(-129,3)]:print(''.join([['{0:08b}'.format(([1109661696, 4342398, 2084731904, 8143426, 1078082560, 3949120, 1111653376, 8143426, 2017492480, 8273984, 2017492480, 4210752, 1078082560, 3949132, 2118271488, 4342338, 269498368, 3674128, 269498368, 6328336, 1884045824, 4342856, 1077952512, 8273984, 1381656064, 4342338, 1382171136, 4343370, 1111636992, 3949122, 2084731904, 4210752, 1111636992, 4015690, 2084731904, 4342856, 1010842624, 8126978, 269516288, 1052688, 1111638528, 3949122, 608469504, 1052712, 1385333248, 6707794, 270811648, 8537128, 675578368, 3674128, 268729856, 16662560][2*ord(x)+a]//(256**c))%256) for c in range(4)][b] if ord('A')<=ord(x)<=ord('Z') else '00000000' for x in word.upper()]).replace('0',' ').replace('1','\x90'))


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
		"--ascii",
		help="displays ascii art",
		action='store_true')
	args = parser.parse_args()
	consts.VERBOSE = args.verbose
	consts.ROOT_DIR = args.rootdir
	consts.ASCII = args.ascii
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

