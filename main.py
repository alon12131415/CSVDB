from Parser import Parser
from consts import *
import argparse
import signal
import sys
import traceback
import os
import filecmp
import inspect
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
tests = ["age_null","cmp_null","ctas","name_semi","noa17", "age_ord", "age_ord_asc", "order_where", "group1", "big_order"]

def signal_handler(sig, frame):
	print()
	if VERBOSE(): print("good bye")
	sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
def execute_command(input_text):
	if VERBOSE(): print("Executing Command:", input_text)
	p = Parser(input_text)
	try:
		exc_info = sys.exc_info()
		a = p.execute()
		a[0](*a[1])
		if VERBOSE(): print("Execution Success!")
	except Exception as err:
		# if not isinstance(err, FileNotFoundError):
		traceback.print_tb(err.__traceback__)
		print("{}: {}".format(type(err).__name__, err))

def main_loop():
	print("""Welcome to the best csvdb program
Please enter a command according to the negotiated syntax
please finish a command with a ; IN A NEW LINE.""")
	while True:
		input_text = ''
		new_row = ''
		while new_row != ';':
			new_row = input("csvdb>")
			input_text += new_row + " " 
		execute_command(input_text)

def perform_tests():
	for test in tests:
		print("[running test]: ", test)
		with open(os.path.join(SOURCE_DIR, "unittests", test, "test.sql")) as infile:
			for input_text in infile:
				execute_command(input_text.strip())
			if filecmp.cmp("output.csv",os.path.join(SOURCE_DIR,"unittests", test, "good_output.csv")):
				print("test passed!")
			else:
				input("test did not pass! (press ENTER to continue)")
			print("------")
		if os.path.exists("output.csv"):
			os.remove("output.csv")
		if os.path.exists("tmp.txt"):
			os.remove("tmp.txt")
		

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("--rootdir", help="Directory under which all table directories reside.", default="../")
	parser.add_argument("--run", help="execute all SQL commands inside FILENMAE. If one fails - stop execution")
	parser.add_argument("--verbose", help="print log messages that helps debug", action='store_true')
	parser.add_argument("--test", help="print log messages that helps debug", action='store_true')
	parser.add_argument("--unit", help="apply unittests", action='store_true')
	args = parser.parse_args()
	set_const("VERBOSE", args.verbose)
	set_const("ROOT_DIR", args.rootdir)
	os.chdir(args.rootdir)
	if args.unit: 
		perform_tests()
		return
	if args.run:
		with open(os.path.join(args.rootdir, args.run)) as infile:
			for input_text in infile:
				execute_command(input_text.strip())
		return

	main_loop()

if __name__ == '__main__':
	main()