from Parser import Parser
from consts import *
from sqltokenizer import *
import argparse
import signal
import sys
import traceback
import os
import filecmp
import inspect
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
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
    "group_order"]


def signal_handler(sig, frame):
    print()
    if VERBOSE:
        print("good bye")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def execute_command(input_text):
    if VERBOSE:
        print("Executing Command:", input_text)
    p = Parser(input_text)
    try:
        # exc_info = sys.exc_info()
        a = p.execute()
        a[0](*a[1]) #( ͡° ͜ʖ ͡°)
        if VERBOSE:
            print("Execution Success!")
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
    with open(out) as o:
        with open(good) as g:
            for i, (lo, lg) in enumerate(zip(o, g)):
                if lo != lg:
                    print(f"DIFF[{'%4d'% i}]: {lo}\t{lg}")
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


def perform_tests():
    for test in tests:
        print("[running test]: ", test)
        with open(os.path.join(SOURCE_DIR, "unittests", test, "test.sql")) as infile:
            currentPath = os.path.abspath(os.getcwd())
            os.chdir(os.path.join(SOURCE_DIR, "unittests", test))
            for input_text in infile:
                execute_command(input_text.strip())
            if compare_files("output.csv", "good_output.csv"):
                print("test passed!")
            else:
                input("test did not pass! (press ENTER to continue)")
            print("------")
            if os.path.exists("output.csv"):
                os.remove("output.csv")
            if os.path.exists("tmp.txt"):
                os.remove("tmp.txt")
            os.chdir(currentPath)


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
    parser.add_argument("--unit", help="apply unittests", action='store_true')
    args = parser.parse_args()
    VERBOSE = args.verbose
    ROOT_DIR = args.rootdir
    # set_const("VERBOSE", args.verbose)
    # set_const("ROOT_DIR", args.rootdir)
    os.chdir(args.rootdir)
    if args.unit:
        perform_tests()
        return
    if args.run:
        with open(os.path.join(SOURCE_DIR, args.run)) as infile:
            input_text = infile.read()
            for command in get_commands(input_text):
                execute_command(command)
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

