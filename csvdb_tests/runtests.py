import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import itertools
import csv

g_prog = None

def OSysArgs(args):
    print("Executing: ", " ".join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    out, err = p.communicate()
    if out:
        out = out.decode('utf-8')
    if err:
        print(out)
        print("ERROR =============\n", err.decode('utf-8'))
        raise RuntimeError("Error executing command")
    return out


def compare_files(out, good):
    print("Comparing files:\n\t{}\n\t{}".format(out, good))
    errs = 0
    with open(out) as o:
        with open(good) as g:
            for i, (lo, lg) in enumerate(itertools.zip_longest(csv.reader(o), csv.reader(g), fillvalue=[])):

                if lo != lg:
                    print(f"DIFF[{'%4d'% i}]: {repr(lo)}\t{repr(lg)}")
                    errs += 1
    if errs:
        raise RuntimeError("Test failed")


def run_test(test_dir, verbose):
    test_dir = os.path.abspath(test_dir)
    print("Running test at: ", test_dir)
    save_dir = os.getcwd() # in windows directory cannot be removed
            # if CD into it.
    with tempfile.TemporaryDirectory() as tmp_dir:
        sql = 'test.sql'
        for src in [sql] + [f for f in os.listdir(test_dir) if f.endswith(".csv")]:
            shutil.copyfile(os.path.join(test_dir, src), os.path.join(tmp_dir, src))
        verbose_args = ["--verbose"] if verbose else []
        cmd_args = [sys.executable, g_prog] + verbose_args + [
                    "--run", os.path.join(tmp_dir, sql),
                    "--rootdir", ".",
                    ]
        os.chdir(tmp_dir)
        cmd_out = OSysArgs(cmd_args)
        print(cmd_out)
        # go over output files and compare
        for output in os.listdir("."):
            if not re.match("^output.*\.csv$", output):
                continue
            out_file = os.path.join(tmp_dir, output)
            good_out = os.path.join(test_dir, "good_" + output)
            compare_files(out_file, good_out)
        os.chdir(save_dir)


def run_tests(tests_dir, verbose, testname=None):
    save_dir = os.getcwd()
    tests_count = 0
    for f in os.listdir(tests_dir):
        if re.match(r"^[\._]", f):
            # ignore .idea, __pycache__ etc.
            continue
        if testname and f != testname:
            continue
        full = os.path.join(tests_dir, f)
        if not os.path.isdir(full):
            continue
        tests_count += 1
        print("Running test: ", f)
        run_test(full, verbose)
    os.chdir(save_dir)
    print("{} tests done.".format(tests_count))

def main():
    global g_prog
    if not sys.version.startswith("3."):
        raise RuntimeError("please use Python 3")
    parser = argparse.ArgumentParser()
    parser.add_argument("--testsdir", help="Directory under which tests sub directory exist", required=True)
    parser.add_argument("--prog", help="python file to run", default="csvdb.py")
    parser.add_argument("--verbose", help="if given - run program with --verbose", action='store_true')
    parser.add_argument("--testname", help="if given - run test only on this directory")
    args = parser.parse_args()
    g_prog = os.path.abspath(args.prog)
    run_tests(args.testsdir, args.verbose, args.testname)


if __name__ == '__main__':
  main()
