from consts import *
import os
import csv


class writer(object):

    def __init__(self, out):
        self.csvfile = open(os.path.join(out), 'w+', newline='')
        self.csvwriter = csv.writer(self.csvfile)
        self.lines = []

    def flush(self):
        self.csvwriter.writerows(self.lines)
        self.lines = []

    def add_line(self, line):
        self.lines.append(line)
        if len(self.lines) >= LINE_BATCHES():
            self.flush()

    def done(self):
        self.flush()
        self.csvfile.close()
