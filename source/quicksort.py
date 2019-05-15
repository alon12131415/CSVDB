import random  # package for generating (pseudo) random elements
from consts import *
from writer import writer
from null import NULL
import os
import csv


def get_compare(ind, types):
    def real_cmp(line1, line2):
        l1 = list(line1[i[0]] for i in ind)
        l2 = list(line2[i[0]] for i in ind)
        cmp1 = []
        cmp2 = []
        for x1, x2, i in zip(l1, l2, ind):
            if types[i[0]] == "varchar":
                f1, f2 = str(x1), str(x2)
            elif types[i[0]] in ("int", "timestamp"):
                f1, f2 = int(x1) if x1 not in [
                    "NULL", NULL()] else NULL(), int(x2) if x2 not in [
                    "NULL", NULL()] else NULL()
            elif types[i[0]] == "float":
                f1, f2 = float(x1) if x1 not in [
                    "NULL", NULL()] else NULL(), float(x2) if x2 not in [
                    "NULL", NULL()] else NULL()
            else:
                raise NameError("Type Not Recognized!", types[i[0]])
            if i[1] == "asc":
                cmp1.append(f1)
                cmp2.append(f2)
            else:
                cmp1.append(f2)
                cmp2.append(f1)
        return -1 if cmp1 < cmp2 else 1

    return real_cmp


def merge(name1, name2, table_name, iteration, ind, compareFunc):
    out = "tmp_{}_{}".format(iteration, name1[3:])
    csvwriter = writer(os.path.join(table_name, out))
    f1 = open(os.path.join(table_name, name1))
    f2 = open(os.path.join(table_name, name2))
    csv1 = csv.reader(f1)
    csv2 = csv.reader(f2)

    line1 = next(csv1)
    line2 = next(csv2)
    a = 0
    b = 0
    while True:
        # if list(line1[i[0]] for i in ind)[0] < list(line2[i[0]] for i in
        # ind)[0]:
        if compareFunc(line1, line2) == -1:
            csvwriter.add_line(line1)
            a += 1
            try:
                line1 = next(csv1)
            except StopIteration:
                csvwriter.add_line(line2)
                for line2 in csv2:
                    csvwriter.add_line(line2)
                break
        else:
            csvwriter.add_line(line2)
            b += 1
            try:
                line2 = next(csv2)
            except StopIteration:
                csvwriter.add_line(line1)
                for line1 in csv1:
                    csvwriter.add_line(line1)
                break
    f1.close()
    f2.close()
    csvwriter.done()
    return out
