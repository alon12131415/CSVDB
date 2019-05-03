import os
import json
import inspect
path = os.path.dirname(
    os.path.abspath(
        inspect.getfile(
            inspect.currentframe())))


def ROOT_DIR():
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    return j["ROOT_DIR"]


def MAX_PRINT_IN_SELECT():
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    return j["MAX_PRINT_IN_SELECT"]


def FILE_SIZES():
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    return j["FILE_SIZES"]


def LINE_BATCHES():
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    return j["LINE_BATCHES"]


def VERBOSE():
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    return j["VERBOSE"]


def set_const(const, value):
    fp = open(os.path.join(path, "consts.json"), "r")
    j = json.load(fp)
    fp.close()
    fp = open(os.path.join(path, "consts.json"), "w")
    j[const] = value
    json.dump(j, fp, indent=True)
