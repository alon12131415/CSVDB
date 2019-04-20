from __future__ import print_function
import ast
from enum import Enum
import re

class SqlTokenKind(Enum):
    ERROR = -1
    EOF = 0
    KEYWORD = 1
    IDENTIFIER = 2
    LIT_STR = 3
    LIT_NUM = 4
    OPERATOR = 5


class SqlTokenizer(object):
    # ERROR = -1
    # EOF = 0
    # KEYWORD = 1
    # IDENTIFIER = 2
    # LIT_STR = 3
    # LIT_NUM = 4
    # OPERATOR = 5


    _reserved_words = [
        'select',
        'from',
        'where',
        'avg',
        'sum',
        'min',
        'max',
        'load',
        'drop',
        'order',
        'by',
        'group',
        'into',
        'outfile',
        'as',
        'having',
        'data',
        'infile',
        'table',
        'ignore',
        'lines',
        'null',
        'int',
        'float',
        'varchar',
        'timestamp',
        'desc',
        'asc',
        'and',
        'or',
        'not',
        'create',
        'if',
        'exists',
    ]
    _operators = [
        ",",
        "(",
        ")",
        "<>",
        "<",
        "<=",
        "=",
        ">=",
        ">",
        ";",
        "*",
    ]

    def __init__(self, text):
        self._text = text
        self._n = len(text)
        self._i_next = 0   # index into text[]
        # cur_line_start_index, cur_line_number are used to give syntax error messages location
        self._cur_line_start_index = 0  # index into text[] of the line start
        self._cur_line_number = 0

    def next_token(self):
        """returns two value - token kind and value"""
        self._skip_wspace()
        if self._cur() == "-" and self._cur1() == "-":
            # sql comment
            self._skip_till_eoline()
        if self._eof():
            return SqlTokenKind.EOF, None
        if self._cur().isalpha() or self._cur() == "_":
            return self._next_keyword_or_identifier()
        num_val = self._next_lit_numeric()
        if num_val is not None:
            return SqlTokenKind.LIT_NUM, num_val
        if self._cur() == '"':
            s = self._next_lit_string()
            if not s or s[-1] != '"':
                self._i_next = len(self._text)   # error - move to end
                return SqlTokenKind.ERROR, "ERROR: BAD TOKEN"
            return SqlTokenKind.LIT_STR, ast.literal_eval(s)
        next_operator = self._next_operator()
        if next_operator:
            return SqlTokenKind.OPERATOR, next_operator
        bad_text = self._text[self._i_next:]
        self._i_next = len(self._text)  # error - move to end
        return SqlTokenKind.ERROR, "ERROR: UNKNOWN TOKEN " + bad_text

    def cur_text_location(self):
        """return line number and offset in line of the current token to allow
        error messages. returns two calues: line, column"""
        return self._cur_line_number + 1, self._i_next - self._cur_line_start_index + 1

    def _next_lit_numeric(self):
        m = re.match(r"^([\-\+]?\d+(\.\d*)?(e[\-\+]?\d+)?)", self._text[self._i_next:])
        if m:
            num, point, exp = m.group(1,2,3)

            if point or exp:
                val = float(num)
            else:
                val = int(num)
        else:
            # not starting with a digit - digits after decimal point are mandatory
            m = re.match(r"^([\-\+]?\.\d+(e[\-\+]?\d+)?)", self._text[self._i_next:])
            if m:
                num = m.group(1)
                val = float(num)
        if not m:
            return None
        self._i_next += len(num)
        return val

    def _next_lit_string(self):
        val = self._cur()  # assume it to be '"'
        quote = self._cur()
        skip_char = 0
        while not self._eof():
            self._next_char()
            val += self._cur()
            if skip_char > 0:
                skip_char -= 1
                continue
            if self._cur() == "\\":
                skip_char = 1
                continue
            if self._cur() == quote:
                self._next_char()
                break
        return val

    def _next_operator(self):
        for op in SqlTokenizer._operators:
            if self._text[self._i_next:].startswith(op):
                self._i_next += len(op)
                return op
        return None

    def _eof(self):
        return self._i_next >= self._n

    def _cur(self):
        if self._eof():
            return ""
        return self._text[self._i_next]

    def _cur1(self):
        if self._i_next + 1 >= self._n:
            return ""
        return self._text[self._i_next + 1]

    def _next_char(self):
        self._i_next += 1

    def _start_line(self):
        self._cur_line_start_index = self._i_next
        self._cur_line_number += 1

    def _skip_wspace(self):
        while not self._eof():
            if self._cur().isspace():
                if self._cur() == "\n":
                    self._start_line()
                self._next_char()
            else:
                break

    def _skip_till_eoline(self):
        while not self._eof():
            c = self._cur()
            self._next_char()
            if c == "\n":
                self._start_line()
                break

    def _is_reserved_word(self, w):
        return w in SqlTokenizer._reserved_words

    def _next_keyword_or_identifier(self):
        token_value = ""
        while not self._eof() and (self._cur().isalnum() or self._cur() == "_"):
            token_value += self._cur()
            self._next_char()
        token_value = token_value.lower()
        if self._is_reserved_word(token_value):
            return SqlTokenKind.KEYWORD, token_value
        return SqlTokenKind.IDENTIFIER, token_value


def _test():
    text = r"""select year, max(name) , avg(duration) 
    into outfile "c:\\temp\\mobvie_duration.csv"
    from movies
    where year >= 2000
    group by year
    having avg(duration) > 0.1
    order by year asc
    """

    tokenizer = SqlTokenizer(text)
    while True:
        line, col = tokenizer.cur_text_location()
        tok, val = tokenizer.next_token()

        print("Line {}/{}  {}  {}".format(line, col, tok, val))

        if tok == SqlTokenKind.EOF:
            break

if __name__ == "__main__":
    _test()
