from andb.sql.parser import lexer
from andb.sql.parser import parser_


def test_lexer():
    lexer.SQLLexer()


def test_parser():
    parser_.SQLParser()
