import re
import sly


class SQLLexer(sly.Lexer):
    reflags = re.IGNORECASE
    ignore = ' \t\n\r'
    ignore_multi_comment = r'/\*[\s\S]*?\*/'
    ignore_line_comment = r'--[^\n]*'

    tokens = {
        # DDL
        CREATE, DROP,
        DATABASE, TABLE, INDEX, VIEW, COLUMN, ALTER,

        # Misc
        EXPLAIN, USING, IF_EXISTS,

        # SELECT statement
        WITH,
        SELECT, DISTINCT, STAR, FROM, WHERE, AS,
        GROUP_BY, HAVING,
        ORDER_BY, LIMIT, OFFSET, ASC, DESC,
        JOIN, FULL, INNER, OUTER, CROSS, LEFT, RIGHT, ON,
        UNION, ALL,

        # CASE
        CASE, ELSE, END, THEN, WHEN,

        # DML
        INSERT, DELETE, INTO, VALUES,
        UPDATE, SET,

        # PUNCTUATION
        DOT, COMMA, LPAREN, RPAREN, PARAMETER,

        # OPERATORS
        PLUS, MINUS, DIVIDE, MODULO,
        EQ, NE, GT, GEQ, LT, LEQ,
        AND, OR, NOT, IS, IS_NOT,
        IN, LIKE, CONCAT, BETWEEN, WINDOW, OVER, PARTITION_BY,

        # DATA TYPES
        ID,
        FLOAT, INTEGER, QUOTE_STRING, DQUOTE_STRING, NULL, TRUE, FALSE,
        CAST,

        # COMMANDS
        CHECKPOINT,

        # Add new tokens
        PROMPT,
        FILE
    }

    CREATE = 'CREATE'
    DROP = 'DROP'
    DATABASE = 'DATABASE'
    TABLE = 'TABLE'
    INDEX = 'INDEX'
    VIEW = 'VIEW'
    COLUMN = 'COLUMN'
    ALTER = 'ALTER'
    EXPLAIN = 'EXPLAIN'
    USING = 'USING'
    IF_EXISTS = 'IF EXISTS'
    WITH = 'WITH'
    SELECT = 'SELECT'
    DISTINCT = 'DISTINCT'
    STAR = r'\*'
    FROM = 'FROM'
    WHERE = 'WHERE'
    AS = 'AS'
    GROUP_BY = 'GROUP BY'
    HAVING = 'HAVING'
    ORDER_BY = 'ORDER BY'
    LIMIT = 'LIMIT'
    OFFSET = 'OFFSET'
    ASC = 'ASC'
    DESC = 'DESC'
    JOIN = 'JOIN'
    INNER = 'INNER'
    OUTER = 'OUTER'
    CROSS = 'CROSS'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    ON = 'ON'
    UNION = 'UNION'
    ALL = 'ALL'
    CASE = 'CASE'
    ELSE = 'ELSE'
    END = 'END'
    THEN = 'THEN'
    WHEN = 'WHEN'
    INSERT = 'INSERT'
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'
    SET = 'SET'
    INTO = 'INTO'
    VALUES = 'VALUES'
    CHECKPOINT = 'CHECKPOINT'
    PROMPT = 'PROMPT'
    FILE = 'FILE'

    DOT = r'\.'
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'

    PLUS = r'\+'
    MINUS = r'-'
    DIVIDE = r'/'
    MODULO = r'%'
    EQ = r'='
    NE = r'!='
    GEQ = r'>='
    GT = r'>'
    LEQ = r'<='
    LT = r'<'
    AND = r'\bAND\b'
    OR = r'\bOR\b'
    IS_NOT = r'\bIS[\s]+NOT\b'
    NOT = r'\bNOT\b'
    IS = r'\bIS\b'
    LIKE = r'\bLIKE\b'
    IN = r'\bIN\b'
    CAST = r'\bCAST\b'
    CONCAT = r'\|\|'
    BETWEEN = r'\bBETWEEN\b'
    WINDOW = r'\bWINDOW\b'
    OVER = r'\bOVER\b'
    PARTITION_BY = r'\bPARTITION BY\b'

    NULL = r'\bNULL\b'
    TRUE = r'\bTRUE\b'
    FALSE = r'\bFALSE\b'

    @_(r'(?:([a-zA-Z_$0-9]*[a-zA-Z_$]+[a-zA-Z_$0-9]*)|(?:`([^`]+)`))(?:\.(?:([a-zA-Z_$0-9]*[a-zA-Z_$]+['
       r'a-zA-Z_$0-9]*)|(?:`([^`]+)`)))*')
    def ID(self, t):
        return t

    @_(r'-?\d+\.\d*')
    def FLOAT(self, t):
        return t

    @_(r'-?\d+')
    def INTEGER(self, t):
        return t

    @_(r"'[^']*'")
    def QUOTE_STRING(self, t):
        return t

    @_(r'"[^"]*"')
    def DQUOTE_STRING(self, t):
        return t

    @_(r'\n+')
    def ignore_newline(self, t):
        self.lineno += len(t.value)
