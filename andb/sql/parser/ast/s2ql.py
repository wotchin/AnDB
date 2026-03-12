from .base import ASTNode
from .identifier import Identifier


class MatchesPredicate(ASTNode):
    """WHERE column MATCHES 'assertion' [WITH (params)]

    Represents a semantic selection predicate in S²QL.
    The column's text content is evaluated against the assertion
    using semantic matching (LLM or embedding-based).
    """

    def __init__(self, column, assertion, with_params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.column = column  # Identifier for target column
        self.assertion = assertion  # str: semantic assertion text
        self.with_params = with_params or {}  # dict: {threshold: float, model: str, ...}


class ExtractExpression(ASTNode):
    """EXTRACT(source INTO (col1 TYPE1, col2 TYPE2, ...))

    Represents a semantic projection that extracts structured data
    from unstructured text using LLM-based extraction.
    """

    def __init__(self, source_column, target_schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_column = source_column  # Identifier: source text column
        self.target_schema = target_schema  # list of (name: str, type: str) tuples


class TransformExpression(ASTNode):
    """TRANSFORM(input AS output USING 'instruction')

    Represents a semantic transformation that converts text
    from one form to another using an LLM instruction.
    """

    def __init__(self, input_column, output_name, instruction, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_column = input_column  # Identifier: input column
        self.output_name = output_name  # str: output column name
        self.instruction = instruction  # str: transformation instruction


class ClassifyingGroupBy(ASTNode):
    """GROUP BY CLASSIFYING source_col AS key_name

    Represents a semantic aggregation where an LLM classifies
    each row into groups based on semantic understanding.
    """

    def __init__(self, source_column, key_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_column = source_column  # Identifier: column to classify
        self.key_name = key_name  # str: generated group key column name


class WithClause(ASTNode):
    """WITH (key1 = val1, key2 = val2, ...)

    Represents optional parameters for semantic operations,
    such as threshold, model selection, etc.
    """

    def __init__(self, params, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = params  # dict[str, any]: parameter key-value pairs
