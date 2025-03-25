from .base import ASTNode
from .identifier import Identifier
import re

class FileSource(ASTNode):
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.parts = file_path.value
        
class DirectorySource(ASTNode):
    def __init__(self, directory_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory_path = directory_path
        self.parts = directory_path.value

class SemanticMatch(ASTNode):
    def __init__(self, prompt_text, threshold=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.condition = None
        self.identifiers = []
        self.threshold = threshold.value if threshold else None
        
        if self.threshold and not isinstance(self.threshold, float) and not isinstance(self.threshold, int):
            raise ValueError(f"Threshold is neither float or int but type {type(self.threshold)}")
        
        self._convert_prompt_to_conditions(prompt_text)
    
    def _convert_prompt_to_conditions(self, prompt_text):
        def replace_placeholder(match):
            placeholder = match.group(1).strip()

            # Convert the placeholder into an Identifier object
            identifier = Identifier(parts=placeholder)
            self.identifiers.append(identifier)

            # Replace with indexed reference
            return f"'{{{len(self.identifiers) - 1}}}'"

        # Detect and replace placeholders in the prompt text
        self.condition = re.sub(r'\{([^{}]+)\}', replace_placeholder, prompt_text)

class Prompt(ASTNode):
    def __init__(self, prompt_text, defined_column, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prompt_text = prompt_text.value
        self.defined_column = defined_column
        self.alias = None

class SemanticTabular(ASTNode):
    def __init__(self, identifier, expr_list, table_source, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.semantic_schemas = expr_list
        self.table_source = table_source
        self.identifier = identifier
        
class SemanticGroup(ASTNode):
    def __init__(self, identifier, prompt, k, alias, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identifier = identifier
        self.prompt = prompt
        self.k = k
        self.alias = alias
        
class SemanticFilter(ASTNode):
    def __init__(self, identifier, prompt, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identifier = identifier
        self.prompt = prompt
