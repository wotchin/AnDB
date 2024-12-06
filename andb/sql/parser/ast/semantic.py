from .base import ASTNode

class FileSource(ASTNode):
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.parts = file_path.value

class Prompt(ASTNode):
    def __init__(self, prompt_text, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prompt_text = prompt_text
        self.alias = None 
