from .base import ASTNode

class Command(ASTNode):
    def __init__(self, command: str, *args, **kwargs):
        super().__init__(*args, **kwargs) 
        self.command = command
    