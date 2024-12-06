import logging
import os

from openai import OpenAI
from andb.errno.errors import ExecutionStageError
from andb.executor.operator.logical import Condition, DummyTableName, PromptColumn, TableColumn
from andb.executor.operator.physical.base import PhysicalOperator
from andb.constants.strings import OPENAI_API_KEY
from andb.executor.operator.physical.select import Filter
from andb.sql.parser.ast.join import JoinType

lm_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY') or OPENAI_API_KEY)
lm_model_name = 'gpt-4o-mini'

class SemanticPrompt(PhysicalOperator):
    def __init__(self, prompt_text):
        super().__init__('SemanticPrompt')
        self.prompt_text = prompt_text
        self.stream = None

    def open(self):
        self.stream = lm_client.chat.completions.create(
            model=lm_model_name,
            messages=[{
                "role": "user", 
                "content": f"Hello, this is a specific requirments: \"{self.prompt_text}\". "
                          f"And then, I am gonna give you a text, please generate a response "
                          f"based on the requirments and the text."
            }],
            stream=True
        )

    def next(self):
        for text in self.children[0].next():
            response_stream = lm_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "assistant", "content": "I understand. Please provide the text."},
                    {"role": "user", "content": text}
                ],
                stream=True
            )
            
            full_response = ""
            try:
                for chunk in response_stream:
                    if chunk.choices[0].delta.content is not None:
                        full_response += chunk.choices[0].delta.content
            except Exception as e:
                print(f"Error processing chunk: {e}")
                continue
            
            cleaned_response = full_response.strip()
            if cleaned_response:
                yield cleaned_response

    def close(self):
        if self.stream:
            try:
                self.stream.close()
            except:
                pass
        self.stream = None


class SemanticFilter(PhysicalOperator):
    def __init__(self, condition):
        super().__init__('SemanticFilter')
        if isinstance(condition, Condition):
            self.condition_prompt = f'we only consider the following condition: {str(condition)}'
        elif isinstance(condition, str):
            self.condition_prompt = f'we only consider the following condition: {condition}'
        else:
            raise ValueError("Condition must be a Condition object or a string")
    
    def open(self):
        return super().open()
    
    def close(self):
        return super().close()
    
    def judge(self, tuple):
        # Convert tuple to text format for analysis
        text = str(tuple[0]) if len(tuple) == 1 else " ".join(str(x) for x in tuple)
        
        try:
            # Create a prompt that combines the condition and the tuple text
            prompt = f"""
            Condition: {self.condition_prompt}
            Text to evaluate: {text}
            
            Does the text satisfy the condition? Please respond with only 'true' or 'false'.
            """
            
            # Call OpenAI API
            response = lm_client.chat.completions.create(
                model=lm_model_name,
                messages=[
                    {"role": "system", "content": "You are a precise evaluator that only responds with 'true' or 'false'."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1  # Low temperature for more consistent responses
            )
            
            # Get the response and convert to boolean
            result = response.choices[0].message.content.strip().lower()
            return result == 'true'
            
        except Exception as e:
            logging.error(f"Error in semantic filtering: {e}")
            raise e

    def next(self):
        for tuple in self.children[0].next():
            if self.judge(tuple):
                yield tuple

class SemanticJoin(PhysicalOperator):
    """
    Semantic Join operator that uses OpenAI API to join documents based on their semantic meaning
    """
    def __init__(self, join_type, target_columns=None, join_filter: Filter = None):
        super().__init__('SemanticJoin')
        self.client = lm_client
        self.join_type = join_type
        self.target_columns = target_columns
        self.join_filter = join_filter
        self.join_prompt = ''

        if self.join_type == JoinType.INNER_JOIN:
            self.join_prompt += "Only return the relationship if the two texts are semantically related."
        elif self.join_type == JoinType.LEFT_JOIN:
            self.join_prompt += "Only return the relationship if the 'Text 1' is semantically related to the Text 2."
        elif self.join_type == JoinType.RIGHT_JOIN:
            self.join_prompt += "Only return the relationship if the 'Text 2' is semantically related to the Text 1."
        elif self.join_type == JoinType.FULL_JOIN:
            self.join_prompt += "Return the relationship if the two texts are semantically related."
        
    def open(self):
        """
        Initialize the operator and validate children
        """
        # Validate we have exactly 2 children (left and right input)
        if len(self.children) != 2:
            raise ValueError("SemanticJoin requires exactly two input operators")
        
        # Open both child operators
        self.children[0].open()
        self.children[1].open()
        
        # Set output columns
        self.columns = (
            self.children[0].columns +  # Left input columns
            self.children[1].columns +  # Right input columns 
            [TableColumn(DummyTableName.TEMP_TABLE_NAME, 'relationship')]  # Add relationship column
        )

    def next(self):
        """
        Generate joined results by semantically comparing documents
        """
        # Get all texts from left child
        for left_tuple in self.children[0].next():
            left_text = left_tuple[0]  # Assuming text content is first column
            
            # Get all texts from right child
            for right_tuple in self.children[1].next():
                right_text = right_tuple[0]  # Assuming text content is first column
                
                # Get semantic relationship using OpenAI
                relationship = self._get_semantic_relationship(left_text, right_text)
                
                # Yield combined tuple with relationship
                yield left_tuple + right_tuple + (relationship,)

    def _get_semantic_relationship(self, text1, text2):
        """
        Use OpenAI API to analyze relationship between two texts
        """
        try:
            # Create prompt combining both texts
            prompt = f"""
            Text 1: {text1}
            
            Text 2: {text2}
            
            {self.join_prompt}
            """
            
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=lm_model_name,  # Using GPT-4 for better understanding
                messages=[
                    {"role": "system", "content": "You are a text analysis expert focused on finding relationships between documents."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # Lower temperature for more focused responses
                max_tokens=200    # Limit response length
            )
            
            # Extract and return the relationship description
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            raise ExecutionStageError(f"Error in semantic analysis: {e}")

    def close(self):
        """
        Clean up resources
        """
        self.children[0].close()
        self.children[1].close()
        super().close()


class SemanticTransform(PhysicalOperator):
    """Physical operator for processing semantic target list with prompts"""
    
    def __init__(self, target_columns, prompt_text):
        """
        Args:
            target_columns: List of target columns including prompts
            prompt_text: Prompt text
        """
        super().__init__('SemanticTarget')
        self.columns = target_columns
        self.prompt_text = prompt_text
        self.prompt_columns = [col for col in target_columns if isinstance(col, PromptColumn)]
        self.stream = None

    def open(self):
        """Initialize the operator"""
        if len(self.children) != 1:
            raise ValueError("SemanticTarget requires exactly one input operator")
        self.children[0].open()

    def next(self):
        """
        Process each input tuple with semantic prompts
        Returns:
            Generator yielding processed tuples
        """
        # Process each input tuple
        for input_tuple in self.children[0].next():
            result_tuple = []
            
            # Convert input tuple to text format
            input_text = " ".join(str(x) for x in input_tuple)
            
            # Process each target column
            for target in self.columns:
                if isinstance(target, PromptColumn):
                    # Process semantic prompt
                    try:
                        response = lm_client.chat.completions.create(
                            model=lm_model_name,
                            messages=[
                                {"role": "system", "content": "Process the following text based on the given prompt."},
                                {"role": "user", "content": f"Prompt: {self.prompt_text}\nText: {input_text}"}
                            ],
                            temperature=0.3
                        )
                        result = response.choices[0].message.content.strip()
                        result_tuple.append(result)
                    except Exception as e:
                        logging.error(f"Error in semantic processing: {e}")
                        result_tuple.append(None)
                else:
                    # Pass through original column value
                    col_index = self.children[0].columns.index(target)
                    result_tuple.append(input_tuple[col_index])
            
            yield tuple(result_tuple)

    def close(self):
        """Clean up resources"""
        if self.stream:
            try:
                self.stream.close()
            except:
                pass
        self.stream = None
        self.children[0].close()
