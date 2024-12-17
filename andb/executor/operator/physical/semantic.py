import logging
import os
import json
import re

from andb.errno.errors import ExecutionStageError
from andb.executor.operator.logical import Condition, DummyTableName, PromptColumn, TableColumn
from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.select import Filter
from andb.sql.parser.ast.join import JoinType

class SemanticPrompt(PhysicalOperator):
    def __init__(self, prompt_text, client_model):
        super().__init__('SemanticPrompt')
        self.prompt_text = prompt_text
        self.client_model = client_model
        self.stream = None

    def open(self):
        messages=[{
            "role": "user", 
            "content": f"Hello, these are specific requirements: \"{self.prompt_text}\". "
                        f"Then, I am going to give you a text, please generate a response "
                        f"based on the requirements and the text."
        }]
        self.stream = self.client_model.complete_messages(messages=messages, stream=True)

    def next(self):
        for text in self.children[0].next():
            messages=[
                {"role": "assistant", "content": "I understand. Please provide the text."},
                {"role": "user", "content": text}
            ]
            response_stream = self.client_model.complete_messages(messages=messages, stream=True)
            
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
    def __init__(self, condition, client_model):
        super().__init__('SemanticFilter')
        self.client_model = client_model
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
            messages=[
                {"role": "system", "content": "You are a precise evaluator that only responds with 'true' or 'false'."},
                {"role": "user", "content": prompt}
            ]
            
            response = self.client_model.complete_messages(messages=messages, temperature=0.1)
            
            # Get the response and convert to boolean
            result = response.strip().lower()
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
    def __init__(self, join_type, client_model, target_columns=None, join_filter: Filter = None):
        super().__init__('SemanticJoin')
        self.client_model = client_model
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
            
            messages=[
                {"role": "system", "content": "You are a text analysis expert focused on finding relationships between documents."},
                {"role": "user", "content": prompt}
            ]
            
            # Call OpenAI API
            # Lower temperature for more focused responses and limit response length
            response = self.client_model.complete_messages(messages=messages, temperature=0.3, max_tokens=200)
            
            # Extract and return the relationship description
            return response.strip()
            
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
    
    def __init__(self, target_columns, prompt_text, client_model):
        """
        Args:
            target_columns: List of target columns including prompts
            prompt_text: Prompt text
        """
        super().__init__('SemanticTransform')
        self.columns = target_columns
        self.prompt_text = prompt_text
        self.client_model = client_model
        self.prompt_columns = [col for col in target_columns if isinstance(col, PromptColumn)]
        self.stream = None

    def open(self):
        """Initialize the operator"""
        if len(self.children) != 1:
            raise ValueError("SemanticTransform requires exactly one input operator")
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
                        messages=[
                            {"role": "system", "content": "Process the following text based on the given prompt."},
                            {"role": "user", "content": f"Prompt: {self.prompt_text}\nText: {input_text}"}
                        ]
                        response = self.client_model.complete_messages(messages=messages, temperature=0.3)
                        result = response.strip()
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
        
class SemanticScan(PhysicalOperator):
    """Physical operator for processing document into a proper table with prompts"""
    
    def __init__(self, schema, document, client_model, intermediate_data="tabular"):
        """
        Args:
            schema: Schema of the table.
            document: Document from which information will be extracted.
            client_model: Model for prompting.
            intermediate_data: Intermediate output (for debugging purposes between 'json' and 'tabular')
        """
        super().__init__('SemanticScan')
        self.schema = schema
        self.document = document
        self.client_model = client_model
        self.intermediate_data = intermediate_data
        if self.intermediate_data not in ["json", "tabular"]:
            raise NotImplementedError(f"Intermediate data `{self.intermediate_data}` is not implemented!")
        
        self.stream = None
        self.columns = None

    def open(self):
        return super.open()
    
    def _parse_json(output):
        try:
            # Try parsing directly first
            return json.loads(output)
        except json.JSONDecodeError:
            # Clean the output for common issues
            cleaned_output = output.strip()

            # Extract potential JSON objects or arrays
            cleaned_entries = []
            json_object_pattern = re.compile(r'\{.*?\}', re.DOTALL)
            entries = json_object_pattern.findall(cleaned_output)
            for entry in entries:
                try:
                    # Test if each entry is valid JSON
                    json.loads(entry)
                    cleaned_entries.append(entry)
                except json.JSONDecodeError:
                    # Skip invalid entries
                    pass

            # Reconstruct the cleaned JSON array
            cleaned_output = "[" + ",".join(cleaned_entries) + "]"

            # Attempt to parse again
            try:
                return json.loads(cleaned_output)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Error cleaning JSON: {e}")
    
    def _parse_markdown_into_tuples(self, raw_markdown):
        """
        Parses a markdown-style table into a list of tuples.
        The assumption is that `|` is used as the separator.
        """
        # Clean and split lines and combine into a CSV string
        lines = raw_markdown.strip().split("\n")
        cleaned_lines = [line.strip("|").strip() for line in lines]
        
        # Convert each line into a list of cells
        table_tuples = []
        for line in cleaned_lines:
            # Split on '|' and strip spaces
            cells = [cell.strip() for cell in line.split("|")]
            
            # Replace any cell with only dashes with None (to simulate NaN)
            cells = [None if re.fullmatch(r"-+", cell) else cell for cell in cells]
            
            # Append as a tuple
            table_tuples.append(tuple(cells))
        
        # Remove the separator row (if exists)
        if len(table_tuples) > 0:
            self.columns = list(table_tuples.pop(0))
        if len(table_tuples) > 1 and all(cell is None for cell in table_tuples[0]):
            table_tuples.pop(0)
        if len(table_tuples) > 1 and all(cell is None for cell in table_tuples[-1]):
            table_tuples.pop()
        
        return table_tuples
    
    def _parse_json_into_tuples(self, raw_output):
        cleaned_json = self._parse_json(raw_output)
        if len(cleaned_json) == 0:
            return []

        # Extract column names (keys of the first dictionary)
        self.columns = list(cleaned_json[0].keys())
        table_tuples = [tuple(item.get(col, None) for col in self.columns) for item in cleaned_json]

        return table_tuples
        
    def next(self):
        """
        Process whole document with semantic prompts
        Returns:
            Dataframe
        """
        temperature = 0.1
        prompt_schema = f"Schema: {self.schema}"
        table_tuples = []

        if self.intermediate_data == 'json':
            prompt_system = """
            You are a data extraction assistant.
            Your task is to extract structured information from unstructured text and format it into JSON.
            Follow the provided schema exactly.
            """

            messages = [
                {"role": "system", "content": prompt_system},
                {"role": "user", "content": f"""
                Convert the following raw text into a JSON array using this schema: '{prompt_schema}'.
                Ensure the response starts with '[' and ends with ']', with no additional text or explanation.
                Missing or empty values should be represented as null. 
                
                Raw text:
                {self.document}
                """}
            ]
            response = self.model.complete_messages(messages, temperature=temperature)
            table_tuples = self._parse_json_into_tuples(response)
            
        else:
            prompt_system = """
            You are a data extraction assistant.
            Your task is to extract structured information from unstructured text and format it into a row-based tabular format.
            Follow the provided schema exactly and ensure the output adheres to the specified structure.
            """
            
            messages = [
                {"role": "system", "content": prompt_system},
                {"role": "user", "content": f"""
                Convert the following raw text into a row-based tabular format with `|` as the delimitter, and use this schema: '{prompt_schema}'.
                Do not include any additional text or explanation outside the table.
                
                Raw text:
                {self.document}
                """}
            ]
            response = self.model.complete_messages(messages, temperature=temperature)
            table_tuples = self._parse_markdown_into_tuples(response)
        
        for tup in table_tuples:
            yield tup

    def close(self):
        """Clean up resources"""
        return super.close()

