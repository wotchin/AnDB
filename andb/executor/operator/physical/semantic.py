import json
import re
from collections import defaultdict

import numpy as np
import faiss

from andb.ai.client_model import ClientModelFactory
from andb.ai.embedding_model import EmbeddingModelFactory
from andb.catalog.oid import INVALID_OID
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS, CATALOG_ANDB_TYPE
from andb.executor.operator.logical import FunctionColumn, PromptColumn, SemanticTransformColumn
from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.select import Filter
from andb.runtime import session_vars


def default_client_model():
    return ClientModelFactory.create_model(model_type=session_vars.get_session_value('client_llm'),
                                           **session_vars.SessionParameter.__dict__)


def default_embedding_model():
    return EmbeddingModelFactory.create_model(model_type=session_vars.get_session_value('embed_llm'),
                                              **session_vars.SessionParameter.__dict__)


## HELPER FUNCTIONS

def _parse_json(output):
    try:
        # Try parsing directly first
        json_out = json.loads(output)
        if not isinstance(json_out, list):
            return [json_out]
        else:
            return json_out
    except json.JSONDecodeError:
        # Clean the output for common issues
        cleaned_output = output.strip()

        # Extract potential JSON objects or arrays
        cleaned_entries = []
        json_object_pattern = re.compile(r'\{.*?}', re.DOTALL)
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


class SemanticFilter(PhysicalOperator):
    def __init__(self, condition):
        super().__init__('SemanticFilter')
        self.has_init_models = False
        self.client_model = None
        self.embedding_model = None
        self.condition = condition.condition
        self.threshold = condition.threshold
        self.filter_table_columns = condition.table_columns
        self.child_columns = []
        self.tab_col_idxs = []

    def set_proj_index(self, columns):
        # Iterate over which column index to be used; for now assume only left and right
        for filter_col in self.filter_table_columns:
            match_found = False
            for col_idx, col in enumerate(columns):
                if filter_col == col:  # Compare TableColumn with TableColumn
                    self.tab_col_idxs.append(col_idx)
                    match_found = True
                    break

            if match_found:
                break

            # If no match was found for some reason
            if not match_found:
                raise ValueError(f"Column '{filter_col.column_name}' not found.")

    def open(self):
        if len(self.children) != 1:
            raise ValueError("SemanticFilter currently only supports one input operator")

        self.children[0].open()
        self.columns = self.children[0].columns
        self.set_proj_index(self.columns)
        super().open()

    def close(self):
        for child in self.children:
            child.close()
        return super().close()

    def _construct_prompt_condition(self, row):
        # Condition is already a string that just needs to be formatted
        entry_vals = []
        for col_idx in self.tab_col_idxs:
            entry_vals.append(row[col_idx])

        return self.condition.format(*tuple(entry_vals))

    def filter(self, iterator):
        if not self.has_init_models:
            self.has_init_models = True
            self.client_model = default_client_model()
            self.embedding_model = default_embedding_model()

        # Materialize all tuple for batch call
        list_tuple = []
        batched_messages = []
        filtered_tuples = []
        if isinstance(self.threshold, float):
            text_list = []
            instruction = "Embed the following tuples for semantic similarity:"
            for tup in iterator:
                entry_vals = []
                for col_idx in self.tab_col_idxs:
                    entry_vals.append(tup[col_idx])
                text_list.append(f"{instruction} {str(*entry_vals)}")
                list_tuple.append(tup)

            query_instr = [self.condition.format(*[""] * len(self.tab_col_idxs))]

            # Compute cosine similarity matrix
            query_embed = np.array(self.embedding_model.generate_embeddings(query_instr, normalize_embeddings=True),
                                   dtype=np.float32)
            embeddings = np.array(self.embedding_model.generate_embeddings(text_list, normalize_embeddings=True),
                                  dtype=np.float32)
            similarity_matrix = np.dot(query_embed, embeddings.T)

            # Find pairs where similarity exceeds threshold
            for i, row in enumerate(list_tuple):
                if similarity_matrix[0, i] >= self.threshold:
                    filtered_tuples.append(row)

            # Additional post-filtering based on LLM
            for tup in filtered_tuples:
                msg = self._construct_prompt_condition(tup)
                messages = [
                    {"role": "system",
                     "content": "You are a strict judge that outputs only 'true' or 'false' "
                                "with no punctuation or extra characters based on a given statement."},
                    {"role": "user", "content": msg}
                ]
                batched_messages.append(messages)
        elif isinstance(self.threshold, int):
            import faiss

            # Extract embeddings from tuples
            text_list = []
            instruction = "Embed the following tuples for semantic similarity:"
            for tup in iterator:
                entry_vals = []
                for col_idx in self.tab_col_idxs:
                    entry_vals.append(tup[col_idx])
                text_list.append(f"{instruction} {str(*entry_vals)}")
                list_tuple.append(tup)

            query_instr = [self.condition.format(*[""] * len(self.tab_col_idxs))]
            query_embed = np.array(self.embedding_model.generate_embeddings(query_instr, normalize_embeddings=True),
                                   dtype=np.float32)
            embeddings = np.array(self.embedding_model.generate_embeddings(text_list, normalize_embeddings=True),
                                  dtype=np.float32)

            # Build FAISS index for embeddings
            d = embeddings.shape[1]  # Embedding dimension
            index = faiss.IndexFlatIP(d)  # Inner product index
            index.add(embeddings)  # Add rows to the index

            # Search for top k
            _, indices = index.search(query_embed, self.threshold)
            for j in range(self.threshold):
                filtered_tuples.append(list_tuple[indices[0][j]])

            for tup in filtered_tuples:
                msg = self._construct_prompt_condition(tup)
                messages = [
                    {"role": "system",
                     "content": "You are a strict judge that outputs only 'true' or 'false' "
                                "with no punctuation or extra characters based on a given statement."},
                    {"role": "user", "content": msg}
                ]
                batched_messages.append(messages)
        else:
            for tup in iterator:
                msg = self._construct_prompt_condition(tup)
                messages = [
                    {"role": "system",
                     "content": "You are a strict judge that outputs only 'true' or 'false' with "
                                "no punctuation or extra characters based on a given statement."},
                    {"role": "user", "content": msg}
                ]
                filtered_tuples.append(tup)
                batched_messages.append(messages)

        response_list = []
        for messages in batched_messages:
            response_list.append(self.client_model.complete_messages(messages, max_tokens=8))
        for tup, response in zip(filtered_tuples, response_list):
            if response.lower() == "true":
                yield tup

    def next(self):
        for filtered_tuple in self.filter(self.children[0].next()):
            yield filtered_tuple


class SemanticTransform(PhysicalOperator):
    """Physical operator for processing semantic target list with prompts"""

    def __init__(self, columns):
        """
        Args:
            columns: List of target columns including prompts
        """
        super().__init__('SemanticTransform')
        self.columns = []
        self.children_columns = None
        for col in columns:
            if isinstance(col, FunctionColumn):
                for fc in col.columns:
                    self.columns.append(fc)
            else:
                self.columns.append(col)
        self.transform_columns = []
        self._transform_method = None
        self._initialize_transform_method()

        self.has_init_models = False
        self.client_model = None
        self.embedding_model = None
        self.stream = None
        self.result_tuples = []
        self.buffered_projected_input = [[] for _ in range(len(self.transform_columns))]
        self.projection_idxs = []
        self.target_transform_idxs = []  # for transform columns only
        self.target_child_idxs = []  # for child columns only

    def _initialize_transform_method(self):
        need_clustering = False
        for col in self.columns:
            if isinstance(col, SemanticTransformColumn):
                self.transform_columns.append(col)
                if col.k is None or col.k != 1:
                    need_clustering = True
            elif isinstance(col, PromptColumn):
                self.transform_columns.append(col)
        
        if need_clustering:
            try:
                import faiss
                import numpy as np
                # Num few-shot examples
                self.shot_examples = 10
                self._transform_method = self._next_internal_embed_then_cluster
            except ImportError:
                self.answer_choice_fld = "answer_choice"
                self._transform_method = self._next_internal_naive_mapping
        else:
            self._transform_method = self._next_internal_transform_tuples

    def open(self):
        """Initialize the operator"""
        if len(self.children) != 1:
            raise ValueError("SemanticTransform requires exactly one input operator")
        self.children[0].open()
        self.children_columns = self.children[0].columns
        self._get_projection_columns_indices()

    def _get_projection_columns_indices(self):
        # Convert PromptColumn into SemanticTransformColumn now that we know the childrens are
        for i in range(len(self.transform_columns)):
            cur_col = self.transform_columns[i]
            if isinstance(cur_col, PromptColumn):
                self.transform_columns[i] = SemanticTransformColumn(table_name=cur_col.table_name,
                                                                    original_columns=self.children_columns,
                                                                    target_column = cur_col.column_name,
                                                                    prompt_text=cur_col.prompt_text)
        
        column_name_to_idx = {col.column_name: i for i, col in enumerate(self.children_columns)}
        target_col_name_to_idx = {col.column_name: i for i, col in enumerate(self.columns)}

        for i, col in enumerate(self.columns):
            if isinstance(col, SemanticTransformColumn):
                # Map original_columns to indices in self.columns
                self.projection_idxs.append([
                    column_name_to_idx[source_col] for source_col in col.original_columns
                ])

                # Determine target index for the transformed column
                self.target_transform_idxs.append(target_col_name_to_idx[col.column_name])
            elif isinstance(col, PromptColumn):
                # Considers all columns from children columns
                self.projection_idxs.append(list(range(len(self.children_columns))))
                self.target_transform_idxs.append(target_col_name_to_idx[col.column_name])
            else:
                self.target_child_idxs.append((i, column_name_to_idx[col.column_name]))

    def _transform_tuples(self):
        """Simple map/extract using prompting"""
        prompt_system = """You are a helpful assistant that follows the instruction provided by the user."""

        for idx, col in enumerate(self.transform_columns):
            json_array = self.buffered_projected_input[idx]

            # Transform
            batched_messages = []
            for json_obj in json_array:
                messages_transform = [
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"""
                    Without explanation, transform the following JSON object into a new single JSON object with field '{col.column_name}' by following this instruction: {col.prompt_text}.

                    JSON object:
                    {str(json_obj)}
                    """}
                ]
                batched_messages.append(messages_transform)

            response_list = []
            for messages in batched_messages:
                response_list.append(self.client_model.complete_messages(messages))            
            answer_json = []
            for response in response_list:
                if col.column_name == 'prompt':
                    answer_json.extend([{col.column_name: str(response)}])
                else:    
                    answer_json.extend(_parse_json(response))

            # Check length TODO: Log misalignment?
            if len(answer_json) > len(self.result_tuples):
                # Truncate, but should we put warning?
                answer_json = answer_json[:len(self.result_tuples)]
            elif len(answer_json) < len(self.result_tuples):
                # Unexpected, leaving rest to None, should we put warning?
                answer_json.extend([{col.column_name: None}] * (len(self.result_tuples) - len(answer_json)))

                # Modify answer
            for i, answer in enumerate(answer_json):
                if self.target_transform_idxs[idx] < len(self.result_tuples[i]):
                    self.result_tuples[i][self.target_transform_idxs[idx]] = answer.get(col.column_name, None)
                else:
                    raise IndexError("Unexpected indexing error when appending answer")

    def _generate_mcq_options(self, clustered_categories):
        def index_to_letter(index):
            """Convert an index to an alphabetic label (A, B, ..., AA, AB, ...)."""
            letters = []
            while index >= 0:
                letters.append(chr(65 + (index % 26)))  # Convert to A-Z
                index = index // 26 - 1
            return ''.join(reversed(letters))

        # Split the categories into a list
        categories = [cat.strip() for cat in clustered_categories.split(",")]

        # Generate Multiple-Choice Question (MCQ) options
        mcq_options = []
        choice_to_category = {}
        for i, category in enumerate(categories):
            choice = index_to_letter(i)  # Generate letter-based choice
            mcq_options.append(f"{choice}. {category}")
            choice_to_category[choice] = category

        # Join the MCQ options into a single string
        mcq_options_str = "\n".join(mcq_options)

        return mcq_options_str, choice_to_category

    def _append_answer_through_options(self, response, choice_to_category, index):
        answer_json = _parse_json(response)
        # Check length TODO: Log misalignment?
        if len(answer_json) > len(self.result_tuples):
            # Truncate, but should we put warning?
            answer_json = answer_json[:len(self.result_tuples)]
        elif len(answer_json) < len(self.result_tuples):
            # Unexpected, leaving rest to None, should we put warning?
            answer_json.extend([{self.answer_choice_fld: None}] * (len(self.result_tuples) - len(answer_json)))

            # Modify answer
        for i, opt_answer in enumerate(answer_json):
            ans = opt_answer[self.answer_choice_fld]
            if ans is not None:
                ans = ans.upper()
                actual_answer = choice_to_category.get(ans, None)
            else:
                actual_answer = None
            if self.target_transform_idxs[index] < len(self.result_tuples[i]):
                self.result_tuples[i][self.target_transform_idxs[index]] = actual_answer
            else:
                raise IndexError("Unexpected indexing error when appending answer")

    def _convert_tuples_to_json(self, transform_idx):
        column_names = [self.columns[j].column_name for j in self.projection_idxs[transform_idx]]
        return [
            {name: tup[j] for name, j in zip(column_names, self.projection_idxs[transform_idx])}
            for tup in self.result_tuples
        ]

    def _embed_then_cluster(self, embed_list):
        for idx, (col, embeddings) in enumerate(zip(self.transform_columns, embed_list)):
            # TODO: Edge case when K is greater or equal than the number of inputs (clustering is not possible);
            #  should we log warning?
            if col.k < len(self.result_tuples):
                json_array = self.buffered_projected_input[idx]

                # Initialize FAISS for K-means clustering
                if not isinstance(embeddings, np.ndarray):
                    embeddings_array = np.array(embeddings)
                else:
                    embeddings_array = embeddings
                d = embeddings_array.shape[1]
                k = col.k if col.k else 3  # TODO: Dynamically adjust the k, maybe with DBScan
                kmeans = faiss.Kmeans(d, k, niter=20, verbose=True)
                kmeans.train(embeddings_array)

                # Get cluster assignments and distances
                distances, cluster_assignments = kmeans.index.search(embeddings_array, 1)
                cluster_assignments = cluster_assignments.flatten()

                # Organize data into clusters with distances
                clusters = defaultdict(list)

                for local_idx, (cluster_id, distance) in enumerate(zip(cluster_assignments, distances)):
                    clusters[cluster_id].append((json_array[local_idx], distance[0]))
                
                # Get representative examples (closest to centroid)
                cluster_examples = {}
                for cluster_id in clusters:
                    # Sort by distance ascending (smallest first)
                    sorted_items = sorted(clusters[cluster_id], key=lambda x: x[1])
                    cluster_examples[cluster_id] = [item[0] for item in
                                                    sorted_items[:self.shot_examples]]  # Get few-shot examples

                # Generate categories using LLM
                prompt_system = f"""
                You are a data categorization expert.
                Classify items into {col.k} distinct, non-overlapping categories based on their
                semantic similarity and this context: "{col.prompt_text}". Consider both specific and general patterns."""

                # Build cluster examples string
                clusters_str = "\n\n".join(
                    f"Cluster {cid} examples:\n" + "\n".join(f"- {ex}" for ex in ex_list)
                    for cid, ex_list in sorted(cluster_examples.items())
                )

                messages_cluster = [
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"""
                    {clusters_str}
                    
                    Analyze these {col.k} clusters of items and suggest a distinct category name for each without any explanation.
                    The context behind such clustering is: "{col.prompt_text}"

                    Rules:
                    1. Use clear, specific names
                    2. Maintain parallel structure
                    3. No overlapping categories
                    
                    Output: Comma-separated names (one per cluster) without any explanations and maintain order starting from cluster 0 first.
                    Example output format: Technology, Sports Apparel, Healthcare Devices
                    
                    Your categories:"""}
                ]

                clustered_categories = self.client_model.complete_messages(messages_cluster)
                categories = [cat.strip() for cat in clustered_categories.split(",")]

                # Based on the K-means result, assign based on the cluster id
                for i, cluster_id in enumerate(cluster_assignments):
                    if self.target_transform_idxs[idx] < len(self.result_tuples[i]):
                        self.result_tuples[i][self.target_transform_idxs[idx]] = categories[cluster_id]
                    else:
                        raise IndexError("Unexpected indexing error when appending answer")

    def _naive_mapping(self):
        """Clustering, but just prompt the whole thing"""

        prompt_system = """You are a helpful assistant that follows the instruction provided by the user."""

        for idx, col in enumerate(self.transform_columns):
            # TODO: Edge case when K is greater or equal than the number of inputs (clustering is not possible);
            #  should we log warning?
            if col.k < len(self.result_tuples):
                json_array = self.buffered_projected_input[idx]

                # Clustering prompt
                messages_cluster = [
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"""
                    Without any explanation, given the following JSON array of objects, come up with {col.k} distinct categories.
                    Each category should represent a meaningful grouping based on this context: "{col.prompt_text}".
                    Output only the {col.k} category names separated by commas.
                    
                    JSON array:
                    {str(json_array)}
                    """}
                ]
                clustered_categories = self.client_model.complete_messages(messages_cluster)
                mcq_options_str, choice_to_category = self._generate_mcq_options(clustered_categories)

                # Classification through MCQ
                messages_classify = [
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"""
                    Without any explanation, given the following JSON array of objects, classify each object
                    by choosing only one from the given alphabet choices (e.g., A, B, C), based on the
                    following context: {col.prompt_text}

                    Output only the answers in JSON format with field '{self.answer_choice_fld}'.
                    For example:
                    [
                        {{"{self.answer_choice_fld}": "A"}},
                        {{"{self.answer_choice_fld}": "B"}}
                    ]

                    Options:
                    {mcq_options_str}

                    JSON array:
                    {str(json_array)}
                    """}
                ]
                response = self.client_model.complete_messages(messages_classify)
                self._append_answer_through_options(response, choice_to_category, idx)

    def _next_internal_transform_tuples(self):
        # TODO: We don't need to do gather first and then project, we can directly do batch streaming
        if len(self.result_tuples) == 0:
            for input_tuple in self.children[0].next():
                input_tuple = list(input_tuple)

                # Project tuple based on target
                target_tuple = [None for _ in range(len(self.columns))]
                for i, child_idx in self.target_child_idxs:
                    target_tuple[i] = input_tuple[child_idx]
                self.result_tuples.append(target_tuple)

                for i in range(len(self.transform_columns)):
                    # Project tuple based on children
                    projected_tuple = [input_tuple[j] for j in self.projection_idxs[i]]
                    proj_json_entry = {}
                    for j, entry in enumerate(projected_tuple):
                        col_idx = self.projection_idxs[i][j]
                        proj_json_entry[self.children_columns[col_idx].column_name] = entry
                    self.buffered_projected_input[i].append(proj_json_entry)

            self._transform_tuples()

        for tup_ in self.result_tuples:
            yield tuple(tup_)

    def _next_internal_naive_mapping(self):
        if len(self.result_tuples) == 0:
            for input_tuple in self.children[0].next():
                input_tuple = list(input_tuple)

                # Project tuple based on target
                target_tuple = [None for _ in range(len(self.columns))]
                for i, child_idx in self.target_child_idxs:
                    target_tuple[i] = input_tuple[child_idx]
                self.result_tuples.append(target_tuple)

                for i in range(len(self.transform_columns)):
                    # Project tuple based on children
                    projected_tuple = [input_tuple[j] for j in self.projection_idxs[i]]
                    proj_json_entry = {}
                    for j, entry in enumerate(projected_tuple):
                        col_idx = self.projection_idxs[i][j]
                        proj_json_entry[self.children_columns[col_idx].column_name] = entry
                    self.buffered_projected_input[i].append(proj_json_entry)

            self._naive_mapping()

        for tup_ in self.result_tuples:
            yield tuple(tup_)

    def _next_internal_embed_then_cluster(self):
        import numpy as np

        if len(self.result_tuples) == 0:
            instruction_text_list = []
            for input_tuple in self.children[0].next():
                input_tuple = list(input_tuple)

                # Project tuple based on target
                target_tuple = [None for _ in range(len(self.columns))]
                for i, child_idx in self.target_child_idxs:
                    target_tuple[i] = input_tuple[child_idx]
                self.result_tuples.append(target_tuple)

                # Generate embedding based on required groupby specification
                for i in range(len(self.transform_columns)):
                    # Project tuple based on children
                    projected_tuple = [input_tuple[j] for j in self.projection_idxs[i]]
                    proj_json_entry = {}
                    for j, entry in enumerate(projected_tuple):
                        col_idx = self.projection_idxs[i][j]
                        proj_json_entry[self.children_columns[col_idx].column_name] = entry
                    self.buffered_projected_input[i].append(proj_json_entry)
                    str_proj_tuple = json.dumps(proj_json_entry)
                    instruction_text = f"""Extract the following JSON object based on the following context
                    Context: {self.transform_columns[i].prompt_text}
                    JSON object: {{
                        {str_proj_tuple}
                    }}
                    """
                    instruction_text_list.append(instruction_text.strip())

            embed_list = self.embedding_model.generate_embeddings(instruction_text_list)
            if not isinstance(embed_list, np.ndarray):
                embed_list = np.array(embed_list)

            embed_list = list(embed_list.reshape((len(self.transform_columns),
                                                  embed_list.shape[0] // len(self.transform_columns),
                                                  embed_list.shape[1])))

            self._embed_then_cluster(embed_list)

        for tup_ in self.result_tuples:
            yield tuple(tup_)

    def next(self):
        """
        Right now we are not supporting K = None (adaptive clustering)
        Process each input tuple with semantic prompts
        Returns:
            Generator yielding processed tuples
        """
        if not self.has_init_models:
            self.has_init_models = True
            self.client_model = default_client_model()
            self.embedding_model = default_embedding_model()

        yield from self._transform_method()

    def close(self):
        """Clean up resources"""
        if self.stream:
            try:
                self.stream.close()
            except:
                pass
        self.stream = None
        self.children[0].close()


class SemanticJoin(PhysicalOperator):
    def __init__(self, condition, join_type, children_table_names):
        """
        Args:
            schema: Schema of the table.
        """
        super().__init__('SemanticJoin')
        self.columns = []
        self.children_columns = []
        self.condition = condition.condition
        self.threshold = condition.threshold
        self.join_table_columns = condition.table_columns
        self.join_type = join_type
        self.children_table_names = children_table_names
        self.has_init_models = False
        self.client_model = None
        self.embedding_model = None
        self.tab_col_idxs = []  # Format: (tuple of (child_idx, column_idx))

    def _get_proj_index(self):
        # Iterate over which children and which column index to be joined with; for now assume only left and right
        for join_col in self.join_table_columns:
            match_found = False
            for child_idx, child_table_name in enumerate(self.children_table_names):
                if join_col.table_name == child_table_name:
                    for col_idx, col in enumerate(self.children_columns[child_idx]):
                        if join_col == col:  # Compare TableColumn with TableColumn
                            self.tab_col_idxs.append((child_idx, col_idx))
                            match_found = True
                            break

                    if match_found:
                        break

            # If no match was found for some reason
            if not match_found:
                raise ValueError(f"Column '{join_col.column_name}' not found in table '{join_col.table_name}'.")

    def open(self):
        if len(self.children) <= 1:
            raise ValueError("SemanticJoin requires more than one input operator")

        for child in self.children:
            child.open()
            self.columns.extend(child.columns)
            self.children_columns.append(child.columns)
        self._get_proj_index()
        super().open()

    def _construct_prompt_condition(self, left_row, right_row):
        # Condition is already a string that just needs to be formatted
        entry_vals = []
        for child_idx, col_idx in self.tab_col_idxs:
            if child_idx == 0:
                entry_vals.append(left_row[col_idx])
            else:
                entry_vals.append(right_row[col_idx])

        return self.condition.format(*tuple(entry_vals))

    def next(self):
        """
        Process whole document with semantic prompts
        Returns:
            Dataframe
        """
        if not self.has_init_models:
            self.has_init_models = True
            self.client_model = default_client_model()
            self.embedding_model = default_embedding_model()

        # Materialize everything for batch processing
        cached_left_rows = []
        cached_right_rows = []
        for left_row in self.children[0].next():
            cached_left_rows.append(left_row)

        for right_row in self.children[1].next():
            cached_right_rows.append(right_row)

        # If threshold is specified, do a pre-filtering first
        batched_messages = []
        joined_results = []
        if isinstance(self.threshold, float):
            left_entries = []
            right_entries = []

            for left_row in cached_left_rows:
                proj_row = []
                for child_idx, col_idx in self.tab_col_idxs:
                    if child_idx == 0:
                        proj_row.append(left_row[col_idx])
                left_entries.append(proj_row)

            for right_row in cached_right_rows:
                proj_row = []
                for child_idx, col_idx in self.tab_col_idxs:
                    if child_idx != 0:
                        proj_row.append(right_row[col_idx])
                right_entries.append(proj_row)

            instruction = "Embed the following tuples for semantic similarity:"
            left_text_list = [f"{instruction} {str(*left_row)}" for left_row in left_entries]
            right_text_list = [f"{instruction} {str(*right_row)}" for right_row in right_entries]

            # Compute cosine similarity matrix
            left_embeddings = np.array(
                self.embedding_model.generate_embeddings(left_text_list, normalize_embeddings=True), dtype=np.float32)
            right_embeddings = np.array(
                self.embedding_model.generate_embeddings(right_text_list, normalize_embeddings=True), dtype=np.float32)
            similarity_matrix = np.dot(left_embeddings, right_embeddings.T)

            # Find pairs where similarity exceeds threshold
            for i, left_row in enumerate(cached_left_rows):
                for j, right_row in enumerate(cached_right_rows):
                    if similarity_matrix[i, j] >= self.threshold:
                        joined_results.append((left_row, right_row))

            # Additional post-filtering based on LLM
            for left_row, right_row in joined_results:
                msg = self._construct_prompt_condition(left_row, right_row)
                messages = [
                    {"role": "system",
                     "content": "You are a strict judge that outputs only 'true' or 'false' "
                                "with no punctuation or extra characters based on a given statement."},
                    {"role": "user", "content": msg}
                ]
                batched_messages.append(messages)
        elif isinstance(self.threshold, int):
            import faiss

            # Extract embeddings from tuples
            left_entries = []
            right_entries = []

            for left_row in cached_left_rows:
                proj_row = []
                for child_idx, col_idx in self.tab_col_idxs:
                    if child_idx == 0:
                        proj_row.append(left_row[col_idx])
                left_entries.append(proj_row)

            for right_row in cached_right_rows:
                proj_row = []
                for child_idx, col_idx in self.tab_col_idxs:
                    if child_idx != 0:
                        proj_row.append(right_row[col_idx])
                right_entries.append(proj_row)

            instruction = "Embed the following tuples for semantic similarity:"
            left_text_list = [f"{instruction} {str(*left_row)}" for left_row in left_entries]
            right_text_list = [f"{instruction} {str(*right_row)}" for right_row in right_entries]
            left_embeddings = np.array(
                self.embedding_model.generate_embeddings(left_text_list, normalize_embeddings=True), dtype=np.float32)
            right_embeddings = np.array(
                self.embedding_model.generate_embeddings(right_text_list, normalize_embeddings=True), dtype=np.float32)

            # Build FAISS index for right embeddings
            d = right_embeddings.shape[1]  # Embedding dimension
            index = faiss.IndexFlatIP(d)  # Inner product index
            index.add(right_embeddings)  # Add right rows to the index

            # Search for right row
            _, indices = index.search(left_embeddings, self.threshold)

            # Filter results based on threshold
            for i, left_row in enumerate(cached_left_rows):
                for j in range(self.threshold):
                    right_row = cached_right_rows[indices[i][j]]
                    joined_results.append((left_row, right_row))

            for left_row, right_row in joined_results:
                msg = self._construct_prompt_condition(left_row, right_row)
                print(msg)
                messages = [
                    {"role": "system",
                     "content": "You are a strict judge that outputs only 'true' or 'false' "
                                "with no punctuation or extra characters based on a given statement."},
                    {"role": "user", "content": msg}
                ]
                batched_messages.append(messages)
        else:
            for left_row in cached_left_rows:
                for right_row in cached_right_rows:
                    msg = self._construct_prompt_condition(left_row, right_row)
                    messages = [
                        {"role": "system",
                         "content": "You are a strict judge that outputs only 'true' or 'false' "
                                    "with no punctuation or extra characters based on a given statement."},
                        {"role": "user", "content": msg}
                    ]
                    batched_messages.append(messages)
                    joined_results.append((left_row, right_row))

        response_list = []
        for messages in batched_messages:
            response_list.append(self.client_model.complete_messages(messages, max_tokens=8))

        print(response_list)
        for i, response in enumerate(response_list):
            if response.lower() == "true":
                left_row, right_row = joined_results[i]
                yield left_row + right_row

    def close(self):
        """Clean up resources"""
        for child in self.children:
            child.close()
        super().close()


class SemanticScan(PhysicalOperator):
    """Physical operator for processing document into a proper table with prompts"""

    def __init__(self, target_columns, prompt_columns, filter=None):
        super().__init__('SemanticScan')
        self.columns = target_columns
        self._convert_prompt_columns_to_schema(prompt_columns)
        self.has_init_models = False
        self.client_model = None
        self._filter = filter
        self.document = None
        self.embeddings = None
        self.stream = None
        self.result_tuples = []

        # get type oids from catalog
        type_oids = []
        for column in self.columns:
            table_oid = CATALOG_ANDB_CLASS.get_relation_oid(
                column.table_name, session_vars.get_session_value('database_oid'))
            if table_oid == INVALID_OID:
                raise RuntimeError(f'table {column.table_name} not found.')

            attr = CATALOG_ANDB_ATTRIBUTE.get_table_attr(table_oid, column.column_name)
            if attr is None:
                raise RuntimeError(f'table {column.table_name} column {column.column_name} not found.')
            type_oids.append(attr.type_oid)

        # get type forms by type oids
        self.type_forms = [CATALOG_ANDB_TYPE.get_type_form_by_oid(type_oid) for type_oid in type_oids]

    def open(self):
        if len(self.children) != 1:
            raise ValueError("SemanticScan requires exactly one input operator")
        self.children[0].open()

        if self._filter:
            if isinstance(self._filter, Filter):
                columns = []
                for table_column in self.columns:
                    columns.append(table_column)
                self._filter.set_tuple_columns(columns, type_oids=None)
            elif isinstance(self._filter, SemanticFilter):
                self._filter.set_proj_index(self.columns)
        super().open()

    def _convert_prompt_columns_to_schema(self, prompt_columns):
        # Extract schema from PromptColumns
        schema_lines = []

        for column in prompt_columns:
            if isinstance(column, PromptColumn):
                schema_lines.append(f"{column.column_name}: Extract '{column.prompt_text}'")

        # Combine into a formatted schema
        self.schema = "\n".join(schema_lines)

    def _parse_json_into_tuples(self, raw_output):
        cleaned_json = _parse_json(raw_output)
        if len(cleaned_json) == 0:
            return []
        elif not isinstance(cleaned_json, list):
            cleaned_json = [cleaned_json]

        # Extract column names (keys of the first dictionary)
        table_tuples = [
            tuple(
                type_form.format_value(item.get(col.column_name, None))
                for type_form, col in zip(self.type_forms, self.columns)
            )
            for item in cleaned_json
        ]

        return table_tuples

    def _extract_document(self):
        # Decide with LLM whether to process the text as a whole or line by line
        prompt_system = """
        You are a data extraction assistant.
        Your task is to extract structured information from unstructured text and format it into JSON.
        If some information is not explicitly present in the text, infer it based on general knowledge.
        Adhere to the provided schema exactly.
        """

        batched_messages = []
        for chunk in self.document:
            # Construct the LLM prompt for the current line
            messages = [
                {"role": "system", "content": prompt_system},
                {"role": "user", "content": f"""
                Raw text:
                ```{chunk}```
                 
                Schema:
                {self.schema}
                
                Your task is to extract structured information from unstructured 'Raw text' by adhering to the following schema:
                {self.schema}
                
                Requirements:
                - Ensure that each instance is represented as a separate JSON object.
                - Format the final output as a JSON array containing these objects.
                - If some information is not explicitly present in the text, infer it based on general knowledge.
                - Do not include any additional text or explanations.
                """}
            ]
            batched_messages.append(messages)

        for messages in batched_messages:
            self.result_tuples.extend(self._parse_json_into_tuples(self.client_model.complete_messages(messages)))

    def next(self):
        """
        Process whole document with semantic prompts
        Returns:
            Dataframe
        """
        if not self.has_init_models:
            self.has_init_models = True
            self.client_model = default_client_model()

        if len(self.result_tuples) == 0:
            self.document = []
            for doc in self.children[0].next():
                self.document.append(doc)

            # Get appropriate response based on extraction strategy
            self._extract_document()

        if self._filter:
            for filtered_tup in self._filter.filter(iter(self.result_tuples)):
                yield filtered_tup
        else:
            for tup in self.result_tuples:
                yield tup

    def close(self):
        """Clean up resources"""
        self.children[0].close()
        super().close()
