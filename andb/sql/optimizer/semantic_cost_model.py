"""Semantic Cost Model: unified execution cost and accuracy loss.

Cost function: Cost(op) = C_exec(op, N) + lambda * N * (1 - A(op))

Where:
    C_exec: execution cost (time + money) in abstract cost units
    A(op): accuracy score in [0, 1]
    lambda: user-adjustable accuracy weight parameter
"""


class SemanticCostModel:
    """Semantic cost model that unifies execution cost and accuracy loss.

    For each physical operator, computes:
        total_cost = exec_cost + lambda_acc * input_cardinality * (1 - accuracy)
    """

    # Cost constants (can be overridden via configuration)
    C_LLM = 100.0      # Single LLM call cost (includes latency and monetary)
    C_EMBED = 1.0       # Single embedding computation cost
    C_CODE = 0.01       # Single code execution cost
    C_INDEX = 0.1       # Single index lookup cost
    C_HASH = 0.01       # Single hash operation cost

    # Operator type -> (cost_formula_key, base_accuracy)
    OPERATOR_PROFILES = {
        # Selection operators
        'LLMFilterScan': {'accuracy': 1.0},
        'CodegenFilterScan': {'accuracy': 0.75},
        'EmbeddingFilterScan': {'accuracy': 0.85},
        'HybridFilterScan': {'accuracy': 0.95},
        # Join operators
        'NestedLoopSemanticJoin': {'accuracy': 1.0},
        'IndexedNLSemanticJoin': {'accuracy': 0.90},
        'HashSemanticJoin': {'accuracy': 0.87},
        # Aggregation operators
        'HashBasedSemanticAggregation': {'accuracy': 0.95},
        'EmbeddingClusterAggregation': {'accuracy': 0.80},
        # Existing operators (mapped)
        'SemanticFilter': {'accuracy': 1.0},
        'SemanticJoin': {'accuracy': 1.0},
        'SemanticTransform': {'accuracy': 0.85},
    }

    def __init__(self, c_llm=None, c_embed=None, c_code=None, c_index=None, c_hash=None):
        if c_llm is not None:
            self.C_LLM = c_llm
        if c_embed is not None:
            self.C_EMBED = c_embed
        if c_code is not None:
            self.C_CODE = c_code
        if c_index is not None:
            self.C_INDEX = c_index
        if c_hash is not None:
            self.C_HASH = c_hash

    def compute_cost(self, operator_name, input_cardinality,
                     lambda_acc=1.0, **kwargs):
        """Compute total cost for an operator.

        Args:
            operator_name: Name of the physical operator
            input_cardinality: Estimated input row count N
            lambda_acc: Accuracy weight parameter (>= 0)
            **kwargs: Additional parameters (selectivity, k_candidates, etc.)

        Returns:
            Total cost as float
        """
        exec_cost = self.compute_exec_cost(operator_name, input_cardinality, **kwargs)
        acc = self.get_accuracy_score(operator_name)
        acc_loss = input_cardinality * (1 - acc)
        return exec_cost + lambda_acc * acc_loss

    def compute_exec_cost(self, operator_name, input_cardinality, **kwargs):
        """Compute pure execution cost C_exec.

        Args:
            operator_name: Name of the physical operator
            input_cardinality: Estimated input row count N
            **kwargs: selectivity, k_candidates, n_tables, inner_cardinality, etc.

        Returns:
            Execution cost as float
        """
        N = input_cardinality
        sel = kwargs.get('selectivity', 0.1)
        k = kwargs.get('k_candidates', 10)
        n_tables = kwargs.get('n_tables', 4)
        inner_card = kwargs.get('inner_cardinality', N)

        if operator_name == 'LLMFilterScan':
            return N * self.C_LLM
        elif operator_name == 'CodegenFilterScan':
            return self.C_LLM + N * self.C_CODE
        elif operator_name == 'EmbeddingFilterScan':
            return N * self.C_EMBED
        elif operator_name == 'HybridFilterScan':
            return N * self.C_EMBED + sel * N * self.C_LLM
        elif operator_name == 'NestedLoopSemanticJoin':
            return N * inner_card * self.C_LLM
        elif operator_name == 'IndexedNLSemanticJoin':
            return N * (self.C_EMBED + k * self.C_LLM)
        elif operator_name == 'HashSemanticJoin':
            return (N + inner_card) * self.C_EMBED + sel * N * inner_card * self.C_LLM
        elif operator_name == 'HashBasedSemanticAggregation':
            return N * self.C_LLM
        elif operator_name == 'EmbeddingClusterAggregation':
            n_clusters = kwargs.get('n_clusters', 10)
            return N * self.C_EMBED + n_clusters * self.C_LLM
        elif operator_name in ('SemanticFilter', 'SemanticJoin'):
            return N * self.C_LLM
        elif operator_name == 'SemanticTransform':
            return N * self.C_EMBED + kwargs.get('n_clusters', 5) * self.C_LLM
        else:
            # Unknown operator, return high cost
            return N * self.C_LLM

    def get_accuracy_score(self, operator_name):
        """Get the accuracy score A(op) in [0, 1].

        Initially uses static values; can be adapted via Calibrator data.
        """
        profile = self.OPERATOR_PROFILES.get(operator_name)
        if profile:
            return profile['accuracy']
        return 0.5  # conservative default

    def select_best_operator(self, candidates, input_cardinality,
                             lambda_acc=1.0, **kwargs):
        """Select the cost-optimal physical operator from candidates.

        Args:
            candidates: list of operator name strings
            input_cardinality: Estimated input row count
            lambda_acc: Accuracy weight
            **kwargs: Additional cost parameters

        Returns:
            (best_operator_name, best_cost) tuple
        """
        best_op = None
        best_cost = float('inf')

        for op_name in candidates:
            cost = self.compute_cost(op_name, input_cardinality,
                                     lambda_acc=lambda_acc, **kwargs)
            if cost < best_cost:
                best_cost = cost
                best_op = op_name

        return best_op, best_cost

    def select_best_filter(self, input_cardinality, lambda_acc=1.0,
                           has_vector_index=False, selectivity=0.1):
        """Select best semantic filter operator.

        Args:
            input_cardinality: N
            lambda_acc: accuracy weight
            has_vector_index: whether vector index exists
            selectivity: estimated selectivity

        Returns:
            (best_operator_name, best_cost)
        """
        candidates = ['LLMFilterScan', 'CodegenFilterScan',
                       'EmbeddingFilterScan', 'HybridFilterScan']
        return self.select_best_operator(
            candidates, input_cardinality,
            lambda_acc=lambda_acc, selectivity=selectivity
        )

    def select_best_join(self, outer_cardinality, inner_cardinality,
                         lambda_acc=1.0, has_vector_index=False,
                         k_candidates=10, selectivity=0.1):
        """Select best semantic join operator."""
        candidates = ['NestedLoopSemanticJoin', 'HashSemanticJoin']
        if has_vector_index:
            candidates.append('IndexedNLSemanticJoin')
        return self.select_best_operator(
            candidates, outer_cardinality,
            lambda_acc=lambda_acc,
            inner_cardinality=inner_cardinality,
            k_candidates=k_candidates,
            selectivity=selectivity
        )

    def select_best_aggregation(self, input_cardinality, lambda_acc=1.0,
                                n_clusters=10):
        """Select best semantic aggregation operator."""
        candidates = ['HashBasedSemanticAggregation', 'EmbeddingClusterAggregation']
        return self.select_best_operator(
            candidates, input_cardinality,
            lambda_acc=lambda_acc, n_clusters=n_clusters
        )
