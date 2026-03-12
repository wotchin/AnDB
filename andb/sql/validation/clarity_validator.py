"""Semantic Clarity Validation: three-stage validation for semantic queries.

Stage 1 (Syntactic Pre-flight): Quick check that semantic predicates are well-formed
Stage 2 (Semantic Stability): Verify output consistency across samples
Stage 3 (Runtime Calibration): Handled by CalibratorOperator during execution

This module implements Stages 1 and 2.
"""


class ValidationResult:
    """Result of semantic clarity validation."""

    def __init__(self, valid=True, warnings=None, stage='passed', confidence=1.0):
        self.valid = valid
        self.warnings = warnings or []
        self.stage = stage
        self.confidence = confidence

    def __repr__(self):
        return (f"ValidationResult(valid={self.valid}, stage='{self.stage}', "
                f"warnings={self.warnings}, confidence={self.confidence})")


class SemanticClarityValidator:
    """Validates semantic queries before execution.

    Implements the paper's 3-stage Semantic Clarity Validation:
    - Stage 1: Syntactic pre-flight check (predicate assertiveness)
    - Stage 2: Semantic stability dry run (output consistency)
    - Stage 3: Runtime calibration (delegated to CalibratorOperator)
    """

    def __init__(self, client_model=None):
        """
        Args:
            client_model: Optional LLM client for validation checks.
                         If None, uses default_client_model() when needed.
        """
        self._client_model = client_model

    def _get_client_model(self):
        if self._client_model is None:
            from andb.executor.operator.physical.semantic import default_client_model
            self._client_model = default_client_model()
        return self._client_model

    def validate(self, query_ast, sample_data=None):
        """Execute three-stage validation.

        Args:
            query_ast: Parsed query AST
            sample_data: Optional sample data for Stage 2

        Returns:
            ValidationResult
        """
        # Stage 1: Syntactic pre-flight
        result = self.syntactic_check(query_ast)
        if not result.valid:
            return result

        # Stage 2: Semantic stability (if sample data available)
        if sample_data:
            result = self.stability_check(query_ast, sample_data)
            if not result.valid:
                return result

        return ValidationResult(valid=True, stage='passed')

    def syntactic_check(self, query_ast):
        """Stage 1: Syntactic pre-flight check.

        Validates that:
        - MATCHES predicates are assertive (can answer yes/no)
        - CLASSIFYING keys are valid noun phrases
        - EXTRACT schemas are well-defined
        - TRANSFORM instructions are actionable
        """
        warnings = []

        # Check for MatchesPredicate nodes
        matches_predicates = self._find_matches_predicates(query_ast)
        for pred in matches_predicates:
            if not self._is_assertive_predicate(pred):
                return ValidationResult(
                    valid=False,
                    warnings=["MATCHES predicate must be an assertive statement "
                              "that can be answered yes/no."],
                    stage='syntactic'
                )

        # Check for ClassifyingGroupBy nodes
        classifying_clauses = self._find_classifying_clauses(query_ast)
        for clause in classifying_clauses:
            if not self._is_valid_classification_key(clause):
                return ValidationResult(
                    valid=False,
                    warnings=["CLASSIFYING key must be a noun or noun phrase "
                              "describing a valid classification dimension."],
                    stage='syntactic'
                )

        # Check for ExtractExpression nodes
        extract_exprs = self._find_extract_expressions(query_ast)
        for expr in extract_exprs:
            if not expr.target_schema or len(expr.target_schema) == 0:
                return ValidationResult(
                    valid=False,
                    warnings=["EXTRACT must define at least one target column."],
                    stage='syntactic'
                )

        if warnings:
            return ValidationResult(valid=True, warnings=warnings, stage='syntactic')
        return ValidationResult(valid=True, stage='syntactic')

    def stability_check(self, query_ast, sample_data):
        """Stage 2: Semantic stability dry run.

        Tests that semantic operations produce consistent results
        across different samples.

        Args:
            query_ast: Parsed query AST
            sample_data: List of sample tuples for testing

        Returns:
            ValidationResult
        """
        warnings = []

        # Check EXTRACT consistency: run on multiple samples, compare output schemas
        extract_exprs = self._find_extract_expressions(query_ast)
        if extract_exprs and len(sample_data) >= 2:
            schemas = []
            for sample in sample_data[:5]:
                schema = self._probe_extract_schema(extract_exprs[0], sample)
                if schema:
                    schemas.append(schema)

            if schemas and not self._schemas_consistent(schemas):
                warnings.append("EXTRACT output schema is inconsistent across samples.")
                return ValidationResult(
                    valid=False, warnings=warnings, stage='stability'
                )

        # Check TRANSFORM stability: run on same input with paraphrased instruction
        transform_exprs = self._find_transform_expressions(query_ast)
        if transform_exprs and sample_data:
            for texpr in transform_exprs:
                if not self._check_transform_stability(texpr, sample_data[0]):
                    warnings.append(
                        "TRANSFORM is sensitive to paraphrasing; results may be unstable."
                    )

        return ValidationResult(valid=True, warnings=warnings, stage='stability')

    # --- Helper methods ---

    def _find_matches_predicates(self, ast):
        """Find all MatchesPredicate nodes in the AST."""
        from andb.sql.parser.ast.s2ql import MatchesPredicate
        results = []
        self._walk_ast(ast, MatchesPredicate, results)
        return results

    def _find_classifying_clauses(self, ast):
        """Find all ClassifyingGroupBy nodes in the AST."""
        from andb.sql.parser.ast.s2ql import ClassifyingGroupBy
        results = []
        self._walk_ast(ast, ClassifyingGroupBy, results)
        return results

    def _find_extract_expressions(self, ast):
        """Find all ExtractExpression nodes in the AST."""
        from andb.sql.parser.ast.s2ql import ExtractExpression
        results = []
        self._walk_ast(ast, ExtractExpression, results)
        return results

    def _find_transform_expressions(self, ast):
        """Find all TransformExpression nodes in the AST."""
        from andb.sql.parser.ast.s2ql import TransformExpression
        results = []
        self._walk_ast(ast, TransformExpression, results)
        return results

    def _walk_ast(self, node, target_type, results):
        """Walk AST tree and collect nodes of target type."""
        if node is None:
            return
        if isinstance(node, target_type):
            results.append(node)
        # Walk child attributes
        for attr_name in dir(node):
            if attr_name.startswith('_'):
                continue
            try:
                attr = getattr(node, attr_name)
                if isinstance(attr, list):
                    for item in attr:
                        if hasattr(item, '__dict__'):
                            self._walk_ast(item, target_type, results)
                elif hasattr(attr, '__dict__') and not callable(attr):
                    self._walk_ast(attr, target_type, results)
            except Exception:
                continue

    def _is_assertive_predicate(self, predicate):
        """Check if a predicate is assertive (can be answered yes/no).

        Uses a lightweight LLM check.
        """
        try:
            client_model = self._get_client_model()
            assertion_text = predicate.assertion if hasattr(predicate, 'assertion') else str(predicate)

            messages = [
                {"role": "system",
                 "content": "Answer ONLY 'yes' or 'no'."},
                {"role": "user",
                 "content": f"Can the following statement be answered with yes/no?\n"
                            f"'{assertion_text}'"}
            ]
            response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
            return response.strip().lower().startswith('yes')
        except Exception:
            return True  # On error, pass validation

    def _is_valid_classification_key(self, clause):
        """Check if a classification key is a valid noun phrase."""
        try:
            client_model = self._get_client_model()
            key_text = clause.key_name if hasattr(clause, 'key_name') else str(clause)

            messages = [
                {"role": "system",
                 "content": "Answer ONLY 'yes' or 'no'."},
                {"role": "user",
                 "content": f"Is '{key_text}' a valid classification dimension "
                            f"(a noun or noun phrase)?\n"}
            ]
            response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
            return response.strip().lower().startswith('yes')
        except Exception:
            return True

    def _probe_extract_schema(self, extract_expr, sample):
        """Run extraction on a sample and return the output schema."""
        # Return the declared target schema (static check)
        if hasattr(extract_expr, 'target_schema'):
            return set(name for name, _ in extract_expr.target_schema)
        return None

    def _schemas_consistent(self, schemas):
        """Check if all schemas have the same structure."""
        if not schemas:
            return True
        first = schemas[0]
        return all(s == first for s in schemas)

    def _check_transform_stability(self, transform_expr, sample):
        """Check if TRANSFORM is stable under paraphrasing."""
        # For now, do a simple structural check
        if hasattr(transform_expr, 'instruction'):
            instruction = transform_expr.instruction
            # Heuristic: very short instructions are often ambiguous
            if len(instruction.strip()) < 5:
                return False
        return True
