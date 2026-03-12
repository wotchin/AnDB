"""Unit tests for Semantic Cost Model."""

import pytest

from andb.sql.optimizer.semantic_cost_model import SemanticCostModel


@pytest.fixture
def cost_model():
    return SemanticCostModel()


def test_compute_exec_cost_llm_filter(cost_model):
    cost = cost_model.compute_exec_cost('LLMFilterScan', 100)
    assert cost == 100 * 100.0  # N * C_LLM


def test_compute_exec_cost_codegen_filter(cost_model):
    cost = cost_model.compute_exec_cost('CodegenFilterScan', 100)
    assert cost == 100.0 + 100 * 0.01  # C_LLM + N * C_CODE


def test_compute_exec_cost_embedding_filter(cost_model):
    cost = cost_model.compute_exec_cost('EmbeddingFilterScan', 100)
    assert cost == 100 * 1.0  # N * C_EMBED


def test_compute_exec_cost_hybrid_filter(cost_model):
    cost = cost_model.compute_exec_cost('HybridFilterScan', 100, selectivity=0.1)
    expected = 100 * 1.0 + 0.1 * 100 * 100.0  # N*C_EMBED + sel*N*C_LLM
    assert cost == expected


def test_get_accuracy_score(cost_model):
    assert cost_model.get_accuracy_score('LLMFilterScan') == 1.0
    assert cost_model.get_accuracy_score('CodegenFilterScan') == 0.75
    assert cost_model.get_accuracy_score('EmbeddingFilterScan') == 0.85
    assert cost_model.get_accuracy_score('HybridFilterScan') == 0.95
    assert cost_model.get_accuracy_score('NestedLoopSemanticJoin') == 1.0
    assert cost_model.get_accuracy_score('IndexedNLSemanticJoin') == 0.90
    assert cost_model.get_accuracy_score('HashSemanticJoin') == 0.87
    assert cost_model.get_accuracy_score('HashBasedSemanticAggregation') == 0.95
    assert cost_model.get_accuracy_score('EmbeddingClusterAggregation') == 0.80


def test_compute_total_cost(cost_model):
    cost = cost_model.compute_cost('LLMFilterScan', 100, lambda_acc=1.0)
    exec_cost = 100 * 100.0
    acc_loss = 100 * (1 - 1.0)
    assert cost == exec_cost + 1.0 * acc_loss


def test_select_best_filter_small_cardinality(cost_model):
    """For small N, codegen should win (low exec cost)."""
    best_op, best_cost = cost_model.select_best_filter(
        input_cardinality=5, lambda_acc=0.0  # ignore accuracy
    )
    # With lambda_acc=0, pure execution cost matters
    # CodegenFilterScan: 100 + 5*0.01 = 100.05
    # EmbeddingFilterScan: 5*1 = 5.0
    assert best_op == 'EmbeddingFilterScan'


def test_select_best_filter_with_accuracy(cost_model):
    """With very high accuracy weight, LLMFilterScan should win."""
    best_op, _ = cost_model.select_best_filter(
        input_cardinality=5, lambda_acc=100000.0
    )
    # LLMFilterScan: exec=500, acc_loss=0 -> total=500
    # HybridFilterScan: exec=55, acc_loss=5*0.05*100000=25000 -> total=25055
    # At extreme lambda, zero accuracy loss wins
    assert best_op == 'LLMFilterScan'


def test_select_best_join(cost_model):
    best_op, _ = cost_model.select_best_join(
        outer_cardinality=10, inner_cardinality=10,
        lambda_acc=0.0
    )
    # With lambda_acc=0, only exec cost matters
    # NLSJ: 10*10*100 = 10000
    # HSJ: (10+10)*1 + 0.1*10*10*100 = 20 + 1000 = 1020
    assert best_op == 'HashSemanticJoin'


def test_select_best_aggregation(cost_model):
    best_op, _ = cost_model.select_best_aggregation(
        input_cardinality=100, lambda_acc=0.0, n_clusters=5
    )
    # HashBased: 100*100 = 10000
    # EmbeddingCluster: 100*1 + 5*100 = 600
    assert best_op == 'EmbeddingClusterAggregation'


def test_custom_cost_constants():
    model = SemanticCostModel(c_llm=50.0, c_embed=2.0)
    cost = model.compute_exec_cost('LLMFilterScan', 10)
    assert cost == 10 * 50.0


def test_unknown_operator(cost_model):
    score = cost_model.get_accuracy_score('UnknownOp')
    assert score == 0.5  # conservative default

    cost = cost_model.compute_exec_cost('UnknownOp', 10)
    assert cost == 10 * 100.0  # defaults to N * C_LLM


def test_join_exec_cost_nlsj(cost_model):
    cost = cost_model.compute_exec_cost('NestedLoopSemanticJoin', 50,
                                         inner_cardinality=30)
    assert cost == 50 * 30 * 100.0


def test_join_exec_cost_inlsj(cost_model):
    cost = cost_model.compute_exec_cost('IndexedNLSemanticJoin', 50,
                                         k_candidates=5)
    assert cost == 50 * (1.0 + 5 * 100.0)


def test_agg_exec_cost_embedding_cluster(cost_model):
    cost = cost_model.compute_exec_cost('EmbeddingClusterAggregation', 100,
                                         n_clusters=10)
    assert cost == 100 * 1.0 + 10 * 100.0
