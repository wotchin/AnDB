# AnDB (AI-Native Database) — 详细设计文档

## 1. 论文分析与不足之处

### 1.1 论文核心贡献
论文提出了 AnDB，一个 LLM-native 数据库框架，核心创新点包括：
- **语义关系代数 (SRA)**：扩展经典关系代数，引入语义选择、语义投影（提取+转换）、语义连接、语义聚合等算子
- **S²QL 查询语言**：基于 SQL 方言的声明式接口，通过 MATCHES、EXTRACT、TRANSFORM、CLASSIFYING 等关键字映射到 SRA 算子
- **语义直方图**：基于嵌入空间聚类的新型数据结构，用于语义谓词的基数估算
- **语义代价模型**：统一传统数据库代价与语义操作代价（执行成本 + 准确性损失）
- **多物理算子**：每个逻辑 SRA 算子对应多种物理实现策略，优化器可选择最优方案

### 1.2 论文不足分析

#### 1.2.1 理论层面不足
1. **SRA 形式化不完整**：语义聚合算子 `G_sem` 的分组键派生机制缺乏严格的数学定义；语义投影的 schema 推断规则未形式化
2. **代价模型过度简化**：准确性评分 `A(o_sem)` 被假设为静态预校准值，实际上它高度依赖数据分布、查询内容和 LLM 版本
3. **语义直方图假设过强**：假设嵌入空间中的聚类能有效近似语义谓词的选择性，但对于复杂逻辑谓词（如"not X and Y"），嵌入距离不能准确反映语义匹配
4. **缺少并发控制与事务语义**：论文未讨论语义操作在事务上下文中的一致性保证

#### 1.2.2 系统设计层面不足
1. **缺少错误恢复机制**：LLM 调用可能因超时、rate limiting 等原因失败，论文未讨论重试策略和容错机制
2. **批处理优化缺失**：当前设计中每个 tuple 独立调用 LLM，缺少 batching 优化策略
3. **缓存机制缺失**：相似或相同的语义查询结果未被缓存，导致重复计算
4. **多模态支持薄弱**：虽然提到 MultimodalInterpretation 算子，但未详细设计图像/音频到文本的转换流程
5. **分布式执行未考虑**：论文提到未来方向但未设计任何分布式语义处理框架

#### 1.2.3 实验评估不足
1. **数据集规模过小**：15 条书摘、16 条评论的实验无法证明系统在真实场景下的可扩展性
2. **缺少性能基准**：未包含端到端延迟分解、LLM token 消耗分析、代价模型准确性评估
3. **优化器效果未验证**：语义直方图和代价模型的效果仅通过理论分析，缺乏实验对比
4. **Calibrator 未实验验证**：trust-awareness 是核心卖点之一，但实验中完全未涉及

---

## 2. 代码库现状分析

### 2.1 已实现的组件

| 组件 | 状态 | 说明 |
|------|------|------|
| SQL Parser (基础) | ✅ 完成 | 支持标准 SQL DQL/DML/DDL，使用 sly 库 |
| SQL Parser (语义扩展-旧版) | ✅ 部分完成 | 支持 `PROMPT`, `FILE`, `DIRECTORY`, `TABULAR`, `SEM_MATCH`, `SEM_CLUSTER` |
| Logical Operators | ✅ 完成 | SemanticScan, SemanticTransform, SemanticJoin, SemanticCondition |
| Physical Operators - SemanticScan | ✅ 完成 | 文档到结构化数据的提取 |
| Physical Operators - SemanticFilter | ✅ 完成 | 基于 LLM 的语义过滤（支持嵌入预过滤） |
| Physical Operators - SemanticTransform | ✅ 完成 | 语义转换和聚类（K-Means + LLM） |
| Physical Operators - SemanticJoin | ✅ 完成 | 基于 LLM 的语义连接（支持嵌入预过滤） |
| AI Client Model | ✅ 完成 | 支持 DeepSeek API |
| Embedding Model | ✅ 完成 | 支持离线 SentenceTransformer |
| Stage Decomposition | ✅ 完成 | 支持 CTE、临时表、多阶段查询 |
| Schema Inference | ✅ 完成 | 使用 LLM 推断非结构化数据的 schema |
| 存储引擎 | ✅ 完成 | Heap 存储、B+树索引、缓冲池、WAL |
| 事务管理 | ✅ 完成 | Undo/Redo 日志 |

### 2.2 未实现的组件（论文中提到但代码缺失）

| 组件 | 状态 | 论文章节 |
|------|------|----------|
| S²QL 新语法（MATCHES, EXTRACT, TRANSFORM, CLASSIFYING） | ❌ 未实现 | §5.2 |
| CodegenFilterScan | ❌ 未实现 | §6 Table 2 |
| HybridFilterScan | ❌ 未实现 | §6 Table 2 |
| LLMProjectionWithCodegen | ❌ 未实现 | §6 Table 2 |
| IndexedNestedLoopSemanticJoin (INLSJ) | ❌ 未实现 | §6 Table 2 |
| HashSemanticJoin (HSJ) | ❌ 未实现 | §6 Table 2 |
| SortBasedSemanticAggregation | ❌ 未实现 | §6 Table 2 |
| EmbeddingCluster (独立算子) | ❌ 未实现 | §6 Table 2 |
| CalibratorOperator | ❌ 未实现 | §6 Table 2 |
| MultimodalInterpretation | ❌ 未实现 | §6 Table 2 |
| Semantic Histogram | ❌ 未实现 | §7.1 |
| Semantic Cost Model | ❌ 未实现 | §7.3 |
| Cost-Based Optimizer (语义) | ❌ 未实现 | §7 |
| Semantic Clarity Validation (3-stage) | ❌ 未实现 | §5.2.2 |

### 2.3 现有实现与论文的差异

1. **语法差异**：现有代码使用 `SEM_MATCH('...')`, `SEM_CLUSTER(...)`, `PROMPT(...)` 语法；论文定义的是 `MATCHES`, `EXTRACT INTO`, `TRANSFORM AS`, `GROUP BY CLASSIFYING`
2. **物理算子单一**：每个逻辑算子只有一种物理实现，论文要求每个逻辑算子有多种物理实现供优化器选择
3. **优化器缺失**：无语义代价模型、无语义直方图、无基于成本的物理算子选择
4. **Calibrator 缺失**：无信任感知执行
5. **验证机制缺失**：无 Semantic Clarity Validation

---

## 3. 详细算法设计

### 3.1 语义直方图 (Semantic Histogram)

#### 3.1.1 理论基础

**定义**: 给定关系 R 的文本列 A，语义直方图 `H_sem(R.A)` 是一个三元组 `(C, W, f_embed)`，其中：
- `C = {c_1, c_2, ..., c_k}` 是嵌入空间 `ℝ^d` 中的 k 个聚类质心
- `W = {w_1, w_2, ..., w_k}` 是每个聚类的 tuple 计数
- `f_embed: String → ℝ^d` 是嵌入函数

**选择性估算原理**: 对于语义谓词 `p_sem` 和查询嵌入 `e_q = f_embed(p_sem)`，选择性估算为：

```
sel(p_sem, H_sem) = Σ_{i: sim(e_q, c_i) ≥ τ} w_i / Σ w_i
```

其中 `sim(·,·)` 是余弦相似度，`τ` 是匹配阈值。

**误差界**: 令真实选择性为 `sel*`，在嵌入空间中语义一致性假设下（即语义相似的文本在嵌入空间中距离接近），误差满足：

```
|sel(p_sem, H_sem) - sel*| ≤ ε_cluster + ε_embed
```

其中 `ε_cluster` 是聚类量化误差（与 k 成反比），`ε_embed` 是嵌入模型的语义-几何失配误差。

**最优性分析**:
- K-Means 聚类是 NP-hard 问题（一般情况），但 Lloyd 算法收敛于局部最优，实践中效果良好
- 使用 FAISS 的 K-Means 实现，支持 GPU 加速，时间复杂度 `O(n·k·d·I)` 其中 I 是迭代次数
- 相比于精确遍历所有嵌入向量计算选择性 `O(n·d)`，直方图查询仅需 `O(k·d)`，k << n 时显著加速

**局限性与改进**:
- 对于否定谓词（"NOT about neural networks"），嵌入距离无法准确反映。改进：引入**分层直方图**，在每个聚类内存储代表性文本摘要，辅助 LLM 二次判定
- 对于组合谓词（"A AND B"），单一嵌入无法表达。改进：分别对子谓词估算选择性，使用独立性假设 `sel(A ∧ B) ≈ sel(A) · sel(B)`

#### 3.1.2 构建算法

```
Algorithm: BUILD_SEMANTIC_HISTOGRAM
Input:
  R.A — 关系 R 的文本列 A 的所有值 {t_1, ..., t_n}
  f_embed — 嵌入模型
  k — 聚类数量（默认 k = min(32, ⌈√n⌉)）
  n_iter — K-Means 迭代次数（默认 20）
Output:
  H_sem = (C, W, stats)

1.  E ← []                              // 嵌入向量集合
2.  for each t_i in R.A:
3.      e_i ← f_embed(t_i)              // 生成嵌入向量, O(d)
4.      normalize(e_i)                   // L2 归一化
5.      E.append(e_i)
6.
7.  E_matrix ← stack(E)                 // shape: (n, d)
8.
9.  // K-Means 聚类 (使用 FAISS)
10. kmeans ← FAISS.Kmeans(d, k, niter=n_iter)
11. kmeans.train(E_matrix)
12.
13. C ← kmeans.centroids               // shape: (k, d), 聚类质心
14.
15. // 计算每个聚类的 tuple 计数
16. _, assignments ← kmeans.index.search(E_matrix, 1)
17. W ← zeros(k)
18. for each a_i in assignments:
19.     W[a_i] += 1
20.
21. // 计算聚类内方差（用于误差估算）
22. variances ← zeros(k)
23. for each (e_i, a_i) in zip(E, assignments):
24.     variances[a_i] += ||e_i - C[a_i]||²
25. for i in 0..k-1:
26.     variances[i] /= max(W[i], 1)
27.
28. stats ← {total_count: n, variances: variances}
29. return (C, W, stats)
```

**时间复杂度**: `O(n·d)` (嵌入生成) + `O(n·k·d·I)` (K-Means) = `O(n·d·(1 + k·I))`
**空间复杂度**: `O(n·d)` (临时) + `O(k·d)` (持久化)

#### 3.1.3 选择性估算算法

```
Algorithm: ESTIMATE_SELECTIVITY
Input:
  H_sem = (C, W, stats) — 语义直方图
  p_sem — 语义谓词文本
  f_embed — 嵌入模型
  τ — 相似度阈值 (默认 0.5)
Output:
  sel — 估算的选择性 ∈ [0, 1]

1.  e_q ← f_embed(p_sem)
2.  normalize(e_q)
3.
4.  // 计算查询与所有质心的相似度
5.  similarities ← C · e_q^T           // shape: (k,), 内积 = 余弦相似度（已归一化）
6.
7.  // 加权匹配
8.  matched_count ← 0
9.  for i in 0..k-1:
10.     if similarities[i] ≥ τ:
11.         matched_count += W[i]
12.     elif similarities[i] ≥ τ - margin:
13.         // 边界区域：按比例估算（软匹配）
14.         ratio ← (similarities[i] - (τ - margin)) / margin
15.         matched_count += W[i] * ratio
16.
17. sel ← matched_count / stats.total_count
18. return clamp(sel, 0.0, 1.0)
```

**时间复杂度**: `O(d)` (嵌入) + `O(k·d)` (相似度计算) = `O(k·d)`
**空间复杂度**: `O(d)` (查询嵌入)

#### 3.1.4 关键接口

```python
class SemanticHistogram:
    """语义直方图：基于嵌入空间聚类的数据分布摘要。

    理论保证：在嵌入空间语义一致性假设下，
    选择性估算误差 ≤ ε_cluster + ε_embed。
    """

    def __init__(self, table_oid: int, column_name: str, n_clusters: int = 32):
        """
        Args:
            table_oid: 目标表的 OID
            column_name: 目标文本列名
            n_clusters: 聚类数量 k，建议取 min(32, ⌈√n⌉)
        """

    def build(self, text_values: list[str], embedding_model: EmbeddingModel) -> None:
        """从文本值集合构建语义直方图。

        时间复杂度: O(n·d·(1 + k·I))
        空间复杂度: O(n·d) 临时 + O(k·d) 持久化
        """

    def estimate_selectivity(self, predicate_text: str,
                              embedding_model: EmbeddingModel,
                              threshold: float = 0.5) -> float:
        """估算语义谓词的选择性。

        时间复杂度: O(k·d)
        Returns: 选择性值 ∈ [0.0, 1.0]
        """

    def estimate_cardinality(self, predicate_text: str,
                              embedding_model: EmbeddingModel,
                              threshold: float = 0.5) -> int:
        """估算语义谓词的输出基数 = selectivity * total_count"""

    def serialize(self) -> bytes:
        """序列化直方图用于持久化存储到 catalog"""

    @classmethod
    def deserialize(cls, data: bytes) -> 'SemanticHistogram':
        """从序列化数据恢复直方图"""
```

---

### 3.2 语义代价模型 (Semantic Cost Model)

#### 3.2.1 理论基础

**统一代价函数**: 对于语义算子 `o_sem`，总代价定义为：

```
Cost(o_sem) = C_exec(o_sem) + λ · L_acc(o_sem)
```

其中：
- `C_exec(o_sem)` 是执行代价（时间 + 金钱），单位统一为抽象代价单位
- `L_acc(o_sem) = N · (1 - A(o_sem))` 是准确性损失，N 是输入基数，A 是准确性评分
- `λ ≥ 0` 是用户可调的准确性权重参数

**论文问题**: 原论文将 `A(o_sem)` 视为静态常量，实际上准确性依赖于：
1. 数据分布 — 某些数据类型/领域 LLM 更擅长
2. 谓词复杂度 — 简单匹配 vs 复杂推理
3. LLM 版本 — 不同模型能力差异显著

**改进方案**: 引入**自适应准确性模型**：

```
A(o_sem, q, D) = A_base(o_sem) · f_complexity(q) · f_domain(D)
```

其中：
- `A_base(o_sem)` 是算子基础准确性（通过离线标定获得）
- `f_complexity(q) ∈ (0, 1]` 是查询复杂度衰减因子（谓词越复杂，准确性越低）
- `f_domain(D) ∈ (0, 1]` 是领域匹配因子（可通过 Calibrator 历史数据回归）

初始实现中，`f_complexity` 和 `f_domain` 设为 1.0，退化为论文原始模型。随着 Calibrator 收集数据，逐步训练更精确的模型。

#### 3.2.2 各物理算子代价公式

设 N 为输入基数，d 为嵌入维度，k 为聚类数/top-k 参数。

| 物理算子 | 执行代价 C_exec | 准确性 A | 适用条件 |
|----------|----------------|----------|----------|
| LLMFilterScan | N · C_LLM | 1.0 (最高) | N 较小或需要最高精度 |
| CodegenFilterScan | C_LLM + N · C_code | 0.7~0.85 | 规则性强的谓词 |
| EmbeddingFilterScan | N · C_embed | 0.80~0.90 | 允许近似匹配 |
| EmbeddingFilterIndexScan | C_embed + log(N) · C_idx | 0.80~0.90 | 有向量索引 |
| HybridFilterScan | N · C_embed + sel · N · C_LLM | 0.95 | 大数据量+高精度 |
| NestedLoopSemanticJoin (NLSJ) | N_R · N_S · C_LLM | 1.0 | 小表连接 |
| IndexedNLSemanticJoin (INLSJ) | N_R · (C_embed + k · C_LLM) | 0.90 | inner表有向量索引 |
| HashSemanticJoin (HSJ) | (N_R + N_S) · C_embed + B · C_LLM | 0.85~0.90 | 大表连接 |
| HashBasedSemanticAgg | N · C_LLM + G · C_hash | 0.95 | 低基数分组 |
| EmbeddingClusterAgg | N · C_embed + C_kmeans + k · C_LLM | 0.80 | 自动发现模式 |

其中常量：
- `C_LLM = 100.0` — 单次 LLM 调用代价（包含延迟和费用）
- `C_embed = 1.0` — 单次嵌入计算代价
- `C_code = 0.01` — 单次代码执行代价
- `C_idx = 0.1` — 单次索引查找代价
- `C_hash = 0.01` — 单次哈希操作代价
- `C_kmeans = k · d · I` — K-Means 训练代价

#### 3.2.3 代价最优选择算法

```
Algorithm: SELECT_OPTIMAL_PHYSICAL_OPERATOR
Input:
  logical_op — 逻辑算子（Selection/Projection/Join/Aggregation）
  N — 估算输入基数（来自语义直方图或统计信息）
  λ — 准确性权重参数
  available_indexes — 可用的向量索引集合
Output:
  best_op — 代价最低的物理算子实例

1.  candidates ← []
2.
3.  // 根据逻辑算子类型枚举候选物理算子
4.  switch logical_op.type:
5.      case SELECTION:
6.          candidates.add(LLMFilterScan(logical_op))
7.          candidates.add(CodegenFilterScan(logical_op))
8.          candidates.add(EmbeddingFilterScan(logical_op))
9.          if has_vector_index(logical_op.table, available_indexes):
10.             candidates.add(EmbeddingFilterIndexScan(logical_op))
11.         candidates.add(HybridFilterScan(logical_op))
12.     case JOIN:
13.         candidates.add(NestedLoopSemanticJoin(logical_op))
14.         if has_vector_index(logical_op.inner_table, available_indexes):
15.             candidates.add(IndexedNLSemanticJoin(logical_op))
16.         candidates.add(HashSemanticJoin(logical_op))
17.     case AGGREGATION:
18.         candidates.add(HashBasedSemanticAgg(logical_op))
19.         candidates.add(EmbeddingClusterAgg(logical_op))
20.
21. // 计算每个候选的总代价
22. best_op ← nil
23. best_cost ← +∞
24. for each op in candidates:
25.     exec_cost ← compute_exec_cost(op, N)
26.     acc_loss ← N * (1 - op.accuracy_score)
27.     total ← exec_cost + λ * acc_loss
28.     if total < best_cost:
29.         best_cost ← total
30.         best_op ← op
31.
32. return best_op
```

**最优性**: 该算法在有限候选集上做穷举比较，保证在给定代价模型参数下选出代价最低的物理算子。算法本身是精确的，代价模型的准确性决定了实际最优性。

**时间复杂度**: `O(|candidates|)` — 通常 ≤ 5，可忽略

#### 3.2.4 关键接口

```python
class SemanticCostModel:
    """语义代价模型：统一执行代价与准确性损失。

    代价函数: Cost(op) = C_exec(op, N) + λ · N · (1 - A(op))
    """

    # 代价常量（可通过配置文件覆盖）
    C_LLM: float = 100.0
    C_EMBED: float = 1.0
    C_CODE: float = 0.01
    C_INDEX: float = 0.1
    C_HASH: float = 0.01

    def compute_cost(self, operator: PhysicalOperator,
                     input_cardinality: int,
                     lambda_acc: float = 1.0) -> float:
        """计算算子的总代价。"""

    def compute_exec_cost(self, operator: PhysicalOperator,
                          input_cardinality: int) -> float:
        """计算纯执行代价 C_exec。"""

    def get_accuracy_score(self, operator: PhysicalOperator) -> float:
        """获取算子的准确性评分 A(op) ∈ [0, 1]。
        初始使用静态值，后续通过 Calibrator 数据自适应调整。"""

    def select_best_operator(self, logical_op: LogicalOperator,
                              input_cardinality: int,
                              lambda_acc: float = 1.0) -> PhysicalOperator:
        """选择代价最优的物理算子。"""
```

---

### 3.3 语义选择物理算子 (Semantic Selection)

#### 3.3.1 LLMFilterScan（已有，即现有 SemanticFilter）

**算法**: 对每个 tuple 构造 prompt，调用 LLM 判断 true/false。

```
Algorithm: LLM_FILTER_SCAN
Input: iterator(tuples), predicate_text, columns
Output: filtered tuples

1.  messages ← []
2.  tuples ← materialize(iterator)
3.  for each t in tuples:
4.      prompt ← format_predicate(predicate_text, t, columns)
5.      msg ← [system: "输出 true 或 false", user: prompt]
6.      messages.append(msg)
7.
8.  responses ← batch_llm_call(messages)  // 批量调用
9.  for each (t, resp) in zip(tuples, responses):
10.     if resp.lower() == "true":
11.         yield t
```

**时间复杂度**: `O(N · T_LLM)` 其中 T_LLM 是单次 LLM 调用延迟
**准确性**: 最高（直接使用 LLM 推理），A ≈ 1.0

#### 3.3.2 CodegenFilterScan（新增）

**理论**: 利用 LLM 的代码生成能力，将语义谓词转换为确定性 Python 过滤函数。此方法的核心观察是：许多语义谓词实际上可以用规则或正则表达式表达（如"包含数字的标题"），LLM 可以一次性生成代码，然后以 O(1) 代价执行 N 次。

**适用条件**: 谓词具有明确的规则性（如模式匹配、数值范围、关键字检查）

```
Algorithm: CODEGEN_FILTER_SCAN
Input: iterator(tuples), predicate_text, columns, sample_tuples
Output: filtered tuples

// Phase 1: 代码生成（一次性）
1.  sample ← take(iterator, 5)           // 取 5 个样本
2.  prompt ← construct_codegen_prompt(predicate_text, sample, columns)
3.  code_str ← llm_call(prompt)          // LLM 生成 Python 函数
4.
5.  // Phase 2: 安全验证
6.  ast_tree ← parse(code_str)
7.  if contains_dangerous_calls(ast_tree):  // 检查 os.*, subprocess.* 等
8.      fallback to LLMFilterScan
9.
10. filter_fn ← compile_in_sandbox(code_str)
11.
12. // Phase 3: 验证（用 LLM 交叉验证样本结果）
13. for each s in sample:
14.     code_result ← filter_fn(s)
15.     llm_result ← llm_judge(predicate_text, s)
16.     if code_result != llm_result:
17.         fallback to LLMFilterScan      // 代码不可靠
18.
19. // Phase 4: 批量执行
20. all_tuples ← materialize(iterator)
21. for each t in all_tuples:
22.     if filter_fn(t):
23.         yield t
```

**时间复杂度**: `O(T_LLM)` (代码生成) + `O(N · T_code)` (执行) ≈ `O(T_LLM + N)`
**准确性**: 中等，A ≈ 0.70~0.85（取决于谓词的规则化程度）
**最优性**: 当 N >> 1 且谓词可规则化时，总代价远低于 LLMFilterScan

#### 3.3.3 EmbeddingFilterScan（新增）

**理论**: 将语义谓词和每个 tuple 的文本都嵌入到同一向量空间，利用余弦相似度近似语义匹配。基于**分布式语义假设**（distributional semantics hypothesis）：语义相似的文本在嵌入空间中距离接近。

**数学形式**: 给定谓词嵌入 `e_q` 和 tuple 嵌入 `e_t`：
```
match(t) ⟺ cos(e_q, e_t) = (e_q · e_t) / (||e_q|| · ||e_t||) ≥ τ
```

若嵌入已 L2 归一化，则 `cos(e_q, e_t) = e_q · e_t`（内积）。

```
Algorithm: EMBEDDING_FILTER_SCAN
Input: iterator(tuples), predicate_text, columns, threshold τ
Output: filtered tuples

1.  e_q ← embed(predicate_text)          // 查询嵌入
2.  normalize(e_q)
3.
4.  texts ← []
5.  tuples ← []
6.  for each t in iterator:
7.      texts.append(extract_text(t, columns))
8.      tuples.append(t)
9.
10. E ← embed_batch(texts)               // 批量嵌入, shape: (N, d)
11. normalize_rows(E)
12.
13. similarities ← E · e_q^T             // shape: (N,), 批量内积
14.
15. for i in 0..N-1:
16.     if similarities[i] ≥ τ:
17.         yield tuples[i]
```

**时间复杂度**: `O(N · d)` (嵌入) + `O(N · d)` (内积) = `O(N · d)`
**空间复杂度**: `O(N · d)`
**准确性**: 中高，A ≈ 0.80~0.90（依赖嵌入模型质量和阈值选择）
**最优性**: 是所有不使用 LLM 的方法中最高效的。向量计算可充分利用 SIMD 指令和 GPU 并行。

#### 3.3.4 HybridFilterScan（新增）

**理论**: 两阶段过滤策略，结合嵌入的高效率和 LLM 的高精度。第一阶段用嵌入相似度做粗筛（高召回率、低精确率），第二阶段用 LLM 精筛（高精确率）。

**数学分析**: 设嵌入过滤使用较低阈值 `τ_loose`，选择性为 `sel_embed`。则：
- 第一阶段处理 N 个 tuple，输出 `sel_embed · N` 个候选
- 第二阶段用 LLM 处理 `sel_embed · N` 个候选
- 总 LLM 调用次数从 N 降低到 `sel_embed · N`

**代价**: `C_exec = N · C_embed + sel_embed · N · C_LLM`

当 `sel_embed < 1` 时，严格优于 LLMFilterScan（`N · C_LLM`）。
当 `C_embed / C_LLM < 1 - sel_embed` 时，混合策略更优（通常成立，因为 C_embed << C_LLM）。

```
Algorithm: HYBRID_FILTER_SCAN
Input: iterator(tuples), predicate_text, columns, τ_loose, τ_strict
Output: filtered tuples

// Stage 1: 嵌入粗筛
1.  candidates ← EMBEDDING_FILTER_SCAN(iterator, predicate_text, columns, τ_loose)
2.
// Stage 2: LLM 精筛
3.  for each t in candidates:
4.      result ← LLM_JUDGE(predicate_text, t)
5.      if result == true:
6.          yield t
```

**时间复杂度**: `O(N · d + sel_embed · N · T_LLM)`
**准确性**: 高，A ≈ 0.95（嵌入阶段可能漏掉极少数边界case）
**最优性**: 在 N 较大且 `sel_embed` 较小时，是精度-效率的最佳折中

#### 3.3.5 选择物理算子关键接口

```python
class SemanticFilterBase(PhysicalOperator):
    """所有语义过滤算子的基类。"""

    def __init__(self, condition: SemanticCondition):
        """
        Args:
            condition: 包含谓词文本、阈值和目标列的语义条件
        """

    def open(self) -> None: ...
    def next(self) -> Generator[tuple, None, None]: ...
    def close(self) -> None: ...

    @property
    def accuracy_score(self) -> float:
        """返回该算子的基础准确性评分。"""

class LLMFilterScan(SemanticFilterBase):
    """纯 LLM 过滤。accuracy_score = 1.0"""

class CodegenFilterScan(SemanticFilterBase):
    """代码生成过滤。accuracy_score = 0.75"""

class EmbeddingFilterScan(SemanticFilterBase):
    """纯嵌入过滤。accuracy_score = 0.85"""

class HybridFilterScan(SemanticFilterBase):
    """混合过滤（嵌入+LLM）。accuracy_score = 0.95"""
```

---

### 3.4 语义连接物理算子 (Semantic Join)

#### 3.4.1 NestedLoopSemanticJoin (NLSJ)（已有）

**算法**: 经典嵌套循环连接的语义版本。对 R × S 的每对 tuple 调用 LLM 判断是否匹配。

```
Algorithm: NESTED_LOOP_SEMANTIC_JOIN
Input: R (outer), S (inner), predicate_text
Output: joined tuples

1.  for each r in R:
2.      for each s in S:
3.          prompt ← format_join_predicate(predicate_text, r, s)
4.          if LLM_JUDGE(prompt) == true:
5.              yield concat(r, s)
```

**时间复杂度**: `O(|R| · |S| · T_LLM)` — 对大表不可行
**准确性**: A = 1.0

#### 3.4.2 IndexedNestedLoopSemanticJoin (INLSJ)（新增）

**理论**: 利用 inner 表的向量索引，对 outer 表的每个 tuple 先做 k-NN 搜索，只对 top-k 最相似的候选调用 LLM 精确判定。本质上是将 O(|S|) 的线性扫描替换为 O(log|S|) 的索引查找。

**前提**: inner 表 S 上存在基于嵌入的向量索引（FAISS IndexFlatIP 或 IndexIVFFlat）。

```
Algorithm: INDEXED_NL_SEMANTIC_JOIN
Input: R (outer), S (inner), predicate_text, k_candidates, vector_index
Output: joined tuples

// 预处理: 确保 S 的向量索引已构建
1.  if vector_index is None:
2.      E_S ← embed_batch(extract_texts(S))
3.      vector_index ← build_faiss_index(E_S)  // IndexFlatIP

4.  for each r in R:
5.      e_r ← embed(extract_text(r))
6.      normalize(e_r)
7.
8.      // k-NN 搜索
9.      distances, indices ← vector_index.search(e_r, k_candidates)
10.
11.     // LLM 精筛
12.     for idx in indices[0]:
13.         s ← S[idx]
14.         prompt ← format_join_predicate(predicate_text, r, s)
15.         if LLM_JUDGE(prompt) == true:
16.             yield concat(r, s)
```

**时间复杂度**:
- 索引构建: `O(|S| · d)` (一次性)
- 查询: `O(|R| · (d + k · T_LLM))`
- 对比 NLSJ: `|R| · k · T_LLM` vs `|R| · |S| · T_LLM`，当 `k << |S|` 时显著加速

**准确性**: A ≈ 0.90（可能漏掉 top-k 之外的匹配对）

**最优性分析**: INLSJ 是语义连接中效率-精度折中的最佳选择：
- 与 NLSJ 相比，LLM 调用减少 `|S|/k` 倍
- 与 HSJ 相比，不需要 LSH 参数调优，且 k-NN 保证了最高质量的候选集
- 当 `k ≥ true_matches(r)` 时（即 k 足够大），召回率接近 100%

#### 3.4.3 HashSemanticJoin (HSJ)（新增）

**理论**: 使用 **Locality-Sensitive Hashing (LSH)** 将语义相似的 tuple 映射到相同的桶，然后只在同一桶内进行 LLM 精确匹配。

**LSH 理论基础**: 对于余弦相似度，使用 **SimHash** (随机超平面投影)：
- 选择 L 个随机超平面 `{h_1, ..., h_L}`
- 对向量 v，哈希值 `H(v) = (sign(h_1·v), sign(h_2·v), ..., sign(h_L·v))`
- 性质: `Pr[H(u) = H(v)] = 1 - arccos(cos(u,v)) / π`
- 即余弦相似度越高，哈希碰撞概率越大

为提高召回率，使用 **多表 LSH** (multi-probe LSH)：
- 构建 B 个独立哈希表，每个使用不同的随机超平面集
- tuple 需要在任意一个表中碰撞即视为候选对

```
Algorithm: HASH_SEMANTIC_JOIN
Input: R, S, predicate_text, n_hash_bits, n_tables
Output: joined tuples

// Phase 1: Build — 对 S 构建 LSH 索引
1.  E_S ← embed_batch(extract_texts(S))
2.  hash_tables ← [dict() for _ in range(n_tables)]
3.  for each table_idx in 0..n_tables-1:
4.      hyperplanes ← random_normal(n_hash_bits, d)
5.      for each (i, e_s) in enumerate(E_S):
6.          hash_key ← tuple(sign(hyperplanes · e_s))
7.          hash_tables[table_idx][hash_key].append(i)

// Phase 2: Probe — 对 R 的每个 tuple 查找候选
8.  E_R ← embed_batch(extract_texts(R))
9.  for each (r, e_r) in zip(R, E_R):
10.     candidate_indices ← set()
11.     for each table_idx in 0..n_tables-1:
12.         hash_key ← tuple(sign(hyperplanes[table_idx] · e_r))
13.         candidate_indices.update(hash_tables[table_idx].get(hash_key, []))
14.
15.     // LLM 精筛
16.     for idx in candidate_indices:
17.         s ← S[idx]
18.         if LLM_JUDGE(format_join_predicate(predicate_text, r, s)):
19.             yield concat(r, s)
```

**时间复杂度**:
- Build: `O(|S| · d · B)`
- Probe: `O(|R| · (d · B + |candidates| · T_LLM))`
- 期望 `|candidates| ≈ sel · |S|`

**准确性**: A ≈ 0.85~0.90（取决于 LSH 参数，可能漏掉某些碰撞失败的匹配对）

#### 3.4.4 连接算子关键接口

```python
class SemanticJoinBase(PhysicalOperator):
    """所有语义连接算子的基类。"""

    def __init__(self, condition: SemanticCondition, join_type: JoinType,
                 children_table_names: list[str]):
        """
        Args:
            condition: 语义连接条件
            join_type: INNER/LEFT/RIGHT/FULL
            children_table_names: [left_table, right_table]
        """

    @property
    def accuracy_score(self) -> float: ...

class NestedLoopSemanticJoin(SemanticJoinBase):
    """accuracy = 1.0, cost = O(|R|·|S|·C_LLM)"""

class IndexedNLSemanticJoin(SemanticJoinBase):
    """accuracy = 0.90, cost = O(|R|·k·C_LLM)

    Args:
        k_candidates: k-NN 中的 k 值，默认 10
    """

class HashSemanticJoin(SemanticJoinBase):
    """accuracy = 0.85, cost = O((|R|+|S|)·C_embed + B·C_LLM)

    Args:
        n_hash_bits: LSH 哈希位数，默认 8
        n_tables: LSH 哈希表数量，默认 4
    """
```

---

### 3.5 语义聚合物理算子 (Semantic Aggregation)

#### 3.5.1 HashBasedSemanticAggregation（新增）

**理论**: 对每个 tuple 调用 LLM 生成语义分组键（如将一篇论文分类为"NLP"、"CV"等），然后使用标准哈希聚合。

**与传统哈希聚合的关系**: 传统哈希聚合假设分组键已知，直接读取列值作为键。语义聚合需要先通过 LLM "计算"分组键，再进行哈希聚合。因此可视为 `π_sem` (语义投影) + `γ` (传统聚合) 的组合。

```
Algorithm: HASH_BASED_SEMANTIC_AGGREGATION
Input: iterator(tuples), group_instruction, agg_functions, columns
Output: aggregated results

// Phase 1: LLM 生成分组键
1.  keyed_tuples ← []
2.  messages ← []
3.  tuples ← materialize(iterator)
4.  for each t in tuples:
5.      prompt ← "根据以下指令对数据分类: " + group_instruction
6.                + "\n数据: " + format(t, columns)
7.                + "\n输出一个分类标签（单词或短语）:"
8.      messages.append(prompt)
9.
10. keys ← batch_llm_call(messages)
11. for each (t, key) in zip(tuples, keys):
12.     keyed_tuples.append((normalize_key(key), t))

// Phase 2: 哈希聚合（标准算法）
13. hash_table ← {}
14. for each (key, t) in keyed_tuples:
15.     if key not in hash_table:
16.         hash_table[key] ← init_accumulators(agg_functions)
17.     update_accumulators(hash_table[key], t, agg_functions)

// Phase 3: 输出结果
18. for each (key, accumulators) in hash_table:
19.     yield (key, finalize(accumulators))
```

**时间复杂度**: `O(N · T_LLM + N)` — LLM 调用主导
**空间复杂度**: `O(G)` 其中 G 是分组数
**准确性**: A ≈ 0.95（LLM 分类一般较准确，但可能出现不一致的键名）

**键名归一化问题**: LLM 可能为相同概念生成不同的名称（如"NLP"vs"Natural Language Processing"）。
解决方案：对所有生成的键做嵌入相似度聚合，合并 `cos(e_k1, e_k2) ≥ 0.9` 的键。

#### 3.5.2 EmbeddingClusterAggregation（重构现有实现）

**理论**: 先将所有 tuple 嵌入到向量空间，使用 K-Means 聚类自动发现分组结构，再用 LLM 为每个聚类命名。

**与 HashBasedSemanticAgg 的区别**:
- HashBased: LLM 决定每个 tuple 的分组 → 键名来自 LLM → 更精确但更慢
- EmbeddingCluster: 嵌入+聚类决定分组 → LLM 仅命名 k 个聚类 → 更快但可能不准确

**K-Means 算法选择**: 使用 FAISS 的 K-Means 实现（基于 Lloyd 算法），支持 GPU 加速。
- 为何不用 DBSCAN：DBSCAN 无需指定 k，但对高维嵌入空间效果差（维度诅咒），且输出聚类数不可控
- 为何不用层次聚类：时间复杂度 O(n²) 对大数据不可行

```
Algorithm: EMBEDDING_CLUSTER_AGGREGATION
Input: iterator(tuples), k, group_instruction, agg_functions, columns
Output: aggregated results

// Phase 1: 嵌入
1.  tuples ← materialize(iterator)
2.  texts ← [extract_text(t, columns) for t in tuples]
3.  E ← embed_batch(texts)               // shape: (N, d)

// Phase 2: K-Means 聚类
4.  kmeans ← FAISS.Kmeans(d, k, niter=20)
5.  kmeans.train(E)
6.  _, assignments ← kmeans.index.search(E, 1)
7.  assignments ← assignments.flatten()

// Phase 3: 聚类命名
8.  clusters ← group_by(assignments, tuples)
9.  for each cluster_id in 0..k-1:
10.     representatives ← closest_to_centroid(clusters[cluster_id], kmeans.centroids[cluster_id], top=5)
11.     prompt ← "为以下数据组命名一个分类标签:\n"
12.              + format_examples(representatives)
13.              + "\n上下文: " + group_instruction
14.     category_name ← llm_call(prompt)
15.     clusters[cluster_id].name ← category_name

// Phase 4: 聚合
16. for each cluster in clusters:
17.     accumulators ← init_accumulators(agg_functions)
18.     for each t in cluster.tuples:
19.         update_accumulators(accumulators, t, agg_functions)
20.     yield (cluster.name, finalize(accumulators))
```

**时间复杂度**: `O(N·d)` (嵌入) + `O(N·k·d·I)` (K-Means) + `O(k·T_LLM)` (命名)
**准确性**: A ≈ 0.80（聚类边界的 tuple 可能被错误分组）

#### 3.5.3 聚合算子关键接口

```python
class SemanticAggregationBase(PhysicalOperator):
    """所有语义聚合算子的基类。"""

    def __init__(self, group_instruction: str,
                 agg_functions: list[FunctionColumn],
                 grouping_columns: list):
        """
        Args:
            group_instruction: 分组指令文本
            agg_functions: 聚合函数列表 (COUNT, SUM, AVG, etc.)
            grouping_columns: 需要分组的列
        """

    @property
    def accuracy_score(self) -> float: ...

class HashBasedSemanticAggregation(SemanticAggregationBase):
    """accuracy = 0.95, cost = O(N·C_LLM)"""

class EmbeddingClusterAggregation(SemanticAggregationBase):
    """accuracy = 0.80, cost = O(N·C_embed + k·C_LLM)

    Args:
        k: 聚类数量
        n_iter: K-Means 迭代次数
    """
```

---

### 3.6 校准算子 (Calibrator)

#### 3.6.1 理论基础

**动机**: LLM 输出不可靠，需要事后验证机制。Calibrator 的目标是估算每个语义算子输出的可信度，并在可信度过低时触发重试或降级。

**校准方法**: 采用**采样验证** + **交叉验证**的双重策略：

1. **采样验证 (Sample Verification)**: 随机抽取输出结果的子集，通过独立的 LLM 调用（或不同 prompt）重新验证
   - 抽样率: `min(0.1, 30/N)` — 至少验证 30 个样本或 10%
   - 一致率 = 验证通过数 / 验证总数

2. **交叉模型验证 (Cross-Model Verification)**: 可选，使用不同的 LLM 对相同输入做独立判断
   - 适用于关键业务场景
   - 一致率 = 两模型结果一致数 / 验证总数

**置信度计算**:
```
confidence = α · consistency_sample + (1-α) · consistency_cross
```
其中 α ∈ [0, 1] 是权重（默认 0.7）。

#### 3.6.2 算法

```
Algorithm: CALIBRATE
Input:
  results — 上游语义算子的输出 [(tuple, metadata), ...]
  operator_type — 上游算子类型
  sample_rate — 采样率
  confidence_threshold — 置信度阈值
Output:
  calibrated_results — 附加置信度的结果
  overall_confidence — 整体置信度评分

1.  N ← len(results)
2.  sample_size ← max(min(ceil(N * sample_rate), N), min(30, N))
3.  sample_indices ← random_sample(0..N-1, sample_size)
4.
5.  // 采样验证
6.  verify_count ← 0
7.  for each idx in sample_indices:
8.      original ← results[idx]
9.      re_result ← re_verify(original, operator_type)
10.     if is_consistent(original, re_result):
11.         verify_count += 1
12.
13. consistency ← verify_count / sample_size
14. overall_confidence ← consistency
15.
16. // 如果置信度低于阈值，标记并可选重试
17. if overall_confidence < confidence_threshold:
18.     log_warning("Low confidence: {overall_confidence}")
19.     // 可选: 对全部结果用更高精度方法重新处理
20.
21. // 输出带置信度的结果
22. for each r in results:
23.     yield (r, overall_confidence)
```

**时间复杂度**: `O(sample_size · T_LLM)` — 通常是常数级别
**空间复杂度**: `O(1)` 额外空间

#### 3.6.3 关键接口

```python
class CalibratorOperator(PhysicalOperator):
    """校准算子：估算上游语义算子输出的可信度。

    可插入任意语义算子之后。不改变数据流，仅附加元数据。
    """

    def __init__(self, sample_rate: float = 0.1,
                 confidence_threshold: float = 0.8,
                 cross_model: str = None):
        """
        Args:
            sample_rate: 采样验证比例
            confidence_threshold: 最低可接受置信度
            cross_model: 交叉验证使用的模型名称（None 表示不做交叉验证）
        """

    def open(self) -> None: ...
    def next(self) -> Generator[tuple, None, None]: ...
    def close(self) -> None: ...

    @property
    def confidence(self) -> float:
        """返回最近一次执行的整体置信度。"""

    def get_calibration_stats(self) -> dict:
        """返回校准统计信息:
        {
            'sample_size': int,
            'consistency_rate': float,
            'confidence': float,
            'low_confidence_indices': list[int]
        }
        """
```

---

### 3.7 语义清晰度验证 (Semantic Clarity Validation)

#### 3.7.1 三阶段验证算法

```
Algorithm: SEMANTIC_CLARITY_VALIDATION
Input:
  query_ast — 解析后的查询 AST
  sample_data — 少量样本数据（用于预演）
Output:
  validation_result — {valid: bool, warnings: list, stage: str}

// === Stage 1: 语法预检 (Syntactic Pre-flight Check) ===
// 目标: 快速检查语义谓词的格式正确性

1.  for each MATCHES predicate p in query_ast:
2.      // 检查断言性：谓词必须能判断 true/false
3.      prompt ← "判断以下文本是否是一个可以回答是/否的断言: '" + p.text + "'"
4.      result ← lightweight_llm_call(prompt, max_tokens=16)
5.      if result != "是":
6.          return {valid: false, warnings: ["MATCHES 谓词必须是断言性的"], stage: "syntactic"}

7.  for each CLASSIFYING clause c in query_ast:
8.      // 检查分类目标是否是名词/名词短语
9.      prompt ← "判断以下文本是否是一个有效的分类维度(名词或名词短语): '" + c.key_name + "'"
10.     result ← lightweight_llm_call(prompt, max_tokens=16)
11.     if result != "是":
12.         return {valid: false, warnings: ["CLASSIFYING 的分类维度必须是名词短语"], stage: "syntactic"}

// === Stage 2: 语义稳定性预演 (Semantic Stability Dry Run) ===
// 目标: 验证语义操作在不同样本上的输出一致性

13. if has_extract_clause(query_ast):
14.     // Few-shot Consistency Check
15.     samples ← random_sample(sample_data, 5)
16.     schemas ← []
17.     for each s in samples:
18.         result ← execute_extract(query_ast.extract, s)
19.         schemas.append(get_schema(result))  // 提取输出的列名和类型
20.
21.     if not all_schemas_consistent(schemas):
22.         return {valid: false, warnings: ["EXTRACT 输出 schema 不一致"], stage: "stability"}

23. if has_transform_clause(query_ast):
24.     // Paraphrase Consistency Check
25.     sample ← sample_data[0]
26.     result1 ← execute_transform(query_ast.transform, sample)
27.     paraphrased_instruction ← paraphrase(query_ast.transform.instruction)
28.     result2 ← execute_transform_with(paraphrased_instruction, sample)
29.     if semantic_distance(result1, result2) > 0.3:
30.         warnings.append("TRANSFORM 对改述敏感，结果可能不稳定")

// === Stage 3: 运行时校准 (Runtime — 由 CalibratorOperator 处理) ===
31. return {valid: true, warnings: warnings, stage: "passed"}
```

**时间复杂度**: Stage 1: `O(P · T_LLM_light)` 其中 P 是语义谓词数; Stage 2: `O(5 · T_LLM)` — 固定常数

#### 3.7.2 关键接口

```python
class SemanticClarityValidator:
    """语义清晰度验证器：在查询执行前验证语义操作的合理性。"""

    def validate(self, query_ast, sample_data: list = None) -> ValidationResult:
        """执行三阶段验证。

        Args:
            query_ast: 解析后的查询 AST
            sample_data: 可选的样本数据（用于 Stage 2）

        Returns:
            ValidationResult with fields:
                valid: bool
                warnings: list[str]
                stage: str — 失败的阶段 ("syntactic"/"stability"/"passed")
        """

    def syntactic_check(self, query_ast) -> ValidationResult:
        """Stage 1: 语法预检。"""

    def stability_check(self, query_ast, sample_data: list) -> ValidationResult:
        """Stage 2: 语义稳定性预演。"""

class ValidationResult:
    valid: bool
    warnings: list[str]
    stage: str
    confidence: float = 1.0
```

---

## 4. S²QL 语法扩展设计

### 4.1 新增 Lexer Tokens

```
MATCHES, EXTRACT, TRANSFORM, CLASSIFYING, USING, INTO, WITH
```

### 4.2 新增 Parser 规则

**语义选择 (MATCHES)**:
```sql
SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks';
SELECT * FROM reviews WHERE content MATCHES 'positive sentiment' WITH (threshold = 0.8);
```

**结构化提取 (EXTRACT INTO)**:
```sql
SELECT EXTRACT(text INTO (name TEXT, age INT)) FROM documents;
```

**语义转换 (TRANSFORM AS)**:
```sql
SELECT TRANSFORM(description AS summary USING 'summarize in one sentence') FROM products;
```

**语义聚合 (GROUP BY CLASSIFYING)**:
```sql
SELECT category, COUNT(*) FROM reviews GROUP BY CLASSIFYING review_text AS category;
```

**WITH 子句扩展**:
```sql
SELECT * FROM R JOIN S ON R.desc MATCHES S.desc WITH (threshold = 0.92, model = 'deepseek');
```

### 4.3 AST 节点设计

```python
# andb/sql/parser/ast/s2ql.py

class MatchesPredicate:
    """WHERE column MATCHES 'assertion' [WITH (params)]"""
    column: Identifier          # 目标列
    assertion: str              # 语义断言文本
    with_params: dict | None    # {threshold: float, model: str, ...}

class ExtractExpression:
    """EXTRACT(source INTO (col1 TYPE1, col2 TYPE2, ...))"""
    source_column: Identifier   # 源文本列
    target_schema: list[tuple]  # [(name, type), ...]

class TransformExpression:
    """TRANSFORM(input AS output USING 'instruction')"""
    input_column: Identifier    # 输入列
    output_name: str            # 输出列名
    instruction: str            # 转换指令

class ClassifyingGroupBy:
    """GROUP BY CLASSIFYING source_col AS key_name"""
    source_column: Identifier   # 待分类的源列
    key_name: str               # 生成的分组键列名

class WithClause:
    """WITH (key1 = val1, key2 = val2, ...)"""
    params: dict[str, any]      # 参数键值对
```

---

## 5. 测试计划

### 5.1 单元测试

| 测试文件 | 测试内容 | 测试方法 |
|----------|----------|----------|
| `test_s2ql_parser.py` | MATCHES, EXTRACT, TRANSFORM, CLASSIFYING 解析 | AST 结构断言 |
| `test_semantic_histogram.py` | 直方图构建、选择性估算 | 合成嵌入数据 |
| `test_semantic_cost_model.py` | 代价计算、最优算子选择 | 参数化测试 |
| `test_semantic_filter_operators.py` | 各过滤算子 (Mock LLM) | Mock LLM/Embedding |
| `test_semantic_join_operators.py` | 各连接算子 (Mock LLM) | Mock LLM/Embedding |
| `test_semantic_agg_operators.py` | 各聚合算子 (Mock LLM) | Mock LLM/Embedding |
| `test_calibrator.py` | 校准算子采样验证 | Mock LLM |
| `test_clarity_validator.py` | 三阶段验证 | Mock LLM |

### 5.2 集成测试

| 测试文件 | 测试内容 |
|----------|----------|
| `test_s2ql_e2e.py` | 端到端查询：解析→优化→执行 |
| `test_optimizer_selection.py` | 优化器物理算子选择验证 |

---

## 6. 文件变更清单

### 新增文件
| 文件路径 | 说明 |
|----------|------|
| `andb/sql/parser/ast/s2ql.py` | S²QL 新 AST 节点定义 |
| `andb/executor/operator/physical/semantic_filter.py` | 语义过滤物理算子（多种实现） |
| `andb/executor/operator/physical/semantic_join.py` | 语义连接物理算子（多种实现） |
| `andb/executor/operator/physical/semantic_agg.py` | 语义聚合物理算子（多种实现） |
| `andb/executor/operator/physical/calibrator.py` | 校准算子 |
| `andb/sql/optimizer/semantic_histogram.py` | 语义直方图 |
| `andb/sql/optimizer/semantic_cost_model.py` | 语义代价模型 |
| `andb/sql/validation/clarity_validator.py` | 语义清晰度验证 |
| `tests/unit/test_s2ql_parser.py` | S²QL 解析器测试 |
| `tests/unit/test_semantic_histogram.py` | 语义直方图测试 |
| `tests/unit/test_semantic_cost_model.py` | 代价模型测试 |
| `tests/unit/test_semantic_filter_operators.py` | 语义过滤算子测试 |
| `tests/unit/test_semantic_join_operators.py` | 语义连接算子测试 |
| `tests/unit/test_semantic_agg_operators.py` | 语义聚合算子测试 |
| `tests/unit/test_calibrator.py` | 校准算子测试 |
| `tests/unit/test_clarity_validator.py` | 验证机制测试 |
| `tests/integration/test_s2ql_e2e.py` | 端到端集成测试 |

### 修改文件
| 文件路径 | 变更内容 |
|----------|----------|
| `andb/sql/parser/lexer.py` | 新增 MATCHES, EXTRACT, TRANSFORM, CLASSIFYING, USING, INTO tokens |
| `andb/sql/parser/parser_.py` | 新增 S²QL 语法规则 |
| `andb/sql/optimizer/implementations.py` | 支持多物理算子选择（基于代价模型） |
| `andb/sql/optimizer/planner.py` | 集成语义优化器 |
| `andb/executor/operator/logical.py` | 新增逻辑算子 |
| `andb/sql/optimizer/transformations.py` | 支持新 AST 节点到逻辑计划的转换 |

---

## 7. 实现优先级与里程碑

### Milestone 1: S²QL 语法与解析 (Phase 1)
- 实现 MATCHES, EXTRACT, TRANSFORM, CLASSIFYING 的词法/语法分析
- 新增 AST 节点
- AST → 逻辑计划转换
- **测试**：parser 单元测试

### Milestone 2: 多物理算子 (Phase 2)
- 实现 CodegenFilterScan, HybridFilterScan, EmbeddingFilterScan
- 实现 INLSJ, HSJ
- 实现 HashBasedSemanticAggregation, EmbeddingClusterAggregation
- 实现 CalibratorOperator
- **测试**：物理算子单元测试（使用 mock）

### Milestone 3: 语义优化器 (Phase 3 + 4)
- 实现 SemanticHistogram
- 实现 SemanticCostModel
- 集成到优化器的物理算子选择流程
- **测试**：直方图测试 + 代价模型测试

### Milestone 4: 验证与质量 (Phase 5)
- 实现 Semantic Clarity Validation
- 端到端集成测试

---

## 8. 技术决策与约束

### 8.1 向后兼容性
- 保留现有 `SEM_MATCH`, `SEM_CLUSTER`, `PROMPT` 语法作为别名
- 新 S²QL 语法与现有语法并存
- 现有测试用例不受影响

### 8.2 LLM 抽象层
- 所有 LLM 调用通过 `ClientModelFactory` 抽象
- 所有嵌入计算通过 `EmbeddingModelFactory` 抽象
- 单元测试使用 mock 实现，不依赖真实 LLM

### 8.3 代码组织
- 新增物理算子放在独立文件中（semantic_filter.py, semantic_join.py, semantic_agg.py）
- 保留现有 semantic.py 不变，新文件通过继承和组合复用现有逻辑
- 通过 `__init__.py` 统一导出
