# SynapseBase (AnDB) - 设计文档

## 1. 论文分析与不足之处

### 1.1 论文核心贡献
论文提出了 SynapseBase，一个 LLM-native 数据库框架，核心创新点包括：
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
| FineTunedProjectionOperator | ❌ 未实现 | §6 Table 2 |
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
| WITH clause for JOIN (threshold, model) | ❌ 未实现 | §5.2 Example 4 |
| 单元测试（语义部分） | ❌ 严重不足 | - |

### 2.3 现有实现与论文的差异

1. **语法差异**：现有代码使用 `SEM_MATCH('...')`, `SEM_CLUSTER(...)`, `PROMPT(...)` 语法；论文定义的是 `MATCHES`, `EXTRACT INTO`, `TRANSFORM AS`, `GROUP BY CLASSIFYING`
2. **物理算子单一**：每个逻辑算子只有一种物理实现，论文要求每个逻辑算子有多种物理实现供优化器选择
3. **优化器缺失**：无语义代价模型、无语义直方图、无基于成本的物理算子选择
4. **Calibrator 缺失**：无信任感知执行
5. **验证机制缺失**：无 Semantic Clarity Validation

---

## 3. 实现计划

### Phase 1: S²QL 语法扩展（SQL Engine 层）

#### 3.1.1 新增 Lexer Tokens
```
MATCHES, EXTRACT, TRANSFORM, CLASSIFYING
```

#### 3.1.2 新增 Parser 规则

**语义选择 (MATCHES)**:
```sql
-- WHERE <column> MATCHES '<assertion>'
SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks';
```
Parser 规则：在 `WHERE` 子句中，当遇到 `<identifier> MATCHES <string>` 时，生成 `MatchesPredicate(column, assertion)` AST 节点。

**结构化提取 (EXTRACT INTO)**:
```sql
SELECT EXTRACT(text INTO (name STRING, age INT)) FROM documents;
```
Parser 规则：`EXTRACT LPAREN identifier INTO LPAREN defined_columns RPAREN RPAREN` 生成 `ExtractExpression(source_column, target_schema)` AST 节点。

**语义转换 (TRANSFORM AS)**:
```sql
SELECT TRANSFORM(description AS summary USING 'summarize in one sentence') FROM products;
```
Parser 规则：`TRANSFORM LPAREN identifier AS identifier USING string RPAREN` 生成 `TransformExpression(input_col, output_col, instruction)` AST 节点。

**语义聚合 (GROUP BY CLASSIFYING)**:
```sql
SELECT category, COUNT(*) FROM reviews GROUP BY CLASSIFYING review_text AS category;
```
Parser 规则：`GROUP_BY CLASSIFYING identifier AS identifier` 生成 `ClassifyingGroupBy(source_column, group_key_name)` AST 节点。

**WITH 子句扩展**（用于 JOIN 的精细控制）:
```sql
JOIN ... ON ... MATCHES '...' WITH (threshold = 0.92, model = 'xxx')
```

#### 3.1.3 AST 节点新增

```python
# andb/sql/parser/ast/semantic.py 中新增

class MatchesPredicate(ASTNode):
    """WHERE column MATCHES 'assertion'"""
    def __init__(self, column, assertion, with_params=None):
        self.column = column        # Identifier
        self.assertion = assertion  # string
        self.with_params = with_params  # dict: {threshold, model, ...}

class ExtractExpression(ASTNode):
    """EXTRACT(source INTO (col1 TYPE1, col2 TYPE2, ...))"""
    def __init__(self, source_column, target_schema):
        self.source_column = source_column  # Identifier
        self.target_schema = target_schema  # list of (name, type) pairs

class TransformExpression(ASTNode):
    """TRANSFORM(input AS output USING 'instruction')"""
    def __init__(self, input_column, output_name, instruction):
        self.input_column = input_column    # Identifier
        self.output_name = output_name      # string
        self.instruction = instruction      # string

class ClassifyingGroupBy(ASTNode):
    """GROUP BY CLASSIFYING source_col AS key_name"""
    def __init__(self, source_column, key_name):
        self.source_column = source_column  # Identifier
        self.key_name = key_name            # string
```

### Phase 2: 多物理算子实现（Execution Engine 层）

#### 3.2.1 Selection 物理算子扩展

**现有**: SemanticFilter (等同论文的 LLMFilterScan)

**新增**:

1. **CodegenFilterScan**: 通过 LLM 生成 Python 过滤代码，然后执行该代码进行过滤
   - 适用场景：简单逻辑谓词
   - 实现策略：调用 LLM 生成过滤函数 → 在沙箱中执行

2. **EmbeddingFilterScan**: 纯嵌入相似度过滤
   - 适用场景：语义相似度筛选
   - 实现策略：计算嵌入向量 → 余弦相似度过滤

3. **EmbeddingFilterIndexScan**: 使用向量索引的嵌入过滤
   - 适用场景：大数据量语义筛选
   - 实现策略：利用 FAISS 索引进行 k-NN 搜索

4. **HybridFilterScan**: 两阶段过滤（嵌入预过滤 + LLM 精筛）
   - 适用场景：大数据量高精度需求
   - 实现策略：先 EmbeddingFilterScan 粗筛 → 再 LLMFilterScan 精筛

#### 3.2.2 Projection 物理算子扩展

**现有**: SemanticTransform 中的 _transform_tuples (等同论文的 LLMProjection)

**新增**:

1. **LLMProjectionWithCodegen**: LLM 先生成预处理代码，再用 LLM 精炼
   - 适用场景：有规律性的提取任务
   - 实现策略：LLM 生成代码预处理 → LLM 精炼结果

#### 3.2.3 Join 物理算子扩展

**现有**: SemanticJoin (等同论文的 NestedLoopSemanticJoin)

**新增**:

1. **IndexedNestedLoopSemanticJoin (INLSJ)**: 利用向量索引加速
   - 适用场景：大表连接（inner 表有向量索引）
   - 复杂度：O(|R| * log|S|)

2. **HashSemanticJoin (HSJ)**: 利用 LSH 进行分桶连接
   - 适用场景：大表近似连接
   - 实现策略：LSH 分桶 → 桶内精确匹配

#### 3.2.4 Aggregation 物理算子扩展

**现有**: SemanticTransform 中的 _embed_then_cluster (部分实现了 EmbeddingCluster)

**新增/重构**:

1. **HashBasedSemanticAggregation**: LLM 生成分组键 + 哈希聚合
   - 适用场景：低基数分组
   - 实现策略：逐 tuple 调用 LLM 获取组键 → 哈希表聚合

2. **SortBasedSemanticAggregation**: LLM 生成分组键 + 排序聚合
   - 适用场景：高基数分组
   - 实现策略：第一遍生成 (key, tuple) 对 → 外部排序 → 第二遍聚合

3. **EmbeddingClusterAggregation** (独立算子): 嵌入 + 聚类聚合
   - 适用场景：自动发现分组模式
   - 实现策略：嵌入向量 → K-Means/DBSCAN → LLM 命名类别

#### 3.2.5 Trust-Awareness 算子

**新增**:

1. **CalibratorOperator**: 事后校准算子，可插入任意语义算子之后
   - 功能：事实验证 (fact-grounding)、置信度分析
   - 实现策略：
     - 对输出结果采样验证
     - 交叉模型验证（如有多模型可用）
     - 输出置信度评分

#### 3.2.6 辅助算子

**新增**:

1. **MultimodalInterpretation**: 非文本数据到文本的转换
   - 实现策略：根据文件类型调用对应模型（图像→描述文本，音频→转录文本）

### Phase 3: 语义直方图（Optimizer 层）

#### 3.3.1 数据结构设计

```python
class SemanticHistogram:
    """语义直方图：基于嵌入空间聚类的数据分布摘要"""

    def __init__(self, table_name, column_name, n_clusters=32):
        self.table_name = table_name
        self.column_name = column_name
        self.n_clusters = n_clusters
        self.centroids = None      # np.array, shape=(n_clusters, embed_dim)
        self.counts = None         # np.array, shape=(n_clusters,)
        self.total_count = 0

    def build(self, embeddings):
        """从嵌入向量集合构建直方图"""
        # 1. K-Means 聚类
        # 2. 记录每个聚类的质心和 tuple 数量

    def estimate_selectivity(self, query_embedding, distance_threshold=0.5):
        """估算语义谓词的选择性"""
        # 1. 计算 query_embedding 与所有质心的距离
        # 2. 找到距离在阈值内的聚类集合 C_match
        # 3. 返回 sum(counts[c] for c in C_match) / total_count
```

#### 3.3.2 构建时机
- **数据导入时**：首次加载数据时构建
- **后台任务**：异步更新（当数据发生变化时）
- **存储位置**：作为统计信息存储在 catalog 中

#### 3.3.3 使用场景
- 优化器进行物理算子选择时
- 估算语义选择后的基数
- JOIN 顺序选择

### Phase 4: 语义代价模型与优化器集成

#### 3.4.1 代价模型

```python
class SemanticCostModel:
    """统一代价模型"""

    # 预校准的准确性评分
    ACCURACY_SCORES = {
        'LLMFilterScan': 1.0,
        'CodegenFilterScan': 0.75,
        'EmbeddingFilterScan': 0.85,
        'HybridFilterScan': 0.95,
        'NestedLoopSemanticJoin': 1.0,
        'IndexedNestedLoopSemanticJoin': 0.90,
        'HashSemanticJoin': 0.90,
        'HashBasedSemanticAggregation': 0.95,
        'EmbeddingClusterAggregation': 0.80,
    }

    # 单次操作代价常数
    C_LLM = 100.0       # LLM 调用代价
    C_EMBED = 1.0        # 嵌入计算代价
    C_INDEX_LOOKUP = 0.1 # 索引查找代价

    def cost(self, operator, input_cardinality, lambda_acc=1.0):
        """计算算子总代价 = 执行代价 + λ * 准确性损失"""
        exec_cost = self.execution_cost(operator, input_cardinality)
        acc_loss = self.accuracy_loss(operator, input_cardinality)
        return exec_cost + lambda_acc * acc_loss
```

#### 3.4.2 优化器集成

在 `implementations.py` 的物理算子选择中，加入代价比较逻辑：

```python
class SemanticScanImplementation(BaseImplementation):
    @classmethod
    def on_implement(cls, old_operator):
        # 1. 获取语义直方图估算选择性
        # 2. 计算各物理算子的代价
        # 3. 选择代价最低的物理算子
        candidates = [
            LLMFilterScan(...),
            EmbeddingFilterScan(...),
            HybridFilterScan(...),
            CodegenFilterScan(...),
        ]
        best = min(candidates, key=lambda op: cost_model.cost(op, estimated_card))
        return best
```

### Phase 5: 语义清晰度验证 (Semantic Clarity Validation)

#### 3.5.1 三阶段验证

1. **语法预检 (Syntactic Pre-flight Check)**:
   - MATCHES 谓词必须是断言性的（可判断 true/false）
   - CLASSIFYING 后的表达式必须是名词/名词短语
   - 使用轻量级 LLM 判断（少量 token，单次交互）

2. **语义稳定性预演 (Semantic Stability Dry Run)**:
   - EXTRACT：对少量样本运行提取，检查输出 schema 一致性（Few-shot Consistency Check）
   - TRANSFORM：检查对语义等价的改述是否给出一致结果
   - 随机采样 5 个数据块，独立运行执行计划，比较结构是否一致

3. **运行时校准 (Runtime Calibration)**:
   - CalibratorOperator 在执行过程中进行事实验证
   - 输出置信度评分

---

## 4. 测试计划

### 4.1 单元测试

#### Parser Tests (`tests/unit/test_s2ql_parser.py`)
- 测试所有新 S²QL 语法的解析：MATCHES, EXTRACT INTO, TRANSFORM AS, CLASSIFYING
- 测试 WITH 子句解析
- 测试组合查询（CTE + 语义操作）
- 测试错误输入的异常处理

#### Logical Operator Tests (`tests/unit/test_semantic_logical.py`)
- 测试 AST → 逻辑计划的转换
- 测试各类语义逻辑算子的构建
- 测试谓词下推等优化规则

#### Physical Operator Tests (`tests/unit/test_semantic_physical.py`)
- 使用 mock LLM/embedding 模型测试各物理算子
- 测试 CodegenFilterScan 的代码生成与执行
- 测试 HybridFilterScan 的两阶段过滤
- 测试 INLSJ 和 HSJ
- 测试 CalibratorOperator

#### Semantic Histogram Tests (`tests/unit/test_semantic_histogram.py`)
- 测试直方图构建
- 测试选择性估算的准确性
- 测试边界条件（空表、单行表、全匹配等）

#### Cost Model Tests (`tests/unit/test_semantic_cost_model.py`)
- 测试代价计算公式的正确性
- 测试优化器物理算子选择的正确性
- 测试 lambda 参数的影响

#### Validation Tests (`tests/unit/test_semantic_validation.py`)
- 测试语法预检（断言性检查、名词短语检查）
- 测试 Few-shot Consistency Check
- 测试错误谓词的拒绝

### 4.2 集成测试

#### End-to-End Query Tests (`tests/integration/test_s2ql_e2e.py`)
- 测试论文中的所有示例查询 (Example 1-5)
- 使用 mock LLM 进行确定性测试
- 测试从解析到执行的完整流程

---

## 5. 文件变更清单

### 新增文件
| 文件路径 | 说明 |
|----------|------|
| `andb/sql/parser/ast/s2ql.py` | S²QL 新 AST 节点定义 |
| `andb/executor/operator/physical/semantic_filter.py` | 语义过滤物理算子（多种实现） |
| `andb/executor/operator/physical/semantic_join.py` | 语义连接物理算子（多种实现） |
| `andb/executor/operator/physical/semantic_aggregation.py` | 语义聚合物理算子（多种实现） |
| `andb/executor/operator/physical/calibrator.py` | 校准算子 |
| `andb/sql/optimizer/semantic_histogram.py` | 语义直方图 |
| `andb/sql/optimizer/semantic_cost_model.py` | 语义代价模型 |
| `andb/sql/optimizer/semantic_optimizer.py` | 语义优化器入口 |
| `andb/sql/validation/clarity_validator.py` | 语义清晰度验证 |
| `tests/unit/test_s2ql_parser.py` | S²QL 解析器测试 |
| `tests/unit/test_semantic_logical.py` | 语义逻辑算子测试 |
| `tests/unit/test_semantic_physical.py` | 语义物理算子测试 |
| `tests/unit/test_semantic_histogram.py` | 语义直方图测试 |
| `tests/unit/test_semantic_cost_model.py` | 代价模型测试 |
| `tests/unit/test_semantic_validation.py` | 验证机制测试 |
| `tests/integration/test_s2ql_e2e.py` | 端到端集成测试 |

### 修改文件
| 文件路径 | 变更内容 |
|----------|----------|
| `andb/sql/parser/lexer.py` | 新增 MATCHES, EXTRACT, TRANSFORM, CLASSIFYING tokens |
| `andb/sql/parser/parser_.py` | 新增 S²QL 语法规则 |
| `andb/sql/parser/ast/semantic.py` | 新增/修改 AST 节点 |
| `andb/executor/operator/logical.py` | 新增逻辑算子（如有必要） |
| `andb/sql/optimizer/transformations.py` | 支持新 AST 节点到逻辑计划的转换 |
| `andb/sql/optimizer/implementations.py` | 支持多物理算子选择（基于代价） |
| `andb/sql/optimizer/planner.py` | 集成语义优化器 |
| `andb/executor/operator/physical/semantic.py` | 重构，拆分为独立文件 |

---

## 6. 实现优先级与里程碑

### Milestone 1: S²QL 语法与解析 (Phase 1)
- 实现 MATCHES, EXTRACT, TRANSFORM, CLASSIFYING 的词法/语法分析
- 新增 AST 节点
- AST → 逻辑计划转换
- **测试**：parser 单元测试 + 逻辑算子测试

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
- **测试**：验证测试 + 集成测试

---

## 7. 技术决策与约束

### 7.1 向后兼容性
- 保留现有 `SEM_MATCH`, `SEM_CLUSTER`, `PROMPT` 语法作为别名
- 新 S²QL 语法与现有语法并存
- 现有测试用例不受影响

### 7.2 LLM 抽象层
- 所有 LLM 调用通过 `ClientModelFactory` 抽象
- 所有嵌入计算通过 `EmbeddingModelFactory` 抽象
- 单元测试使用 mock 实现，不依赖真实 LLM

### 7.3 代码组织
- 将大的 `semantic.py` 物理算子文件拆分为模块
- 每种算子类型一个文件，保持清晰的代码结构
- 通过 `__init__.py` 维护对外接口不变
