# sortqa — QA用表统计工具

通过 RAGFlow 智能体工作流调用 LLM，从 Q&A 数据中提取查询条件与查询内容，自动生成 17 列格式的用表统计 Excel 文件。

## 目录结构

```
sortqa/
├── generate_table_statistics.py   # 主脚本：生成用表统计 Excel
├── test_workflow.py               # 测试脚本：验证工作流配置
├── sortqa.json                    # RAGFlow 工作流配置（可导入）
├── workflow_config.md             # 工作流创建与配置文档
├── README.md                      # 本说明文件
└── sortqa/
    └── 用表统计_生成.xlsx          # 输出文件
```

## 快速开始

### 1. 在 RAGFlow 中创建工作流

1. 登录 RAGFlow → 创建智能体 → 选择「工作流编排」模式
2. 按照 `workflow_config.md` 中的说明配置工作流，或直接导入 `sortqa.json`
3. 保存并发布工作流，复制 Agent ID

### 2. 配置脚本

编辑 `generate_table_statistics.py` 中的 `CONFIG`：

```python
CONFIG = {
    "base_url": "http://172.17.2.65:80",   # RAGFlow 服务地址
    "api_key": "ragflow-xxxxx",             # API 密钥
    "agent_id": "你的Agent ID",             # <<<< 填入这里
}
```

### 3. 测试工作流（可选）

```bash
python test_workflow.py
```

运行一系列测试用例，验证工作流是否正确配置。

### 4. 生成用表统计

```bash
python generate_table_statistics.py
```

输出文件：`sortqa/用表统计_生成.xlsx`

---

## 工作流说明

### 节点结构

```
[Begin] → [Agent] → [Message]
```

### Agent 节点关键配置

| 配置项 | 值 |
|--------|-----|
| 模型 | Qwen3-32B |
| Temperature | 0.01 |
| Top P | 0.8 |
| Max Tokens | 4096 |

### 系统提示词要点

1. 添加 `/no_think` 标记关闭思考模式
2. 明确输出格式为 JSON：`{"查询条件": "...", "查询内容": "..."}`
3. 提供充分的示例覆盖常见模式

详细配置见 `workflow_config.md`。

---

## 输出格式（17 列）

| 列 | 字段名 | 列 | 字段名 |
|----|--------|-----|--------|
| A | 所属系统 | J | 跨表查询关联表格 |
| B | 所属数据库 | K | 跨表查询入库问题 |
| C | 表名 | L | 跨表查询入库sql |
| D | 判断条件 | M | 跨库查询数量 |
| E | 查询内容 | N | 跨库查询关联数据库 |
| F | 单表查询问题数量 | O | 跨库查询关联表格 |
| G | 单表查询入库问题 | P | 跨库查询入库问题 |
| H | 单表查询入库sql | Q | 跨库查询入库sql |
| I | 跨表查询问题数量 | | |

---

## 数据库映射规则

| 表名前缀 | 数据库 | 表名前缀 | 数据库 |
|----------|--------|----------|--------|
| `darm_` | darm | `jtsjzy_` | datarms_ods |
| `gjdt_` | datarms_ods | `dim_` | datarms_ods |
| `ldly_` | datarms_ods | 其他 | datarms_ods |

如需修改映射规则，编辑 `generate_table_statistics.py` 中的 `DATABASE_MAPPING` 字典。

---

## 输入文件配置

编辑 `generate_table_statistics.py` 中的 `INPUT_FILES`：

```python
INPUT_FILES = [
    ('path/to/报表.xlsx', '报表'),
    ('path/to/机电.xlsx', '机电'),
    ('path/to/路运.xlsx', '路运'),
    ('path/to/原始.xlsx', '原始'),
]
```

系统名称映射由 `SYSTEM_MAPPING` 字典控制：

```python
SYSTEM_MAPPING = {
    '机电': '高精度地图可视化_机电一张图',
    '路运': '路段路运一体化',
    '原始': '数据资源管理平台',
    '报表': '报表台账',
}
```

---

## 常见问题

| 问题 | 解决方案 |
|------|----------|
| 提取结果不准确 | 检查工作流系统提示词是否完整，补充相关示例 |
| API 调用超时 | 增加 `CONFIG` 中的 `timeout` 和 `retry_count` |
| 找不到 Agent ID | 在 RAGFlow 智能体页面查看详情，或从浏览器 URL 获取 |
| 输出不是 JSON 格式 | 确保提示词包含 `/no_think`，Temperature 设为 0.01 |
