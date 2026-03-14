#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新博数据管家 深度思考 跨表Q-SQL生成脚本
========================================
功能：
  1. 连接到 datarms_ods 和 darm 数据库
  2. 自动扫描表结构和字典信息
  3. 调用 LLM 生成跨表、跨系统的深度思考 Q-SQL 问答对
  4. 输出为 xlsx 文件（与知识库 Q-SQL 格式一致）

使用方式：
  python generate_deep_qa.py [--host HOST] [--port PORT]
      [--darm-db DARM_DB] [--ods-db ODS_DB]
      [--user USER] [--password PASSWORD]
      [--llm-api-key KEY] [--llm-base-url URL] [--llm-model MODEL]
      [--output OUTPUT] [--count COUNT]

依赖：
  pip install pymysql openai openpyxl tqdm

说明：
  - Q列（A列）：自然语言问题
  - A列（B列）：JSON格式的多库查询计划
    {
      "analysis_plan": "...",
      "darm_sqls": ["SELECT ..."],
      "datarms_sqls": ["SELECT ..."]
    }
"""

import argparse
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

try:
    import pymysql
    import openpyxl
    from openai import OpenAI
    from tqdm import tqdm
except ImportError as e:
    print(f"[ERROR] 缺少依赖: {e}")
    print("请执行: pip install pymysql openai openpyxl tqdm")
    sys.exit(1)


# ──────────────────────────────────────────
# 1. 数据库工具
# ──────────────────────────────────────────

def get_connection(host: str, port: int, db: str, user: str, password: str):
    """建立 MySQL 连接"""
    return pymysql.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=password,
        charset="utf8mb4",
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_tables(conn, db_name: str) -> List[str]:
    """获取数据库中所有表名"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME",
            (db_name,)
        )
        return [row["TABLE_NAME"] for row in cur.fetchall()]


def fetch_ddl(conn, db_name: str, table: str) -> str:
    """获取单张表的 DDL"""
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TABLE `{db_name}`.`{table}`")
        row = cur.fetchone()
        if row:
            return list(row.values())[1]  # CREATE TABLE ...
    return ""


def fetch_columns(conn, db_name: str, table: str) -> List[Dict]:
    """获取表的列信息（名称、类型、注释）"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (db_name, table)
        )
        return cur.fetchall()


def fetch_row_count(conn, db_name: str, table: str) -> int:
    """快速估算表行数"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_ROWS FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (db_name, table)
            )
            row = cur.fetchone()
            if row and row.get("TABLE_ROWS") is not None:
                return int(row["TABLE_ROWS"])
    except Exception:
        pass
    return 0


def load_dict_files(dict_dir: str) -> Dict[str, str]:
    """
    从知识库文件夹加载 DICT .txt 文件，返回 {table_name: dict_text}
    """
    table_dict: Dict[str, str] = {}
    if not os.path.isdir(dict_dir):
        return table_dict
    for fname in os.listdir(dict_dir):
        if not fname.endswith(".txt"):
            continue
        with open(os.path.join(dict_dir, fname), encoding="utf-8") as fh:
            content = fh.read()
        # 每个 #table_name：块是一张表的字典
        for block in re.split(r"(?=^#\w)", content, flags=re.MULTILINE):
            m = re.match(r"^#(\w+)[：:]", block.strip())
            if m:
                tname = m.group(1)
                table_dict[tname] = block.strip()
    return table_dict


# ──────────────────────────────────────────
# 2. 跨表关联分析
# ──────────────────────────────────────────

# 已知跨系统实体映射（可按需扩展）
CROSS_TABLE_MAPPING = {
    "摄像枪/摄像头": ["gjdt_jd_base_camera", "ldly_base_d_camera", "jtsjzy_camera"],
    "设备基础信息": ["gjdt_jd_base_device_card_info", "ldly_base_d_camera", "ldly_base_d_cms"],
    "健康指数": ["gjdt_jd_brain_device_health_index_result", "gjdt_jd_res_device_health_index"],
    "故障工单": ["gjdt_jd_ops_order_fault_info", "gjdt_jd_ops_order_fault_info_xy"],
    "车道状态": ["gjdt_jd_monitor_lanestatus", "gjdt_jd_base_tolllane"],
    "路段基础": ["gjdt_jd_base_waysection", "gjdt_jd_base_sectionproperties"],
    "告警": ["gjdt_jd_device_alarm_msg", "gjdt_jd_monitor_road_device_alarm"],
}

# 深度思考问题模板（跨系统/跨表分析）
QUESTION_TEMPLATES = [
    # 跨系统设备统计
    "统计所有系统中{entity}的总数量并对比分析",
    "各系统中{entity}的数量分布情况如何？",
    "{entity}在机电系统、路运系统和原始表中的数量差异有多大？",
    "全面统计{entity}数量，包含所有数据系统",

    # 设备状态分析
    "各系统中启用状态的{entity}数量是多少？",
    "已删除或停用的{entity}在各系统中的占比是多少？",
    "{entity}在各系统中的启用率如何？",

    # 健康与故障
    "当前{entity}的故障工单数量及处理状态统计",
    "最近一个月内{entity}的故障发生频次统计",
    "{entity}的健康指数分布情况",
    "健康指数低于60的{entity}有多少台？",

    # 按路段统计
    "各路段的{entity}数量分布情况",
    "新台高速上{entity}的数量是多少？",
    "佛开高速和开阳高速{entity}数量对比",

    # 趋势与时间
    "近6个月的设备告警数量趋势分析",
    "近30天每天的故障工单数量统计",
    "历史维护记录中维护次数最多的设备类型",

    # 综合分析
    "全面分析当前数据库中{entity}的整体情况",
    "提供{entity}的数量统计、状态分布、路段分布的完整分析",
    "对比darm报表库和原始库中{entity}的数据一致性",
]

ENTITIES = [
    "摄像枪",
    "机电设备",
    "路运设备",
    "故障工单",
    "车道",
    "高速路段",
    "设备告警",
    "维护记录",
    "养护合同",
]


def classify_table(table_name: str) -> str:
    """按前缀分类表所属数据库"""
    if table_name.startswith("darm_"):
        return "darm"
    if any(table_name.startswith(p) for p in ("gjdt_jd_", "ldly_", "jtsjzy_", "dim_")):
        return "datarms_ods"
    return "unknown"


def build_table_catalog(
    ods_conn,
    darm_conn,
    ods_db: str,
    darm_db: str,
    dict_map: Dict[str, str],
    max_tables: int = 60,
) -> Dict[str, dict]:
    """
    构建表目录：{table_name: {db, columns, dict_text, ddl_snippet, row_count}}
    只保留行数 > 0 且有意义的表。
    """
    catalog = {}

    def _process(conn, db_name: str, prefix_filter=None):
        tables = fetch_tables(conn, db_name)
        for t in tables[:max_tables]:
            if prefix_filter and not any(t.startswith(p) for p in prefix_filter):
                continue
            rc = fetch_row_count(conn, db_name, t)
            if rc == 0:
                continue
            cols = fetch_columns(conn, db_name, t)
            ddl = fetch_ddl(conn, db_name, t)
            # 只保留前 1500 字符的 DDL 以节省 token
            ddl_snippet = ddl[:1500] if ddl else ""
            catalog[t] = {
                "db": db_name,
                "db_type": "darm" if db_name == darm_db else "datarms_ods",
                "columns": cols,
                "dict_text": dict_map.get(t, ""),
                "ddl_snippet": ddl_snippet,
                "row_count": rc,
            }

    _process(ods_conn, ods_db, prefix_filter=("gjdt_jd_", "ldly_", "jtsjzy_", "dim_"))
    _process(darm_conn, darm_db, prefix_filter=("darm_",))

    return catalog


# ──────────────────────────────────────────
# 3. LLM 调用
# ──────────────────────────────────────────

SYSTEM_PROMPT = """你是新博高速公路数据库的 SQL 专家，精通以下数据库系统：
- datarms_ods 库：gjdt_jd_* (机电一张图), ldly_* (路运系统), jtsjzy_* (原始表), dim_* (维度表)
- darm 库：darm_* (报表台账系统)

你的任务：根据给定的数据库表目录和用户问题，生成深度思考的多表 SQL 查询计划。

输出格式（严格JSON，不输出其他内容）：
{
  "analysis_plan": "分析方案：涉及哪些表、为什么",
  "darm_sqls": ["SQL1;", "SQL2;"],
  "datarms_sqls": ["SQL1;", "SQL2;"]
}

SQL生成规则：
1. 只使用表目录中真实存在的表名和字段名
2. 深度思考：对于同类数据，对每个相关系统各生成一条SQL
3. 使用中文别名（AS '字段中文名'）
4. WHERE条件要合理（如 commonDelStatus='0' 过滤已删除）
5. 使用聚合函数（COUNT/SUM/AVG）和分组统计
6. darm_ 表只放入 darm_sqls，其他表放入 datarms_sqls
7. 若信息不足，生成探索性SQL（SELECT * FROM 表名 LIMIT 20）
"""

USER_TEMPLATE = """根据以下表目录信息，为问题生成深度思考的多表SQL查询计划：

问题：{question}

相关表目录：
{table_catalog}

请生成能够全面回答该问题的SQL数组（深度思考：覆盖所有相关系统）。"""


def build_table_catalog_text(
    question: str,
    catalog: Dict[str, dict],
    cross_mapping: Dict[str, List[str]],
    max_tables: int = 15,
) -> str:
    """为给定问题选择相关表，构建 LLM 输入用的表目录文本"""
    scored: List[Tuple[str, float, dict]] = []

    kws = question.lower()
    for t, info in catalog.items():
        score = 0.0
        # 关键词匹配
        for kw in kws.split():
            if kw in t.lower():
                score += 2.0
            for col in info["columns"]:
                cmt = col.get("COLUMN_COMMENT", "") or ""
                if kw in cmt.lower():
                    score += 1.0
        # 跨系统映射
        for entity, tables in cross_mapping.items():
            if entity in question and t in tables:
                score += 5.0
        # 有 dict 的表略加分
        if info["dict_text"]:
            score += 0.5
        if score > 0:
            scored.append((t, score, info))

    # 按得分排序，取前 max_tables 张
    scored.sort(key=lambda x: -x[1])
    selected = scored[:max_tables]

    lines = []
    for t, _, info in selected:
        lines.append(f"## 表名：{t}（数据库：{info['db']}，行数约{info['row_count']}）")
        # 列信息（前 20 列）
        col_desc = ", ".join(
            f"{c['COLUMN_NAME']}({c['DATA_TYPE']}"
            + (f",注释:{c['COLUMN_COMMENT']}" if c.get("COLUMN_COMMENT") else "")
            + ")"
            for c in info["columns"][:20]
        )
        lines.append(f"字段：{col_desc}")
        # DDL 片段（前 500 字符）
        if info["ddl_snippet"]:
            lines.append(f"DDL片段：{info['ddl_snippet'][:500]}")
        # 字典值（前 400 字符）
        if info["dict_text"]:
            lines.append(f"字典值：{info['dict_text'][:400]}")
        lines.append("")

    return "\n".join(lines) if lines else "无匹配表，请检查问题关键词"


def call_llm(client: OpenAI, model: str, question: str, catalog_text: str) -> Optional[dict]:
    """调用 LLM 生成 Q-SQL 对，返回 dict 或 None"""
    user_content = USER_TEMPLATE.format(
        question=question,
        table_catalog=catalog_text,
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.1,
            max_tokens=2048,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content.strip()
        result = json.loads(raw)
        # 验证结构
        if not all(k in result for k in ("analysis_plan", "darm_sqls", "datarms_sqls")):
            raise ValueError(f"响应缺少必要字段: {list(result.keys())}")
        return result
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON 解析失败: {e}  (问题={question[:30]})")
    except Exception as e:
        print(f"  [WARN] LLM 调用失败: {e}  (问题={question[:30]})")
    return None


# ──────────────────────────────────────────
# 4. 生成 Q-SQL 并写入 xlsx
# ──────────────────────────────────────────

def generate_questions(count: int) -> List[str]:
    """生成问题列表（模板展开 + 去重）"""
    questions = []
    for tpl in QUESTION_TEMPLATES:
        if "{entity}" in tpl:
            for ent in ENTITIES:
                questions.append(tpl.replace("{entity}", ent))
        else:
            questions.append(tpl)
    # 去重并截断到 count 条
    seen = set()
    unique = []
    for q in questions:
        if q not in seen:
            seen.add(q)
            unique.append(q)
    return unique[:count]


def write_xlsx(qa_pairs: List[Tuple[str, str]], output_path: str) -> None:
    """将 Q-SQL 对写入 xlsx（Q列=问题, A列=JSON查询计划）"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "深度思考Q-SQL"
    # 表头
    ws.append(["用户问题（Q）", "多库查询计划（A）"])
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 120
    for q, a in qa_pairs:
        ws.append([q, a])
    wb.save(output_path)
    print(f"\n[✓] 已写入 {len(qa_pairs)} 条 Q-SQL 到: {output_path}")


# ──────────────────────────────────────────
# 5. 主程序
# ──────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="生成新博数据管家深度思考 Q-SQL 知识库文件",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # 数据库参数
    parser.add_argument("--host", default="127.0.0.1", help="MySQL 主机")
    parser.add_argument("--port", type=int, default=3306, help="MySQL 端口")
    parser.add_argument("--user", default="root", help="MySQL 用户名")
    parser.add_argument("--password", default="", help="MySQL 密码")
    parser.add_argument("--ods-db", default="datarms_ods", help="ODS 数据库名")
    parser.add_argument("--darm-db", default="darm", help="darm 数据库名")
    # LLM 参数
    parser.add_argument("--llm-api-key", default=os.getenv("OPENAI_API_KEY", ""), help="LLM API Key")
    parser.add_argument("--llm-base-url", default=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"), help="LLM Base URL（兼容 OpenAI 接口）")
    parser.add_argument("--llm-model", default="gpt-4o-mini", help="LLM 模型名称（如 Qwen3-32B）")
    # 输出参数
    parser.add_argument("--output", default="新博数据管家_问数_深度思考_Q_SQL_generated.xlsx", help="输出文件名")
    parser.add_argument("--count", type=int, default=50, help="生成 Q-SQL 对数量上限")
    # DICT 参数
    parser.add_argument(
        "--dict-dir",
        default=os.path.join(os.path.dirname(__file__), "../知识库文件/新博数据管家_问数_DICT_v1.0.1"),
        help="DICT 知识库文件夹路径",
    )
    # 是否实际连接数据库
    parser.add_argument("--dry-run", action="store_true", help="仅生成问题列表，不连接数据库和 LLM（调试用）")

    args = parser.parse_args()

    # ── 加载 DICT ──
    dict_dir = os.path.abspath(args.dict_dir)
    print(f"[1/5] 加载字典文件: {dict_dir}")
    dict_map = load_dict_files(dict_dir)
    print(f"      已加载 {len(dict_map)} 张表的字典值")

    if args.dry_run:
        questions = generate_questions(args.count)
        print(f"[DRY-RUN] 将生成 {len(questions)} 个问题：")
        for i, q in enumerate(questions, 1):
            print(f"  {i:3d}. {q}")
        return

    # ── 连接数据库 ──
    print(f"[2/5] 连接数据库 {args.host}:{args.port}")
    try:
        ods_conn = get_connection(args.host, args.port, args.ods_db, args.user, args.password)
        darm_conn = get_connection(args.host, args.port, args.darm_db, args.user, args.password)
        print(f"      连接成功: {args.ods_db}, {args.darm_db}")
    except Exception as e:
        print(f"[ERROR] 数据库连接失败: {e}")
        print("提示：可用 --dry-run 跳过数据库连接，仅输出问题列表")
        sys.exit(1)

    # ── 构建表目录 ──
    print("[3/5] 扫描表结构...")
    catalog = build_table_catalog(ods_conn, darm_conn, args.ods_db, args.darm_db, dict_map)
    print(f"      扫描到 {len(catalog)} 张有效表")
    if not catalog:
        print("[WARN] 未找到任何表，请检查数据库连接和权限")

    # ── 初始化 LLM ──
    print("[4/5] 初始化 LLM 客户端...")
    if not args.llm_api_key:
        print("[ERROR] 未设置 LLM API Key，请通过 --llm-api-key 或环境变量 OPENAI_API_KEY 提供")
        sys.exit(1)
    client = OpenAI(api_key=args.llm_api_key, base_url=args.llm_base_url)

    # ── 生成 Q-SQL ──
    print(f"[5/5] 生成 Q-SQL 对（上限 {args.count} 条）...")
    questions = generate_questions(args.count)
    qa_pairs: List[Tuple[str, str]] = []

    for q in tqdm(questions, desc="生成进度"):
        catalog_text = build_table_catalog_text(q, catalog, CROSS_TABLE_MAPPING)
        if not catalog_text or "无匹配表" in catalog_text:
            continue
        result = call_llm(client, args.llm_model, q, catalog_text)
        if result:
            # 过滤无效结果（两个SQL数组都为空）
            if not result["darm_sqls"] and not result["datarms_sqls"]:
                continue
            qa_pairs.append((q, json.dumps(result, ensure_ascii=False)))
        time.sleep(0.5)  # 避免速率限制

    # ── 写入 xlsx ──
    output_path = os.path.join(os.path.dirname(__file__), args.output)
    write_xlsx(qa_pairs, output_path)

    # 关闭连接
    ods_conn.close()
    darm_conn.close()
    print("[完成] Q-SQL 生成完毕！")
    print(f"  成功生成: {len(qa_pairs)} / {len(questions)} 条")
    print(f"  输出文件: {output_path}")
    print("\n下一步：")
    print("  1. 将生成的 xlsx 文件上传到 RAGflow 新博数据管家_问数_深度思考_Q_SQL 知识库")
    print("  2. 在 RAGflow 工作流 Retrieval:SharpQueriesMatch 节点中配置该知识库的 kb_id")


if __name__ == "__main__":
    main()
