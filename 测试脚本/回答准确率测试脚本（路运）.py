import pandas as pd
import requests
import json
import re
import time
from typing import List, Dict, Tuple, Optional, Union, Any
import warnings
from datetime import datetime
import os
from difflib import Differ, SequenceMatcher
import concurrent.futures
from functools import lru_cache
import logging
import psutil
import gc
from collections import Counter

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')


class EnhancedSQLValidator:
    def __init__(self, base_url: str, api_key: str, agent_id: str, dataset_ids: List[str],
                 excel_path: str, max_workers: int = 1):
        """
        增强版SQL验证器 - 宽松匹配标准

        Args:
            base_url: API基础地址
            api_key: API密钥
            agent_id: 智能体ID
            dataset_ids: 知识库ID列表
            excel_path: 测试用例Excel文件路径
            max_workers: 最大并发工作线程数
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.agent_id = agent_id
        self.dataset_ids = dataset_ids
        self.excel_path = excel_path
        self.max_workers = max_workers

        # API URLs
        self.agent_api_url = f"{self.base_url}/api/v1/agents/{agent_id}/completions"
        self.retrieval_api_url = f"{self.base_url}/api/v1/retrieval"

        # 使用Session保持连接复用
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

        # 预编译正则表达式 - SQL处理
        self._whitespace_regex = re.compile(r'\s+')
        self._table_name_regex = re.compile(r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE)
        self._column_name_regex = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
        self._where_clause_regex = re.compile(r'WHERE\s+(.*?)(?:\s+(GROUP BY|ORDER BY|LIMIT|$))',
                                              re.IGNORECASE | re.DOTALL)
        self._group_by_regex = re.compile(r'GROUP BY\s+(.*?)(?:\s+(ORDER BY|LIMIT|$))', re.IGNORECASE | re.DOTALL)
        self._order_by_regex = re.compile(r'ORDER BY\s+(.*?)(?:\s+(LIMIT|$))', re.IGNORECASE | re.DOTALL)
        self._limit_regex = re.compile(r'LIMIT\s+(\d+)', re.IGNORECASE)

        self._sql_block_regex = re.compile(r'```sql\s*(.*?)\s*```', re.DOTALL | re.IGNORECASE)
        self._generic_block_regex = re.compile(r'```\s*(.*?)\s*```', re.DOTALL)
        self._select_with_semicolon_regex = re.compile(r'(\bSELECT\b.*?;)', re.IGNORECASE | re.DOTALL)
        self._select_without_semicolon_regex = re.compile(
            r'(\bSELECT\b[\s\S]*?)(?=\b(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b|$)',
            re.IGNORECASE
        )

        # 预编译正则表达式 - 知识库召回
        self.table_pattern = re.compile(r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE)

        # 缓存
        self._sql_cache = {}
        self._api_cache = {}

    # ==================== 宽松的SQL匹配逻辑 ====================

    def is_sql_loosely_matched(self, generated_sql: str, expected_sql: str) -> Dict[str, Any]:
        """
        宽松的SQL匹配检查 - 核心匹配逻辑
        只要标准答案中的字段在生成SQL中出现就判定为正确
        """
        # 移除ORDER BY子句，因为它不影响评分
        clean_gen = self._remove_order_by(generated_sql)
        clean_exp = self._remove_order_by(expected_sql)

        # 提取关键元素
        expected_elements = self._extract_key_elements(clean_exp)
        generated_elements = self._extract_key_elements(clean_gen)

        # 检查表名一致性
        tables_match = self._check_tables_match(expected_elements['tables'], generated_elements['tables'])

        # 检查字段包含关系
        columns_contained = self._check_columns_contained(expected_elements['columns'], generated_elements['columns'])

        # 检查WHERE条件兼容性
        where_compatible = self._check_where_compatible(expected_elements['where_conditions'],
                                                        generated_elements['where_conditions'])

        # 检查GROUP BY兼容性
        group_by_compatible = self._check_group_by_compatible(expected_elements['group_by'],
                                                              generated_elements['group_by'])

        # 综合判定
        is_matched = (tables_match and columns_contained and
                      where_compatible and group_by_compatible)

        return {
            'is_matched': is_matched,
            'tables_match': tables_match,
            'columns_contained': columns_contained,
            'where_compatible': where_compatible,
            'group_by_compatible': group_by_compatible,
            'expected_elements': expected_elements,
            'generated_elements': generated_elements,
            'match_details': self._generate_match_details(expected_elements, generated_elements)
        }

    def _remove_order_by(self, sql: str) -> str:
        """移除ORDER BY子句"""
        if not sql:
            return sql

        # 使用正则表达式移除ORDER BY及其后面的内容
        pattern = re.compile(r'\s+ORDER\s+BY\s+.*?(?=LIMIT\s+|$)', re.IGNORECASE | re.DOTALL)
        sql_without_order = pattern.sub('', sql)

        # 如果移除后SQL为空，返回原始SQL
        return sql_without_order.strip() if sql_without_order.strip() else sql

    # ==================== 宽松的SQL匹配逻辑 ====================

    def _extract_key_elements(self, sql: str) -> Dict[str, Any]:
        """提取SQL关键元素"""
        if not sql:
            return {
                'tables': set(),
                'columns': set(),
                'where_conditions': set(),
                'group_by': set(),
                'aggregations': set()
            }

        # 提取表名 - 修复LIMIT错误识别问题
        tables = set()

        # 方法1: 使用更精确的正则表达式提取表名
        # 匹配 FROM table_name 或 JOIN table_name 模式，排除LIMIT等关键字
        table_matches = re.findall(r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', sql, re.IGNORECASE)
        tables.update(table_matches)

        # 方法2: 进一步清理，确保不是SQL关键字
        sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'BY', 'LIMIT', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
                        'OUTER'}
        tables = {table for table in tables if table.upper() not in sql_keywords}

        # 提取SELECT字段
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        columns = set()
        if select_match:
            select_clause = select_match.group(1)
            # 分割字段并清理
            raw_columns = [col.strip() for col in select_clause.split(',')]
            for col in raw_columns:
                # 移除别名
                if ' AS ' in col.upper():
                    col = col.upper().split(' AS ')[0].strip()
                # 提取基础列名
                base_col = self._extract_base_column(col)
                if base_col and base_col.upper() not in sql_keywords:
                    columns.add(base_col.lower())  # 使用小写进行比较

        # 提取WHERE条件中的字段
        where_conditions = set()
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+(GROUP BY|ORDER BY|LIMIT|$))', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            # 提取条件中的列名，排除SQL关键字
            condition_columns = re.findall(r'(\b[a-zA-Z_][a-zA-Z0-9_]*\b)(?=\s*[=<>!])', where_clause)
            where_conditions.update([col.lower() for col in condition_columns if col.upper() not in sql_keywords])

        # 提取GROUP BY字段
        group_by = set()
        group_match = re.search(r'GROUP BY\s+(.*?)(?:\s+(ORDER BY|LIMIT|$))', sql, re.IGNORECASE | re.DOTALL)
        if group_match:
            group_clause = group_match.group(1)
            group_columns = [col.strip().lower() for col in group_clause.split(',')]
            group_by.update([col for col in group_columns if col.upper() not in sql_keywords])

        # 提取聚合函数
        aggregations = set(re.findall(r'\b(COUNT|SUM|AVG|MAX|MIN)\s*\(', sql, re.IGNORECASE))

        logger.debug(f"提取的表名: {tables}")
        logger.debug(f"提取的字段: {columns}")

        return {
            'tables': tables,
            'columns': columns,
            'where_conditions': where_conditions,
            'group_by': group_by,
            'aggregations': aggregations
        }

    @lru_cache(maxsize=1000)
    def extract_tables_from_sql(self, sql_query: str) -> Tuple[str, ...]:
        """从SQL语句中解析表名 - 修复LIMIT错误识别问题"""
        if not sql_query or pd.isna(sql_query):
            return tuple()

        # 使用更精确的正则表达式，避免匹配到LIMIT等关键字
        table_matches = re.findall(r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', sql_query, re.IGNORECASE)

        # 过滤掉SQL关键字
        sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'BY', 'LIMIT', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
                        'OUTER'}
        clean_tables = {table for table in table_matches if table.upper() not in sql_keywords}

        logger.debug(f"从SQL解析出的表名: {clean_tables}")
        return tuple(clean_tables)

    def _extract_base_column(self, column_expr: str) -> str:
        """提取基础列名 - 增强版本"""
        # 移除表名前缀
        if '.' in column_expr:
            column_expr = column_expr.split('.')[-1]

        # 移除函数调用
        if '(' in column_expr and ')' in column_expr:
            # 提取函数参数
            match = re.search(r'\((.*?)\)', column_expr)
            if match:
                column_expr = match.group(1)

        # 清理空格和特殊字符，但保留字母数字和下划线
        column_expr = re.sub(r'[^a-zA-Z0-9_]', ' ', column_expr).strip()

        # 如果清理后包含多个单词，取第一个（可能是列名）
        if ' ' in column_expr:
            column_expr = column_expr.split(' ')[0]

        # 再次检查是否是SQL关键字
        sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'BY', 'LIMIT', 'JOIN'}
        if column_expr.upper() in sql_keywords:
            return ""

        return column_expr

    def _check_tables_match(self, expected_tables: set, generated_tables: set) -> bool:
        """检查表名是否匹配"""
        if not expected_tables:
            return True

        # 预期表名必须全部出现在生成表名中
        return expected_tables.issubset(generated_tables)

    def _check_columns_contained(self, expected_columns: set, generated_columns: set) -> bool:
        """检查字段包含关系"""
        if not expected_columns:
            return True

        # 预期字段必须全部出现在生成字段中
        # 允许生成SQL包含额外字段
        return expected_columns.issubset(generated_columns)

    def _check_where_compatible(self, expected_where: set, generated_where: set) -> bool:
        """检查WHERE条件兼容性"""
        if not expected_where:
            return True

        # 预期WHERE条件中的字段必须出现在生成WHERE条件中
        # 允许生成SQL包含额外条件
        return expected_where.issubset(generated_where)

    def _check_group_by_compatible(self, expected_group_by: set, generated_group_by: set) -> bool:
        """检查GROUP BY兼容性"""
        if not expected_group_by:
            return True

        # 预期GROUP BY字段必须出现在生成GROUP BY中
        return expected_group_by.issubset(generated_group_by)

    def _generate_match_details(self, expected_elements: Dict, generated_elements: Dict) -> Dict[str, Any]:
        """生成匹配详情"""
        return {
            'missing_tables': expected_elements['tables'] - generated_elements['tables'],
            'missing_columns': expected_elements['columns'] - generated_elements['columns'],
            'missing_where_conditions': expected_elements['where_conditions'] - generated_elements['where_conditions'],
            'missing_group_by': expected_elements['group_by'] - generated_elements['group_by'],
            'extra_tables': generated_elements['tables'] - expected_elements['tables'],
            'extra_columns': generated_elements['columns'] - expected_elements['columns'],
            'table_match_rate': len(expected_elements['tables'] & generated_elements['tables']) / max(
                len(expected_elements['tables']), 1),
            'column_match_rate': len(expected_elements['columns'] & generated_elements['columns']) / max(
                len(expected_elements['columns']), 1)
        }

    # ==================== 简化的SQL比较方法 ====================

    def compare_sql_statements(self, generated_sql: str, expected_sql: str) -> Dict:
        """比较两个SQL语句 - 使用宽松匹配标准"""
        logger.debug("开始比较SQL语句 - 宽松匹配")

        # 使用宽松匹配逻辑
        loose_match_result = self.is_sql_loosely_matched(generated_sql, expected_sql)

        # 计算相似度（仅供参考）
        normalized_gen = self.normalize_sql_cached(generated_sql)
        normalized_exp = self.normalize_sql_cached(expected_sql)
        similarity = self.calculate_similarity(normalized_gen, normalized_exp)

        # 高亮差异
        highlighted_expected, highlighted_generated = self.highlight_differences(expected_sql, generated_sql)

        # 生成匹配报告
        match_report = self._generate_loose_match_report(loose_match_result)

        return {
            'exact_match': loose_match_result['is_matched'],  # 使用宽松匹配结果
            'similarity': round(similarity, 4),
            'key_elements_match': loose_match_result['is_matched'],
            'normalized_generated': normalized_gen,
            'normalized_expected': normalized_exp,
            'highlighted_expected': highlighted_expected,
            'highlighted_generated': highlighted_generated,
            'formatted_expected': self.format_sql_for_display(expected_sql),
            'formatted_generated': self.format_sql_for_display(generated_sql),
            'loose_match_result': loose_match_result,
            'match_report': match_report
        }

    def _generate_loose_match_report(self, match_result: Dict) -> str:
        """生成宽松匹配报告"""
        details = match_result['match_details']

        report = []
        report.append("🔍 SQL宽松匹配报告")
        report.append("=" * 50)

        if match_result['is_matched']:
            report.append("✅ SQL匹配结果: 完全匹配")
        else:
            report.append("❌ SQL匹配结果: 不匹配")

        report.append("")
        report.append("📊 匹配详情:")
        report.append(f"  表名匹配: {'✅' if match_result['tables_match'] else '❌'}")
        report.append(f"  字段包含: {'✅' if match_result['columns_contained'] else '❌'}")
        report.append(f"  条件兼容: {'✅' if match_result['where_compatible'] else '❌'}")
        report.append(f"  分组兼容: {'✅' if match_result['group_by_compatible'] else '❌'}")

        # 显示缺失内容
        if details['missing_tables']:
            report.append(f"  📋 缺失表名: {', '.join(details['missing_tables'])}")
        if details['missing_columns']:
            report.append(f"  📋 缺失字段: {', '.join(details['missing_columns'])}")
        if details['missing_where_conditions']:
            report.append(f"  📋 缺失条件字段: {', '.join(details['missing_where_conditions'])}")
        if details['missing_group_by']:
            report.append(f"  📋 缺失分组字段: {', '.join(details['missing_group_by'])}")

        # 显示额外内容
        if details['extra_tables']:
            report.append(f"  📈 额外表名: {', '.join(details['extra_tables'])}")
        if details['extra_columns']:
            report.append(f"  📈 额外字段: {', '.join(details['extra_columns'])}")

        report.append("")
        report.append(f"  表名匹配率: {details['table_match_rate']:.1%}")
        report.append(f"  字段匹配率: {details['column_match_rate']:.1%}")

        return "\n".join(report)

    # ==================== 原有的工具方法 ====================

    def normalize_sql(self, sql: str) -> str:
        """标准化SQL语句以便比较 - 保留原始大小写"""
        if not sql or pd.isna(sql):
            return ""

        sql = sql.replace('\\n', ' ').replace('\\t', ' ').replace('\\r', ' ')
        sql = self._whitespace_regex.sub(' ', sql.strip())
        sql = sql.rstrip(';')
        sql = self._whitespace_regex.sub(' ', sql.strip())

        return sql

    @lru_cache(maxsize=1000)
    def normalize_sql_cached(self, sql: str) -> str:
        """带缓存的SQL标准化"""
        return self.normalize_sql(sql)

    def format_sql_for_display(self, sql: str) -> str:
        """格式化SQL用于显示 - 保留原始大小写"""
        if not sql:
            return ""

        sql = sql.replace('\\n', '\n').replace('\\t', '\t')
        sql = self._whitespace_regex.sub(' ', sql)

        replacements = [
            (' SELECT ', '\nSELECT '),
            (' FROM ', '\nFROM '),
            (' WHERE ', '\nWHERE '),
            (' ORDER BY ', '\nORDER BY '),
            (' GROUP BY ', '\nGROUP BY '),
            (' LIMIT ', '\nLIMIT '),
            (' JOIN ', '\nJOIN ')
        ]

        for old, new in replacements:
            pattern = re.compile(re.escape(old), re.IGNORECASE)
            sql = pattern.sub(new, sql)

        return sql.strip()

    def highlight_differences(self, sql1: str, sql2: str) -> Tuple[str, str]:
        """高亮显示两个SQL语句的差异"""
        norm1 = self.normalize_sql_cached(sql1)
        norm2 = self.normalize_sql_cached(sql2)

        d = Differ()
        diff = list(d.compare(norm1.split(), norm2.split()))

        highlighted1 = []
        highlighted2 = []

        for line in diff:
            if line.startswith('- '):
                highlighted1.append(f"**{line[2:]}**")
            elif line.startswith('+ '):
                highlighted2.append(f"**{line[2:]}**")
            elif line.startswith('  '):
                word = line[2:]
                highlighted1.append(word)
                highlighted2.append(word)

        return ' '.join(highlighted1), ' '.join(highlighted2)

    def calculate_similarity(self, sql1: str, sql2: str) -> float:
        """计算两个SQL语句的相似度"""
        if not sql1 or not sql2:
            return 0.0
        if sql1 == sql2:
            return 1.0
        return SequenceMatcher(None, sql1, sql2).ratio()

    def extract_complete_sql(self, text: str) -> str:
        """从文本中提取完整的SQL语句"""
        if not text:
            return ""

        sql_match = self._sql_block_regex.search(text)
        if sql_match:
            sql_content = sql_match.group(1).strip()
            logger.debug(f"从代码块提取的SQL: {sql_content}")
            return self.extract_to_semicolon(sql_content)

        sql_match = self._generic_block_regex.search(text)
        if sql_match:
            sql_content = sql_match.group(1).strip()
            if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b', sql_content, re.IGNORECASE):
                logger.debug(f"从通用代码块提取的SQL: {sql_content}")
                return self.extract_to_semicolon(sql_content)

        sql_match = self._select_with_semicolon_regex.search(text)
        if sql_match:
            sql_content = sql_match.group(1).strip()
            logger.debug(f"直接提取到分号的SQL: {sql_content}")
            return sql_content

        sql_match = self._select_without_semicolon_regex.search(text)
        if sql_match:
            sql_content = sql_match.group(1).strip()
            logger.debug(f"提取无分号的SQL: {sql_content}")
            return sql_content

        logger.debug(f"无法提取完整SQL，返回原始内容: {text}")
        return text.strip()

    def extract_to_semicolon(self, sql_text: str) -> str:
        """提取SQL直到遇到分号"""
        semicolon_pos = sql_text.find(';')
        if semicolon_pos != -1:
            return sql_text[:semicolon_pos + 1].strip()
        else:
            return sql_text.strip()

    # ==================== API调用方法 ====================

    def query_agent_for_sql(self, question: str) -> str:
        """调用智能体接口获取生成的SQL"""
        payload = {
            "question": question,
            "stream": False
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"正在查询智能体 - 尝试 {attempt + 1}/{max_retries}")

                response = self.session.post(
                    self.agent_api_url,
                    json=payload,
                    timeout=60
                )

                logger.debug(f"HTTP状态码: {response.status_code}")

                if response.status_code != 200:
                    error_msg = f"API请求失败，状态码: {response.status_code}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                data = response.json()
                sql_content = self._parse_api_response(data)
                if sql_content:
                    return sql_content
                else:
                    logger.warning("API返回成功但未解析到SQL内容")

            except requests.exceptions.Timeout:
                logger.warning(f"请求超时 - 尝试 {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                continue
            except Exception as e:
                logger.error(f"查询智能体时发生错误: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return ""

        return ""

    def _parse_api_response(self, data: dict) -> str:
        """解析API响应"""
        if data.get("code") == 0 and "data" in data:
            response_data = data["data"]

            if "data" in response_data:
                inner_data = response_data["data"]
                return self._extract_sql_from_inner_data(inner_data)

            elif "content" in response_data:
                message_content = response_data["content"]
                logger.debug(f"从外层content字段提取的内容: {message_content}")
                return self.extract_complete_sql(message_content)

        logger.warning("API响应格式不符合预期")
        return ""

    def _extract_sql_from_inner_data(self, inner_data: dict) -> str:
        """从内部数据提取SQL"""
        if "content" in inner_data:
            message_content = inner_data["content"]
            logger.debug(f"从content字段提取的内容: {message_content}")
            return self.extract_complete_sql(message_content)

        elif "outputs" in inner_data and "content" in inner_data["outputs"]:
            message_content = inner_data["outputs"]["content"]
            logger.debug(f"从outputs.content字段提取的内容: {message_content}")
            return self.extract_complete_sql(message_content)

        return ""

    # ==================== 知识库召回验证方法 ====================

    @lru_cache(maxsize=1000)
    def extract_tables_from_sql(self, sql_query: str) -> Tuple[str, ...]:
        """从SQL语句中解析表名 - 保留原始大小写"""
        if not sql_query or pd.isna(sql_query):
            return tuple()

        matches = self.table_pattern.findall(sql_query)
        clean_tables = {re.sub(r'[^a-zA-Z0-9_]', '', table) for table in matches}
        clean_tables = {table for table in clean_tables if table}

        logger.debug(f"从SQL解析出的表名: {clean_tables}")
        return tuple(clean_tables)

    def query_retrieval_api_sync(self, question: str, dataset_id: str, top_k: int = 8) -> Dict:
        """同步版本API调用"""
        cache_key = f"{dataset_id}:{question}:{top_k}"
        if cache_key in self._api_cache:
            return self._api_cache[cache_key]

        payload = {
            "question": question,
            "dataset_ids": [dataset_id],
            "similarity_threshold": 0.2,
            "vector_similarity_weight": 0.7,
            "top_k": top_k
        }

        max_retries = 3
        backoff_factor = 0.5

        for attempt in range(max_retries):
            try:
                logger.debug(f"查询知识库 '{dataset_id}' - 尝试 {attempt + 1}/{max_retries}")

                response = self.session.post(
                    self.retrieval_api_url,
                    json=payload,
                    timeout=30
                )

                if response.status_code == 401:
                    raise Exception("API认证失败，请检查API密钥")
                elif response.status_code == 404:
                    raise Exception(f"接口路径不存在: {self.retrieval_api_url}")
                elif response.status_code != 200:
                    raise Exception(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")

                data = response.json()
                result = self._process_api_response(data)
                self._api_cache[cache_key] = result
                return result

            except requests.exceptions.Timeout:
                logger.warning(f"请求超时，{attempt + 1}/{max_retries} 次尝试")
                if attempt < max_retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
                continue

            except Exception as e:
                logger.error(f"查询API时发生错误: {e}")
                if attempt < max_retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
                continue

        empty_result = {"chunks": []}
        self._api_cache[cache_key] = empty_result
        return empty_result

    def _process_api_response(self, data: Dict) -> Dict:
        """处理API响应数据"""
        if isinstance(data, dict):
            if data.get("code") == 0:
                if "data" in data:
                    if data["data"] is False:
                        logger.debug("API返回data为False，无召回结果")
                        return {"chunks": []}
                    elif isinstance(data["data"], dict):
                        return data["data"]
                    else:
                        logger.warning(f"data字段类型异常: {type(data['data'])}")
                        return {"chunks": []}
            else:
                error_msg = data.get("message", "未知错误")
                logger.error(f"API返回错误: code={data.get('code')}, message={error_msg}")

        logger.warning(f"API返回未知类型: {type(data)}")
        return {"chunks": []}

    def extract_content_and_similarity_from_chunks(self, chunks: List[Dict], top_k: int = 8) -> Tuple[
        List[str], List[float], List[Dict]]:
        """从chunks中提取内容、相似度和完整chunk信息"""
        if not chunks or not isinstance(chunks, list):
            return [], [], []

        contents = []
        similarities = []
        chunks_info = []

        for i, chunk in enumerate(chunks[:top_k]):
            if isinstance(chunk, dict):
                content = chunk.get('content', '')
                similarity = chunk.get('similarity') or chunk.get('score', 0.0)

                contents.append(content)
                similarities.append(float(similarity))

                chunk_detail = {
                    'content': content,
                    'similarity': float(similarity),
                    'document': chunk.get('document_keyword', '未知文档'),
                    'chunk_id': chunk.get('chunk_id', f'chunk_{i}'),
                    'metadata': chunk.get('metadata', {})
                }
                chunks_info.append(chunk_detail)

        while len(contents) < top_k:
            contents.append('')
            similarities.append(0.0)
            chunks_info.append({
                'content': '',
                'similarity': 0.0,
                'document': '',
                'chunk_id': '',
                'metadata': {}
            })

        return contents, similarities, chunks_info

    def check_table_in_contents(self, expected_tables: List[str], contents: List[str]) -> Tuple[
        bool, List[str], List[str]]:
        """检查期望的表名是否在召回的内容中 - 保留原始大小写"""
        if not expected_tables or not contents:
            return True, [], []

        found_tables = []
        missing_tables = []

        for expected_table in expected_tables:
            table_found = False

            for content in contents:
                if content and expected_table in content:
                    table_found = True
                    found_tables.append(expected_table)
                    break

            if not table_found:
                missing_tables.append(expected_table)

        all_found = len(missing_tables) == 0
        logger.debug(
            f"表名匹配结果: 期望{len(expected_tables)}个, 找到{len(found_tables)}个, 缺失{len(missing_tables)}个")

        return all_found, missing_tables, found_tables

    def analyze_document_distribution(self, chunks: List[Dict]) -> Dict:
        """分析文档分布情况"""
        if not chunks or not isinstance(chunks, list):
            return {
                "unique_docs": 0,
                "doc_distribution": {},
                "top_doc": ""
            }

        doc_count = {}
        for chunk in chunks:
            if isinstance(chunk, dict):
                doc_name = chunk.get('document_keyword', '未知文档')
                doc_count[doc_name] = doc_count.get(doc_name, 0) + 1

        if not doc_count:
            return {
                "unique_docs": 0,
                "doc_distribution": {},
                "top_doc": ""
            }

        sorted_docs = sorted(doc_count.items(), key=lambda x: x[1], reverse=True)

        return {
            "unique_docs": len(doc_count),
            "doc_distribution": dict(sorted_docs[:3]),
            "top_doc": sorted_docs[0][0]
        }

    # ==================== 简化的验证方法 ====================

    def process_single_test_case(self, index: int, row: pd.Series, total_rows: int) -> List[Dict]:
        """处理单个测试用例 - 使用宽松匹配标准"""
        logger.info(f"验证进度: {index + 1}/{total_rows}")

        question = self._get_column_value(row, ['question', '问题'], 0)
        expected_sql = self._get_column_value(row, ['sql', 'SQL'], 1)

        if pd.isna(question) or not question:
            logger.warning(f"跳过第 {index + 1} 行: 问题为空")
            return []

        logger.info(f"处理问题: {question[:50]}..." if len(question) > 50 else f"处理问题: {question}")

        if pd.isna(expected_sql) or not expected_sql:
            logger.warning(f"问题 {index + 1} 没有提供预期SQL")
            return self._create_error_result(index, question, expected_sql, "缺少预期SQL")

        # 步骤1: 调用智能体获取生成的SQL
        start_time = time.time()
        generated_sql = self.query_agent_for_sql(question)
        sql_response_time = time.time() - start_time

        logger.info(f"SQL生成响应时间: {sql_response_time:.2f}秒")

        if not generated_sql:
            logger.warning("智能体未返回SQL")
            return self._create_error_result(index, question, expected_sql, "智能体无返回", sql_response_time)

        # 步骤2: 比较SQL一致性（使用宽松匹配）
        sql_comparison = self.compare_sql_statements(generated_sql, expected_sql)
        # 简化的验证状态：只有完全匹配和不匹配
        sql_validation_status = "完全匹配" if sql_comparison['exact_match'] else "不匹配"

        # 步骤3: 解析预期SQL中的表名用于召回验证
        expected_tables = self.extract_tables_from_sql(expected_sql)

        results = []
        for dataset_id in self.dataset_ids:
            # 步骤4: 验证知识库召回率
            recall_result = self._validate_recall_for_dataset(
                index, question, expected_sql, dataset_id, expected_tables
            )

            # 步骤5: 合并结果
            combined_result = self._combine_simplified_results(
                index, question, expected_sql, generated_sql, sql_comparison,
                sql_validation_status, sql_response_time, recall_result
            )
            results.append(combined_result)

        # 记录结果
        status_icon = "✅" if sql_comparison['exact_match'] else "❌"
        logger.info(f"{status_icon} SQL验证: {sql_validation_status}")

        return results

    def _validate_recall_for_dataset(self, index: int, question: str, expected_sql: str,
                                     dataset_id: str, expected_tables: tuple) -> Dict:
        """验证单个知识库的召回率"""
        logger.debug(f"验证知识库召回率: {dataset_id}")

        # 查询知识库获取召回结果
        api_response = self.query_retrieval_api_sync(question, dataset_id, top_k=8)

        if not isinstance(api_response, dict):
            logger.warning(f"API响应不是字典类型: {type(api_response)}")
            api_response = {"chunks": []}

        chunks = api_response.get('chunks', [])
        contents, similarities, chunks_info = self.extract_content_and_similarity_from_chunks(chunks, top_k=8)
        doc_analysis = self.analyze_document_distribution(chunks)

        # 检查表名召回
        tables_recalled, missing_tables, found_tables = self.check_table_in_contents(
            list(expected_tables), contents
        )

        # 确定召回状态
        if not chunks:
            recall_status = "无召回"
        elif tables_recalled:
            recall_status = "完全召回"
        else:
            recall_status = "部分召回"

        return {
            'dataset_id': dataset_id,
            'expected_tables': ', '.join(expected_tables),
            'retrieval_count': len(chunks),
            'tables_recalled': tables_recalled,
            'missing_tables': ', '.join(missing_tables),
            'found_tables': ', '.join(found_tables),
            'recall_status': recall_status,
            'unique_docs': doc_analysis['unique_docs'],
            'top_doc': doc_analysis['top_doc'],
            'doc_distribution': str(doc_analysis['doc_distribution']),
            'chunks_info': chunks_info
        }

    def _combine_simplified_results(self, index: int, question: str, expected_sql: str, generated_sql: str,
                                    sql_comparison: Dict, sql_validation_status: str, sql_response_time: float,
                                    recall_result: Dict) -> Dict:
        """合并结果 - 简化版本"""
        loose_match = sql_comparison['loose_match_result']
        match_details = loose_match['match_details']

        result = {
            # 基本信息 - 使用中文列名
            '问题序号': index + 1,
            '问题内容': question,
            '预期SQL': expected_sql,
            '生成SQL': generated_sql,

            # SQL一致性结果 - 简化
            'SQL验证结果': sql_validation_status,  # 只有"完全匹配"和"不匹配"
            '是否完全匹配': sql_comparison['exact_match'],
            '相似度': round(sql_comparison['similarity'], 4),
            '响应时间秒': round(sql_response_time, 2),

            # 宽松匹配详情
            '表名匹配': loose_match['tables_match'],
            '字段包含': loose_match['columns_contained'],
            '条件兼容': loose_match['where_compatible'],
            '分组兼容': loose_match['group_by_compatible'],
            '表名匹配率': match_details['table_match_rate'],
            '字段匹配率': match_details['column_match_rate'],
            '缺失表名': ', '.join(match_details['missing_tables']),
            '缺失字段': ', '.join(match_details['missing_columns']),
            '额外表名': ', '.join(match_details['extra_tables']),
            '额外字段': ', '.join(match_details['extra_columns']),

            # 召回率结果
            '知识库ID': recall_result['dataset_id'],
            '预期表名': recall_result['expected_tables'],
            '召回片段数': recall_result['retrieval_count'],
            '表名是否召回': recall_result['tables_recalled'],
            '缺失表名召回': recall_result['missing_tables'],
            '找到表名': recall_result['found_tables'],
            '召回状态': recall_result['recall_status'],
            '唯一文档数': recall_result['unique_docs'],
            '主要文档': recall_result['top_doc']
        }

        # 添加top1到top3的召回内容
        chunks_info = recall_result.get('chunks_info', [])
        for i in range(1, 4):
            chunk_info = chunks_info[i - 1] if i - 1 < len(chunks_info) else {
                'content': '', 'similarity': 0.0, 'document': ''
            }
            result[f'Top{i}内容'] = chunk_info['content']
            result[f'Top{i}相似度'] = chunk_info['similarity']
            result[f'Top{i}文档'] = chunk_info['document']

        return result

    def _get_column_value(self, row: pd.Series, possible_columns: List[str], default_index: int) -> str:
        """从可能的列名中获取值"""
        for col in possible_columns:
            if col in row:
                value = row[col]
                return "" if pd.isna(value) else str(value)

        if len(row) > default_index:
            value = row.iloc[default_index]
            return str(value) if not pd.isna(value) else ""

        return ""

    def _create_error_result(self, index: int, question: str, expected_sql: str,
                             status: str, response_time: float = 0) -> List[Dict]:
        """创建错误结果"""
        results = []
        for dataset_id in self.dataset_ids:
            result = {
                '问题序号': index + 1,
                '问题内容': question,
                '预期SQL': expected_sql,
                '生成SQL': '',
                'SQL验证结果': '验证失败',
                '是否完全匹配': False,
                '相似度': 0,
                '响应时间秒': response_time,
                '表名匹配': False,
                '字段包含': False,
                '条件兼容': False,
                '分组兼容': False,
                '表名匹配率': 0,
                '字段匹配率': 0,
                '缺失表名': status,
                '缺失字段': '',
                '额外表名': '',
                '额外字段': '',
                '知识库ID': dataset_id,
                '预期表名': '',
                '召回片段数': 0,
                '表名是否召回': False,
                '缺失表名召回': 'SQL生成失败',
                '找到表名': '',
                '召回状态': '无法验证',
                '唯一文档数': 0,
                '主要文档': ''
            }
            for i in range(1, 4):
                result[f'Top{i}内容'] = ''
                result[f'Top{i}相似度'] = 0.0
                result[f'Top{i}文档'] = ''
            results.append(result)
        return results

    def run_comprehensive_validation(self, use_parallel: bool = True) -> pd.DataFrame:
        """执行综合验证"""
        logger.info("开始宽松匹配SQL验证...")
        logger.info(f"智能体ID: {self.agent_id}")
        logger.info(f"知识库ID: {self.dataset_ids}")

        try:
            df = pd.read_excel(self.excel_path)
            logger.info(f"成功读取Excel文件，共 {len(df)} 个测试用例")
        except Exception as e:
            logger.error(f"读取Excel文件失败: {e}")
            return pd.DataFrame()

        all_results = []

        if use_parallel and self.max_workers > 1:
            logger.info(f"使用并行处理，工作线程数: {self.max_workers}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_index = {
                    executor.submit(self.process_single_test_case, index, row, len(df)): index
                    for index, row in df.iterrows()
                }

                for future in concurrent.futures.as_completed(future_to_index):
                    try:
                        results = future.result()
                        if results:
                            all_results.extend(results)
                        if len(all_results) % 50 == 0:
                            gc.collect()
                    except Exception as e:
                        logger.error(f"处理测试用例时发生错误: {e}")
        else:
            logger.info("使用顺序处理")
            for index, row in df.iterrows():
                results = self.process_single_test_case(index, row, len(df))
                if results:
                    all_results.extend(results)
                time.sleep(0.5)

        results_df = pd.DataFrame(all_results)
        if not results_df.empty:
            results_df = results_df.sort_values(['问题序号', '知识库ID']).reset_index(drop=True)

        self.generate_simplified_report(results_df)
        return results_df

    def generate_simplified_report(self, results_df: pd.DataFrame):
        """生成简化版验证报告"""
        if results_df.empty:
            logger.warning("没有验证结果可统计")
            return

        print(f"\n{'=' * 80}")
        print("🔍 SQL宽松匹配验证统计报告")
        print(f"{'=' * 80}")

        total_tests = len(results_df)
        total_questions = len(results_df['问题序号'].unique())

        print(f"总测试次数: {total_tests}")
        print(f"总问题数: {total_questions}")
        print(f"知识库数: {len(self.dataset_ids)}")

        # SQL一致性统计
        print(f"\n📊 SQL验证结果:")
        exact_matches = len(results_df[results_df['是否完全匹配'] == True])
        not_matches = len(results_df[results_df['是否完全匹配'] == False])

        exact_match_rate = (exact_matches / total_tests) * 100

        print(f"  ✅ 完全匹配: {exact_match_rate:.1f}% ({exact_matches}/{total_tests})")
        print(f"  ❌ 不匹配: {not_matches}/{total_tests}")
        print(f"  📈 平均相似度: {results_df['相似度'].mean():.1%}")

        # 宽松匹配组件统计
        print(f"\n🎯 宽松匹配组件分析:")
        tables_match_count = len(results_df[results_df['表名匹配'] == True])
        columns_contained_count = len(results_df[results_df['字段包含'] == True])
        where_compatible_count = len(results_df[results_df['条件兼容'] == True])
        group_by_compatible_count = len(results_df[results_df['分组兼容'] == True])

        print(f"  表名匹配: {tables_match_count}/{total_tests} ({tables_match_count / total_tests * 100:.1f}%)")
        print(
            f"  字段包含: {columns_contained_count}/{total_tests} ({columns_contained_count / total_tests * 100:.1f}%)")
        print(f"  条件兼容: {where_compatible_count}/{total_tests} ({where_compatible_count / total_tests * 100:.1f}%)")
        print(
            f"  分组兼容: {group_by_compatible_count}/{total_tests} ({group_by_compatible_count / total_tests * 100:.1f}%)")

        # 匹配率统计
        print(f"\n📈 匹配率统计:")
        print(f"  平均表名匹配率: {results_df['表名匹配率'].mean():.1%}")
        print(f"  平均字段匹配率: {results_df['字段匹配率'].mean():.1%}")

        # 召回率统计
        print(f"\n📚 知识库召回率结果:")
        for dataset_id in self.dataset_ids:
            dataset_results = results_df[results_df['知识库ID'] == dataset_id]
            if len(dataset_results) == 0:
                continue

            dataset_total = len(dataset_results)
            full_recall = len(dataset_results[dataset_results['召回状态'] == '完全召回'])
            partial_recall = len(dataset_results[dataset_results['召回状态'] == '部分召回'])
            no_recall = len(dataset_results[dataset_results['召回状态'] == '无召回'])

            full_recall_rate = (full_recall / dataset_total) * 100
            effective_recall_rate = ((full_recall + partial_recall) / dataset_total) * 100

            print(f"  {dataset_id}:")
            print(f"    ✅ 完全召回: {full_recall_rate:.1f}% ({full_recall}/{dataset_total})")
            print(f"    ⚠️  部分召回: {(partial_recall / dataset_total) * 100:.1f}% ({partial_recall}/{dataset_total})")
            print(f"    📊 有效召回: {effective_recall_rate:.1f}% ({full_recall + partial_recall}/{dataset_total})")

        # 常见失败原因
        print(f"\n🔍 常见不匹配原因:")
        if not results_df.empty:
            failed_cases = results_df[results_df['是否完全匹配'] == False]
            if len(failed_cases) > 0:
                table_failures = len(failed_cases[failed_cases['表名匹配'] == False])
                column_failures = len(failed_cases[failed_cases['字段包含'] == False])
                where_failures = len(failed_cases[failed_cases['条件兼容'] == False])
                group_failures = len(failed_cases[failed_cases['分组兼容'] == False])

                print(f"  表名不匹配: {table_failures} 次")
                print(f"  字段缺失: {column_failures} 次")
                print(f"  条件不兼容: {where_failures} 次")
                print(f"  分组不兼容: {group_failures} 次")

    def save_simplified_report(self, results_df: pd.DataFrame) -> str:
        """保存简化版详细报告"""
        if results_df.empty:
            logger.warning("没有结果可保存")
            return ""

        output_dir = "单轮对话准确度匹配结果"
        os.makedirs(output_dir, exist_ok=True)

        # 获取原始Excel文件名(不带扩展名)
        base_name = os.path.splitext(os.path.basename(self.excel_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_校对_{timestamp}.xlsx")

        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 重新排列列的顺序，将关键信息放在前面
                base_columns = [
                    '问题序号', '问题内容', '知识库ID', 'SQL验证结果',
                    '是否完全匹配', '相似度', '响应时间秒',
                    '表名匹配', '字段包含', '条件兼容', '分组兼容',
                    '表名匹配率', '字段匹配率',
                    '缺失表名', '缺失字段', '额外表名', '额外字段',
                    '召回状态', '召回片段数', '表名是否召回',
                    '预期表名', '找到表名', '缺失表名召回',
                    '唯一文档数', '主要文档',
                    '预期SQL', '生成SQL'
                ]

                top_columns = []
                for i in range(1, 4):
                    top_columns.extend([f'Top{i}相似度', f'Top{i}文档', f'Top{i}内容'])

                existing_base_columns = [col for col in base_columns if col in results_df.columns]
                existing_top_columns = [col for col in top_columns if col in results_df.columns]
                remaining_columns = [col for col in results_df.columns if
                                     col not in existing_base_columns + existing_top_columns]

                final_ordered_df = results_df[existing_base_columns + existing_top_columns + remaining_columns]
                final_ordered_df.to_excel(writer, sheet_name='验证结果', index=False)

                # 自动调整列宽
                worksheet = writer.sheets['验证结果']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if cell.value and len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

                # 创建统计汇总表
                self._create_simplified_summary_sheet(writer, results_df)

        except Exception as e:
            logger.error(f"保存Excel报告失败: {e}")
            csv_file = output_file.replace('.xlsx', '.csv')
            results_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            return csv_file

        logger.info(f"验证报告已保存到: {output_file}")
        return output_file

    def _create_simplified_summary_sheet(self, writer, results_df: pd.DataFrame):
        """创建简化统计汇总表"""
        summary_data = []

        # 总体统计
        total_tests = len(results_df)
        exact_matches = len(results_df[results_df['是否完全匹配'] == True])
        exact_match_rate = (exact_matches / total_tests) * 100

        summary_data.append({
            '统计指标': '总测试次数',
            '数值': total_tests,
            '百分比': '100%'
        })

        summary_data.append({
            '统计指标': '宽松匹配次数',
            '数值': exact_matches,
            '百分比': f'{exact_match_rate:.1f}%'
        })

        summary_data.append({
            '统计指标': '平均相似度',
            '数值': f"{results_df['相似度'].mean():.1%}",
            '百分比': '-'
        })

        summary_data.append({
            '统计指标': '平均响应时间(秒)',
            '数值': f"{results_df['响应时间秒'].mean():.2f}",
            '百分比': '-'
        })

        # 匹配组件统计
        summary_data.append({
            '统计指标': '表名匹配率',
            '数值': f"{results_df['表名匹配率'].mean():.1%}",
            '百分比': '-'
        })

        summary_data.append({
            '统计指标': '字段匹配率',
            '数值': f"{results_df['字段匹配率'].mean():.1%}",
            '百分比': '-'
        })

        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='统计汇总', index=False)

    def get_memory_usage(self) -> Dict:
        """获取内存使用情况"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            return {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'cache_size': len(self._api_cache) + len(self._sql_cache)
            }
        except Exception as e:
            logger.warning(f"获取内存使用情况失败: {e}")
            return {
                'rss_mb': 0,
                'vms_mb': 0,
                'cache_size': len(self._api_cache) + len(self._sql_cache)
            }

    def clear_cache(self):
        """清空缓存"""
        self._sql_cache.clear()
        self._api_cache.clear()
        self.normalize_sql_cached.cache_clear()
        self.extract_tables_from_sql.cache_clear()
        gc.collect()


# 使用示例
if __name__ == "__main__":
    # 配置参数
    BASE_URL = "http://172.17.0.112:80"
    API_KEY = "ragflow-xHc80pMgUoWzUbmMZn6kApAQhf3orxftaSjKqFj-rN8"
    AGENT_ID = "ef711fa61d0f11f1a7ea27edac30bef8"
    DATASET_IDS = ["5a94caea000811f1a1ac0242ad420006"]
    EXCEL_PATH =  "知识库文件/新博数据管家_问数_Q_SQL_v3.0.1/新博路运q_sql_v2.0.5.xlsx"

    logger.info("开始执行宽松匹配SQL验证...")
    logger.info(f"API地址: {BASE_URL}")
    logger.info(f"智能体ID: {AGENT_ID}")
    logger.info(f"知识库ID: {DATASET_IDS}")

    # 初始化验证器
    validator = EnhancedSQLValidator(
        base_url=BASE_URL,
        api_key=API_KEY,
        agent_id=AGENT_ID,
        dataset_ids=DATASET_IDS,
        excel_path=EXCEL_PATH,
        max_workers=3
    )

    # 运行综合验证
    start_time = time.time()
    results_df = validator.run_comprehensive_validation(use_parallel=True)
    end_time = time.time()

    logger.info(f"总执行时间: {end_time - start_time:.2f} 秒")

    # 保存详细报告
    if not results_df.empty:
        output_file = validator.save_simplified_report(results_df)
        logger.info(f"验证完成！报告已生成: {output_file}")

        # 显示关键指标
        total_tests = len(results_df)
        exact_match_count = len(results_df[results_df['是否完全匹配'] == True])
        exact_match_rate = (exact_match_count / total_tests) * 100

        logger.info(f"📊 最终统计:")
        logger.info(f"  ✅ 宽松匹配率: {exact_match_rate:.1f}%")
        logger.info(f"  📈 平均相似度: {results_df['相似度'].mean():.1%}")
        logger.info(f"  🎯 表名匹配率: {results_df['表名匹配率'].mean():.1%}")
        logger.info(f"  📋 字段匹配率: {results_df['字段匹配率'].mean():.1%}")

        # 显示失败原因
        failed_cases = results_df[results_df['是否完全匹配'] == False]
        if len(failed_cases) > 0:
            logger.info(f"  🔍 失败原因分析:")
            table_failures = len(failed_cases[failed_cases['表名匹配'] == False])
            column_failures = len(failed_cases[failed_cases['字段包含'] == False])
            logger.info(f"    表名不匹配: {table_failures} 次")
            logger.info(f"    字段缺失: {column_failures} 次")
    else:
        logger.error("验证失败，无结果生成")

    # 打印内存使用情况
    memory_usage = validator.get_memory_usage()
    logger.info(f"内存使用: {memory_usage}")

    validator.clear_cache()