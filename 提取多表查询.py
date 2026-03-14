#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
提取多表查询的Q-SQL对
判断多表查询的逻辑：
1. SQL中包含 JOIN 关键字（LEFT JOIN, RIGHT JOIN, INNER JOIN, CROSS JOIN, JOIN等）
2. SQL中包含 UNION / UNION ALL（合并多个表的查询）
3. FROM子句后有多个表（逗号分隔）
4. 子查询中涉及多个不同的表
"""

import pandas as pd
import re
import os

def is_multi_table_query(sql):
    """判断SQL是否为多表查询"""
    if pd.isna(sql):
        return False
    
    sql_str = str(sql)
    sql_upper = sql_str.upper()
    
    # 1. 检查是否包含JOIN关键字
    if re.search(r'\bJOIN\b', sql_upper):
        return True
    
    # 2. 检查UNION ALL/UNION（合并多个表的查询）
    if re.search(r'\bUNION\s+(ALL\s+)?SELECT\b', sql_upper):
        return True
    
    # 3. 检查FROM子句是否有多个表（逗号分隔，排除子查询）
    from_match = re.search(r'\bFROM\s+([^(]+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|;|$)', sql_upper)
    if from_match:
        from_clause = from_match.group(1).strip()
        if ',' in from_clause:
            return True
    
    # 4. 检查子查询中是否涉及多个不同的表
    # 提取所有FROM后的表名
    from_tables = re.findall(r'\bFROM\s+[\`]?(\w+)[\`]?', sql_upper)
    # 过滤掉可能的别名（单字母或两字母）和SQL关键字
    keywords = {'SELECT', 'AS', 'ON', 'AND', 'OR', 'WHERE', 'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT'}
    real_tables = [t for t in from_tables if len(t) > 2 and t not in keywords]
    unique_tables = set(real_tables)
    
    if len(unique_tables) > 1:
        return True
    
    return False

def extract_multi_table_queries(input_file, output_file):
    """从输入文件提取多表查询并保存到输出文件"""
    # 读取Excel文件
    df = pd.read_excel(input_file, header=None)
    
    # 筛选多表查询
    multi_table_rows = []
    for i, row in df.iterrows():
        question = row[0]
        sql = row[1]
        if is_multi_table_query(sql):
            multi_table_rows.append({'问题': question, 'SQL': sql})
    
    # 创建新的DataFrame并保存
    result_df = pd.DataFrame(multi_table_rows)
    result_df.to_excel(output_file, index=False)
    
    return len(multi_table_rows), len(df)

def main():
    # 输入文件列表
    input_files = [
        '新博数据管家_问数_Q_SQL_v3.0.1/q_sql/新博报表q_sql_v3.0.1.xlsx',
        '新博数据管家_问数_Q_SQL_v3.0.1/q_sql/新博机电数据q_sql_v3.0.2.xlsx',
        '新博数据管家_问数_Q_SQL_v3.0.1/q_sql/新博路运q_sql_v2.0.1.xlsx',
        '新博数据管家_问数_Q_SQL_v3.0.1/q_sql/新博原始表q_sql_v3.0.2.xlsx'
    ]
    
    # 创建输出目录
    output_dir = '多表查询Q_SQL'
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 60)
    print("多表查询Q-SQL提取结果")
    print("=" * 60)
    
    for input_file in input_files:
        # 生成输出文件名
        base_name = os.path.basename(input_file)
        name_without_ext = os.path.splitext(base_name)[0]
        output_file = os.path.join(output_dir, f'{name_without_ext}_多表查询.xlsx')
        
        # 提取多表查询
        multi_count, total_count = extract_multi_table_queries(input_file, output_file)
        
        print(f"\n文件: {base_name}")
        print(f"  总Q-SQL对数: {total_count}")
        print(f"  多表查询数: {multi_count}")
        print(f"  输出文件: {output_file}")
    
    print("\n" + "=" * 60)
    print("提取完成！")

if __name__ == '__main__':
    main()
