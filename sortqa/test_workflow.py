#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作流测试脚本
用于验证RAGFlow工作流配置是否正确

使用方法：
1. 配置下方的agent_id
2. 运行: python test_workflow.py
"""

import requests
import json

# ============================================================================
# 配置区 - 请根据实际情况修改
# ============================================================================

CONFIG = {
    "base_url": "http://192.168.110.147:8888",
    "api_key": "ragflow-E5YjI1MjJiMjBhMjExZjBiMTUzMDI0Mm",
    "agent_id": "YOUR_AGENT_ID_HERE",  # <<<< 请替换为实际的Agent ID
}

# ============================================================================
# 测试用例
# ============================================================================

TEST_CASES = [
    # (问题, 期望条件, 期望内容)
    ("查询所有合同信息", "所有", "合同信息"),
    ("查询所有预算的预算编码", "所有预算", "预算编码"),
    ("查询所有合同的乙方名称", "所有合同", "乙方名称"),
    ("查询所有机电摄像枪的设备编号及对应名称", "所有机电摄像枪", "设备编号及对应名称"),
    ("生成2024年新博高速-集团养护考核指标", "2024年新博高速-集团", "养护考核指标"),
    ("查询预算完成率超过100%的预算", "预算完成率超过100%", "预算信息"),
    ("查询设备名称为北斗授时服务器的设备卡供应商参数", "设备名称为北斗授时服务器", "设备卡供应商参数"),
    ("查询已结算的合同", "已结算", "合同信息"),
    ("按地级市统计机电摄像枪数量", "按地级市", "机电摄像枪数量"),
    ("统计合同总数", "", "合同总数"),
    ("查询乙方为广东华路交通科技有限公司的合同", "乙方为广东华路交通科技有限公司", "合同信息"),
    ("查询启用状态的机电摄像枪", "启用状态", "机电摄像枪信息"),
    ("查询路段号为148的机电摄像枪", "路段号为148", "机电摄像枪信息"),
]

# ============================================================================
# 测试函数
# ============================================================================

def test_single_question(question: str, session: requests.Session) -> dict:
    """测试单个问题"""
    api_url = f"{CONFIG['base_url']}/api/v1/agents/{CONFIG['agent_id']}/completions"
    
    try:
        response = session.post(
            api_url,
            json={"question": question, "stream": False},
            timeout=60
        )
        
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}"}
        
        data = response.json()
        
        if data.get("code") != 0:
            return {"error": f"API error: {data.get('message', 'unknown')}"}
        
        # 提取content
        content = None
        if "data" in data:
            resp_data = data["data"]
            if isinstance(resp_data, dict):
                if "data" in resp_data and isinstance(resp_data["data"], dict):
                    content = resp_data["data"].get("content", "")
                elif "content" in resp_data:
                    content = resp_data["content"]
        
        if not content:
            return {"error": "No content in response"}
        
        # 尝试解析JSON
        import re
        json_match = re.search(r'\{[^{}]*"查询条件"[^{}]*\}', content, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group())
                return {
                    "condition": result.get("查询条件", ""),
                    "content": result.get("查询内容", ""),
                    "raw": content
                }
            except json.JSONDecodeError:
                pass
        
        return {"error": f"Cannot parse JSON from: {content[:100]}"}
        
    except Exception as e:
        return {"error": str(e)}


def run_tests():
    """运行所有测试"""
    print("=" * 80)
    print("RAGFlow工作流测试")
    print("=" * 80)
    
    if CONFIG["agent_id"] == "YOUR_AGENT_ID_HERE":
        print("\n错误：请先配置Agent ID！")
        print("将 CONFIG['agent_id'] 设置为实际的Agent ID后重试")
        return
    
    print(f"\nAPI地址: {CONFIG['base_url']}")
    print(f"Agent ID: {CONFIG['agent_id']}")
    print()
    
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {CONFIG['api_key']}",
        "Content-Type": "application/json"
    })
    
    passed = 0
    failed = 0
    
    for question, expected_condition, expected_content in TEST_CASES:
        print(f"问题: {question}")
        print(f"  期望: 条件='{expected_condition}', 内容='{expected_content}'")
        
        result = test_single_question(question, session)
        
        if "error" in result:
            print(f"  错误: {result['error']}")
            failed += 1
        else:
            actual_condition = result["condition"]
            actual_content = result["content"]
            
            # 宽松匹配：检查关键词是否包含
            condition_match = (expected_condition in actual_condition or 
                               actual_condition in expected_condition or
                               expected_condition == actual_condition)
            content_match = (expected_content in actual_content or 
                             actual_content in expected_content or
                             expected_content == actual_content)
            
            status = "✓ 通过" if (condition_match and content_match) else "✗ 不匹配"
            print(f"  实际: 条件='{actual_condition}', 内容='{actual_content}'")
            print(f"  结果: {status}")
            
            if condition_match and content_match:
                passed += 1
            else:
                failed += 1
        
        print()
    
    print("=" * 80)
    print(f"测试结果: {passed} 通过, {failed} 失败, 共 {len(TEST_CASES)} 个用例")
    print("=" * 80)


if __name__ == "__main__":
    run_tests()
