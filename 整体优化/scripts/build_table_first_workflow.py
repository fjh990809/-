from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROMPT_DIR = ROOT / "整体优化" / "prompts"
SOURCE = ROOT / "新博数据管家_问数_v0.24.0_260312.json"
TARGET = ROOT / "整体优化" / "workflow" / "新博数据管家_问数_v0.24.0_260312_非预置表先行优化版.json"

DICT_KB_ID = "705e7a441b9611f18db3e50efcb1c622"
MODEL_ID = "Qwen3-32B___OpenAI-API@OpenAI-API-Compatible"

TABLE_SELECTOR_ID = "Agent:TableScopeSelect"
TABLE_DICT_LOOP_ID = "Iteration:TableDictLoop"
TABLE_DICT_ITEM_ID = "IterationItem:TableDictItem"
TABLE_DICT_RETRIEVE_ID = "Retrieval:TableDictRetrieve"
DICT_TRIM_ID = "Agent:DictFieldTrim"


def load_prompt(filename: str) -> str:
    return (PROMPT_DIR / filename).read_text(encoding="utf-8").strip()


def load_workflow() -> dict:
    return json.loads(SOURCE.read_text(encoding="utf-8"))


def node_map(nodes: list[dict]) -> dict[str, dict]:
    return {node["id"]: node for node in nodes}


def shift_downstream_nodes(nodes: list[dict], min_x: float = 629.0, delta_x: float = 1050.0) -> None:
    for node in nodes:
        if node.get("parentId"):
            continue
        position = node.get("position")
        if not position:
            continue
        if position.get("x", 0) >= min_x:
            position["x"] = position["x"] + delta_x


def remove_node(nodes: list[dict], node_id: str) -> list[dict]:
    return [node for node in nodes if node["id"] != node_id]


def remove_edges(edges: list[dict], *, source: str | None = None, target: str | None = None, source_handle: str | None = None) -> list[dict]:
    result = []
    for edge in edges:
        if source is not None and edge.get("source") != source:
            result.append(edge)
            continue
        if target is not None and edge.get("target") != target:
            result.append(edge)
            continue
        if source_handle is not None and edge.get("sourceHandle") != source_handle:
            result.append(edge)
            continue
        if source is None and target is None and source_handle is None:
            result.append(edge)
            continue
    return result


def edge_exists(edges: list[dict], source: str, source_handle: str, target: str, target_handle: str) -> bool:
    return any(
        edge.get("source") == source
        and edge.get("sourceHandle") == source_handle
        and edge.get("target") == target
        and edge.get("targetHandle") == target_handle
        for edge in edges
    )


def add_edge(edges: list[dict], source: str, source_handle: str, target: str, target_handle: str) -> None:
    if edge_exists(edges, source, source_handle, target, target_handle):
        return
    edges.append(
        {
            "id": f"xy-edge__{source}{source_handle}-{target}{target_handle}",
            "source": source,
            "sourceHandle": source_handle,
            "target": target,
            "targetHandle": target_handle,
            "type": "buttonEdge",
            "markerEnd": "logo",
            "zIndex": 1001,
        }
    )


def build_table_selector_node() -> dict:
    return {
        "id": TABLE_SELECTOR_ID,
        "type": "agentNode",
        "position": {"x": 420.0, "y": -640.0},
        "sourcePosition": "right",
        "targetPosition": "left",
        "data": {
            "label": "Agent",
            "name": "表选择分析器_非预置问题",
            "form": {
                "cite": False,
                "delay_after_error": 1,
                "description": "",
                "exception_default_value": "",
                "exception_goto": [],
                "exception_method": "",
                "frequencyPenaltyEnabled": False,
                "frequency_penalty": 0.7,
                "llm_id": MODEL_ID,
                "maxTokensEnabled": False,
                "max_retries": 0,
                "max_rounds": 1,
                "max_tokens": 1024,
                "mcp": [],
                "message_history_window_size": 0,
                "outputs": {
                    "content": {"type": "string", "value": ""},
                    "structured": {
                        "properties": {
                            "tables": {
                                "description": "当前问题生成SQL时应优先使用的表名数组",
                                "type": "array",
                            }
                        },
                        "required": ["tables"],
                    },
                },
                "parameter": "Custom",
                "presencePenaltyEnabled": False,
                "presence_penalty": 0.4,
                "prompts": [
                    {
                        "role": "user",
                        "content": "\n".join(
                            [
                                "【用户问题】",
                                "{Agent:ElevenThingsChange@content}",
                                "",
                                "【表字段描述检索结果】",
                                "{Retrieval:SlowCamerasOccur@formalized_content}",
                                "",
                                "【建表语句检索结果】",
                                "{Retrieval:FloppyClothsItch@formalized_content}",
                                "",
                                "【Q-SQL样例检索结果（仅供参考，需忽略坏样例）】",
                                "{Retrieval:ModernBirdsRelate@formalized_content}",
                            ]
                        ),
                    }
                ],
                "showStructuredOutput": True,
                "sys_prompt": load_prompt("01_表选择分析器_sys_prompt.txt"),
                "temperature": 0.01,
                "temperatureEnabled": True,
                "tools": [],
                "topPEnabled": True,
                "top_p": 0.8,
                "user_prompt": "",
                "visual_files_var": "",
            },
        },
    }


def build_table_dict_loop_group() -> list[dict]:
    return [
        {
            "id": TABLE_DICT_LOOP_ID,
            "type": "group",
            "position": {"x": 820.0, "y": -820.0},
            "width": 760,
            "height": 300,
            "sourcePosition": "right",
            "targetPosition": "left",
            "data": {
                "label": "Iteration",
                "name": "按表迭代DICT_非预置问题",
                "form": {
                    "items_ref": f"{TABLE_SELECTOR_ID}@structured.tables",
                    "outputs": {
                        "dicts1": {
                            "ref": f"{DICT_TRIM_ID}@structured",
                            "type": "Array<Object>",
                        }
                    },
                },
            },
        },
        {
            "id": TABLE_DICT_ITEM_ID,
            "type": "iterationStartNode",
            "parentId": TABLE_DICT_LOOP_ID,
            "extent": "parent",
            "position": {"x": 50, "y": 100},
            "data": {
                "label": "IterationItem",
                "name": "迭代项_按表DICT",
                "form": {
                    "outputs": {
                        "index": {"type": "integer"},
                        "item": {"type": "unkown"},
                    }
                },
            },
        },
        {
            "id": TABLE_DICT_RETRIEVE_ID,
            "type": "retrievalNode",
            "parentId": TABLE_DICT_LOOP_ID,
            "extent": "parent",
            "position": {"x": 190, "y": 98},
            "sourcePosition": "right",
            "targetPosition": "left",
            "data": {
                "label": "Retrieval",
                "name": "DICT检索_按表_非预置问题",
                "form": {
                    "cross_languages": [],
                    "empty_response": "未查询到相关字典信息",
                    "kb_ids": [DICT_KB_ID],
                    "keywords_similarity_weight": 1,
                    "meta_data_filter": {},
                    "outputs": {
                        "formalized_content": {"type": "string", "value": ""},
                        "json": {"type": "Array<Object>", "value": []},
                    },
                    "query": f"{{{TABLE_DICT_ITEM_ID}@item}}",
                    "rerank_id": "",
                    "retrieval_from": "dataset",
                    "similarity_threshold": 0.2,
                    "toc_enhance": False,
                    "top_k": 1024,
                    "top_n": 4,
                    "use_kg": False,
                },
            },
        },
        {
            "id": DICT_TRIM_ID,
            "type": "agentNode",
            "parentId": TABLE_DICT_LOOP_ID,
            "extent": "parent",
            "position": {"x": 450, "y": 98},
            "sourcePosition": "right",
            "targetPosition": "left",
            "data": {
                "label": "Agent",
                "name": "字典裁剪器_非预置问题",
                "form": {
                    "cite": False,
                    "delay_after_error": 1,
                    "description": "",
                    "exception_default_value": "",
                    "exception_goto": [],
                    "exception_method": "",
                    "frequencyPenaltyEnabled": False,
                    "frequency_penalty": 0.7,
                    "llm_id": MODEL_ID,
                    "maxTokensEnabled": False,
                    "max_retries": 0,
                    "max_rounds": 1,
                    "max_tokens": 1024,
                    "mcp": [],
                    "message_history_window_size": 0,
                    "outputs": {
                        "content": {"type": "string", "value": ""},
                        "structured": {
                            "properties": {
                                "table_name": {
                                    "description": "当前字典裁剪结果所属表名",
                                    "type": "string",
                                },
                                "dict_fields": {
                                    "description": "当前问题真正需要用于条件值转换的字段字典",
                                    "type": "array",
                                },
                            },
                            "required": ["table_name", "dict_fields"],
                        },
                    },
                    "parameter": "Custom",
                    "presencePenaltyEnabled": False,
                    "presence_penalty": 0.4,
                    "prompts": [
                        {
                            "role": "user",
                            "content": "\n".join(
                                [
                                    "【用户问题】",
                                    "{Agent:ElevenThingsChange@content}",
                                    "",
                                    "【当前表名】",
                                    f"{{{TABLE_DICT_ITEM_ID}@item}}",
                                    "",
                                    "【当前表字典检索结果】",
                                    f"{{{TABLE_DICT_RETRIEVE_ID}@formalized_content}}",
                                    "",
                                    "【建表语句检索结果】",
                                    "{Retrieval:FloppyClothsItch@formalized_content}",
                                    "",
                                    "【表字段描述检索结果】",
                                    "{Retrieval:SlowCamerasOccur@formalized_content}",
                                ]
                            ),
                        }
                    ],
                    "showStructuredOutput": True,
                    "sys_prompt": load_prompt("02_字典裁剪器_sys_prompt.txt"),
                    "temperature": 0.01,
                    "temperatureEnabled": True,
                    "tools": [],
                    "topPEnabled": True,
                    "top_p": 0.8,
                    "user_prompt": "",
                    "visual_files_var": "",
                },
            },
        },
    ]


def patch_existing_nodes(nodes: list[dict]) -> None:
    nodes_by_id = node_map(nodes)

    rewrite_node = nodes_by_id["Agent:ElevenThingsChange"]
    rewrite_form = rewrite_node["data"]["form"]
    rewrite_form["sys_prompt"] = load_prompt("00_非预置问题改写器_sys_prompt.txt")
    rewrite_form["prompts"] = [
        {
            "role": "user",
            "content": "\n".join(
                [
                    "【当前用户问题】",
                    "{sys.query}",
                    "",
                    "【对话历史】",
                    "{sys.history}",
                ]
            ),
        }
    ]

    switch_node = nodes_by_id["Switch:ThreeCrewsStay"]
    switch_node["data"]["form"]["end_cpn_ids"] = [TABLE_SELECTOR_ID]

    sql_node = nodes_by_id["Agent:SilverRocketsPoke"]
    sql_form = sql_node["data"]["form"]
    sql_form["tools"] = []
    sql_form["prompts"] = [
        {
            "role": "user",
            "content": "\n".join(
                [
                    "【用户问题】",
                    "{Agent:ElevenThingsChange@content}",
                    "",
                    "【表选择结果】",
                    f"{{{TABLE_SELECTOR_ID}@structured.tables}}",
                    "",
                    "【建表语句检索结果】",
                    "{Retrieval:FloppyClothsItch@formalized_content}",
                    "",
                    "【表字段描述检索结果】",
                    "{Retrieval:SlowCamerasOccur@formalized_content}",
                    "",
                    "【Q-SQL样例检索结果（仅供参考，需忽略坏样例）】",
                    "{Retrieval:ModernBirdsRelate@formalized_content}",
                    "",
                    "【按表裁剪后的字典值】",
                    f"{{{TABLE_DICT_LOOP_ID}@dicts1}}",
                ]
            ),
        }
    ]
    sql_form["sys_prompt"] = load_prompt("03_非预置SQL生成器_sys_prompt.txt")

    summary_node = nodes_by_id["Agent:SweetJokesHear"]
    summary_node["data"]["form"]["prompts"] = [
        {
            "role": "user",
            "content": "\n".join(
                [
                    "数据库查询结果:  {VariableAggregator:BrownDucksLearn@Group0}",
                    "sql查询语句：{Agent:SilverRocketsPoke@content}",
                    "数据行数量：{VariableAggregator:RudeMasksAttack@Group0}",
                    "字段字典值：{Iteration:DullBagsYawn@dicts1}",
                ]
            ),
        }
    ]


def main() -> None:
    workflow = load_workflow()
    graph = workflow["graph"]
    nodes = graph["nodes"]
    edges = graph["edges"]

    shift_downstream_nodes(nodes)

    nodes = remove_node(nodes, "Tool:PlentyBathsFix")
    graph["nodes"] = nodes
    edges = remove_edges(edges, source="Switch:ThreeCrewsStay", target="Agent:SilverRocketsPoke", source_handle="end_cpn_ids")
    edges = remove_edges(edges, source="Agent:SilverRocketsPoke", target="Tool:PlentyBathsFix", source_handle="tool")
    graph["edges"] = edges

    patch_existing_nodes(nodes)

    existing_ids = {node["id"] for node in nodes}
    for node in [build_table_selector_node(), *build_table_dict_loop_group()]:
        if node["id"] not in existing_ids:
            nodes.append(node)

    add_edge(edges, "Switch:ThreeCrewsStay", "end_cpn_ids", TABLE_SELECTOR_ID, "end")
    add_edge(edges, "Retrieval:SlowCamerasOccur", "start", TABLE_SELECTOR_ID, "end")
    add_edge(edges, "Retrieval:FloppyClothsItch", "start", TABLE_SELECTOR_ID, "end")
    add_edge(edges, "Retrieval:ModernBirdsRelate", "start", TABLE_SELECTOR_ID, "end")
    add_edge(edges, TABLE_SELECTOR_ID, "start", TABLE_DICT_LOOP_ID, "end")
    add_edge(edges, TABLE_DICT_ITEM_ID, "start", TABLE_DICT_RETRIEVE_ID, "end")
    add_edge(edges, TABLE_DICT_RETRIEVE_ID, "start", DICT_TRIM_ID, "end")
    add_edge(edges, TABLE_SELECTOR_ID, "start", "Agent:SilverRocketsPoke", "end")
    add_edge(edges, "Retrieval:SlowCamerasOccur", "start", "Agent:SilverRocketsPoke", "end")
    add_edge(edges, TABLE_DICT_LOOP_ID, "start", "Agent:SilverRocketsPoke", "end")

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(json.dumps(workflow, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {TARGET}")


if __name__ == "__main__":
    main()
