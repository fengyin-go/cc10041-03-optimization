"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class PipelineDagService(BaseModuleService):
    MODULE = "pipeline_dag"
    ENTITY = "编排图"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["图名称", "节点列表", "依赖关系", "触发方式", "调度状态"]
    REQUIRED_FIELDS = ["图名称", "节点列表", "依赖关系"]
    STATUS_ORDER = ["草稿", "已发布", "已停用"]
    ACTION_RULES = {"发布DAG": "已发布", "停用DAG": "已停用"}
    NEGATIVE_ACTIONS = ["停用DAG"]
