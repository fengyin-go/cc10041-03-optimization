"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AggregationRuleService(BaseModuleService):
    MODULE = "aggregation_rule"
    ENTITY = "聚合规则"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["规则名称", "聚合函数", "分组键", "过滤条件", "输出度量"]
    REQUIRED_FIELDS = ["规则名称", "聚合函数", "分组键"]
    STATUS_ORDER = ["草稿", "已发布", "已弃用"]
    ACTION_RULES = {"发布规则": "已发布", "弃用规则": "已弃用"}
    NEGATIVE_ACTIONS = []
