"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class TransformRuleService(BaseModuleService):
    MODULE = "transform_rule"
    ENTITY = "转换规则"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["规则名称", "输入Schema", "输出Schema", "转换表达式", "异常处理策略"]
    REQUIRED_FIELDS = ["规则名称", "输入Schema", "输出Schema"]
    STATUS_ORDER = ["草稿", "已发布", "已弃用"]
    ACTION_RULES = {"发布规则": "已发布", "弃用规则": "已弃用"}
    NEGATIVE_ACTIONS = []
