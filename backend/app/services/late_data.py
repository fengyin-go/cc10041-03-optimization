"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class LateDataService(BaseModuleService):
    MODULE = "late_data"
    ENTITY = "迟到策略"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["策略名称", "最大延迟时间", "迟到处理方式", "侧输出目标", "生效管道"]
    REQUIRED_FIELDS = ["策略名称", "最大延迟时间", "迟到处理方式"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]
