"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class LineageTrackerService(BaseModuleService):
    MODULE = "lineage_tracker"
    ENTITY = "血缘条目"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["输入表", "转换管道", "输出表", "血缘类型", "记录时间"]
    REQUIRED_FIELDS = ["输入表", "转换管道", "输出表"]
    STATUS_ORDER = ["已建立", "待验证", "链路断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "链路断裂"}
    NEGATIVE_ACTIONS = []
