"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class OutputAuditService(BaseModuleService):
    MODULE = "output_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["管道名称", "输出目标", "输出时间", "输出行数", "校验结果"]
    REQUIRED_FIELDS = ["管道名称", "输出目标", "输出时间"]
    STATUS_ORDER = ["已记录", "校验失败"]
    ACTION_RULES = {"重新校验": "已记录", "标记通过": "校验失败"}
    NEGATIVE_ACTIONS = []
