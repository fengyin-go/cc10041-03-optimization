"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ErrorHandlerService(BaseModuleService):
    MODULE = "error_handler"
    ENTITY = "异常策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "适用管道", "异常类型", "处理动作", "最大重试"]
    REQUIRED_FIELDS = ["策略名称", "适用管道", "异常类型"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]
