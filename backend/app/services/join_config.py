"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class JoinConfigService(BaseModuleService):
    MODULE = "join_config"
    ENTITY = "关联配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["关联名称", "左表源", "右表源", "关联键", "关联类型"]
    REQUIRED_FIELDS = ["关联名称", "左表源", "右表源"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用关联": "已生效", "停用关联": "已停用"}
    NEGATIVE_ACTIONS = ["停用关联"]
