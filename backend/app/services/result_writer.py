"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ResultWriterService(BaseModuleService):
    MODULE = "result_writer"
    ENTITY = "写入配置"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["配置名称", "写入目标类型", "输出格式", "分区策略", "写入模式"]
    REQUIRED_FIELDS = ["配置名称", "写入目标类型", "输出格式"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用写入": "已生效", "停用写入": "已停用"}
    NEGATIVE_ACTIONS = ["停用写入"]
