"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataCleanseService(BaseModuleService):
    MODULE = "data_cleanse"
    ENTITY = "清洗步骤"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["步骤序号", "清洗类型", "输入字段", "输出字段", "清洗参数"]
    REQUIRED_FIELDS = ["步骤序号", "清洗类型", "输入字段"]
    STATUS_ORDER = ["待启用", "已启用", "已停用"]
    ACTION_RULES = {"启用步骤": "已启用", "停用步骤": "已停用"}
    NEGATIVE_ACTIONS = ["停用步骤"]
