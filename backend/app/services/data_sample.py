"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataSampleService(BaseModuleService):
    MODULE = "data_sample"
    ENTITY = "采样配置"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["配置名称", "采样比例", "采样方式", "目标表", "采样状态"]
    REQUIRED_FIELDS = ["配置名称", "采样比例", "采样方式"]
    STATUS_ORDER = ["待执行", "已执行", "已过期"]
    ACTION_RULES = {"执行采样": "已执行", "过期配置": "已过期"}
    NEGATIVE_ACTIONS = []
