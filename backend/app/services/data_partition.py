"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataPartitionService(BaseModuleService):
    MODULE = "data_partition"
    ENTITY = "分区配置"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["分区名称", "分区键", "分区类型", "分区数量", "生命周期"]
    REQUIRED_FIELDS = ["分区名称", "分区键", "分区类型"]
    STATUS_ORDER = ["待启用", "已启用", "已停用"]
    ACTION_RULES = {"启用分区": "已启用", "停用分区": "已停用"}
    NEGATIVE_ACTIONS = ["停用分区"]
