"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class IcebergTableService(BaseModuleService):
    MODULE = "iceberg_table"
    ENTITY = "Iceberg表"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["表名", "所属业务域", "存储格式", "分区定义", "当前快照数"]
    REQUIRED_FIELDS = ["表名", "所属业务域", "存储格式"]
    STATUS_ORDER = ["创建中", "已注册", "已下线"]
    ACTION_RULES = {"注册表": "已注册", "下线表": "已下线"}
    NEGATIVE_ACTIONS = ["下线表"]
