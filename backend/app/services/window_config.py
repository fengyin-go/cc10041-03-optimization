"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class WindowConfigService(BaseModuleService):
    MODULE = "window_config"
    ENTITY = "窗口配置"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["窗口名称", "窗口类型", "窗口长度", "滑动步长", "时间特征"]
    REQUIRED_FIELDS = ["窗口名称", "窗口类型", "窗口长度"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用窗口": "已生效", "停用窗口": "已停用"}
    NEGATIVE_ACTIONS = ["停用窗口"]
