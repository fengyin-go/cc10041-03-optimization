"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class PipelineMonitorService(BaseModuleService):
    MODULE = "pipeline_monitor"
    ENTITY = "监控指标"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["指标名称", "监控目标", "告警阈值", "检查频率", "生效状态"]
    REQUIRED_FIELDS = ["指标名称", "监控目标", "告警阈值"]
    STATUS_ORDER = ["待启用", "已启用", "已关闭"]
    ACTION_RULES = {"启用监控": "已启用", "关闭监控": "已关闭"}
    NEGATIVE_ACTIONS = []
