"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BatchPipelineService(BaseModuleService):
    MODULE = "batch_pipeline"
    ENTITY = "计算管道"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["管道名称", "输入源", "输出目标", "计算引擎", "调度状态"]
    REQUIRED_FIELDS = ["管道名称", "输入源", "输出目标"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "运行失败"]
    ACTION_RULES = {"启动管道": "运行中", "停止管道": "运行失败"}
    NEGATIVE_ACTIONS = []
