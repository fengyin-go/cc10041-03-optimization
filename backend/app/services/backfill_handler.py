"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BackfillHandlerService(BaseModuleService):
    MODULE = "backfill_handler"
    ENTITY = "回填任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务名称", "目标管道", "回填时间范围", "处理进度", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "目标管道", "回填时间范围"]
    STATUS_ORDER = ["待调度", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "取消回填": "回填失败"}
    NEGATIVE_ACTIONS = []
