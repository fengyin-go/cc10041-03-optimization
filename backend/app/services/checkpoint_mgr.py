"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["管道名称", "检查点序列", "创建时间", "数据偏移量", "恢复状态"]
    REQUIRED_FIELDS = ["管道名称", "检查点序列", "创建时间"]
    STATUS_ORDER = ["活跃", "已完成", "异常"]
    ACTION_RULES = {"从检查点恢复": "活跃", "废弃检查点": "异常"}
    NEGATIVE_ACTIONS = []
