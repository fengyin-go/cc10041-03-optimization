"""数据分区领域服务：分区配置流转 + 运行时分区完整性视图。"""
from __future__ import annotations

from typing import Any

from app.engine.partition_store import partition_coordinator
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

    def runtime_snapshot(
        self, *, target_table: str, target_partition: str, run_id: str
    ) -> dict[str, Any]:
        """返回某个运行在目标分区上的分支参与量与完整性判定。"""
        return partition_coordinator.partition_summary(
            target_table, target_partition, run_id=run_id
        )
