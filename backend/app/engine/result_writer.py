"""结果写入器（引擎侧）：把分支计算结果写入目标分区并交付写入回执。

与 ``result_writer`` 业务模块（写入配置的登记与启停）不同，这里承担真正的
写入职责：一条分支写一次分区，交付一份回执。编排层只凭回执确认输入齐备，
不再依赖「任务返回成功」这种不可靠信号。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.engine.partition_store import PartitionCoordinator, partition_coordinator


@dataclass
class BranchResultWriter:
    """按分支写入分区结果，返回可核对的写入回执。"""

    coordinator: PartitionCoordinator = partition_coordinator

    def deliver(
        self,
        *,
        run_id: str,
        branch: str,
        pipeline: str,
        target_table: str,
        target_partition: str,
        rows: list[dict[str, Any]],
        attempt: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """写入一条分支结果。

        返回 ``(回执, 是否为本次唯一确认)``。同一 ``(run_id, branch)`` 的并发
        重复投递只有第一次返回 ``True``，其余复用同一份回执并返回 ``False``。
        """
        receipt, accepted = self.coordinator.commit(
            run_id=run_id,
            branch=branch,
            pipeline=pipeline,
            target_table=target_table,
            target_partition=target_partition,
            rows=rows,
            attempt=attempt,
            metadata=metadata,
        )
        return receipt.as_dict(), accepted
