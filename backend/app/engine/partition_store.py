"""分区级批次结果仓库：按分支提交写入回执，按回执判定分区是否齐备。

批量 DAG 的多个分支可能并行计算并写入同一目标分区。这里提供三件事：

1. **分支写入回执**：每条分支的每次写入都生成一份回执，回执是编排层确认
   「输入已齐备」的唯一凭据。
2. **幂等提交**：同一个 ``(run_id, branch)`` 的重试与原尝试并发到达时，
   分区锁只放行一份结果，后到者拿到同一份回执并被标记为重复。
3. **完整分区读取**：分区只有在登记过的期望分支全部提交后才可读；未齐备时
   读取结果不包含任何半成品行，避免汇合节点读到另一条分支尚未写完的数据。

仓库只依赖标准库，进程内共享；测试可通过 ``reset_partition_store`` 重置。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable


def _checksum(rows: Iterable[dict[str, Any]]) -> str:
    payload = json.dumps(
        list(rows), sort_keys=True, ensure_ascii=False, default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class BranchReceipt:
    """一条分支写入一份分区结果后交付的回执。"""

    receipt_id: str
    run_id: str
    branch: str
    pipeline: str
    target_table: str
    target_partition: str
    attempt: int
    row_count: int
    checksum: str
    committed_at: int
    state: str = "committed"
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "run_id": self.run_id,
            "branch": self.branch,
            "pipeline": self.pipeline,
            "target_table": self.target_table,
            "target_partition": self.target_partition,
            "attempt": self.attempt,
            "row_count": self.row_count,
            "checksum": self.checksum,
            "committed_at": self.committed_at,
            "state": self.state,
            "metadata": dict(self.metadata),
        }


@dataclass
class _BranchCommit:
    receipt: BranchReceipt
    rows: list[dict[str, Any]]


@dataclass
class _Partition:
    table: str
    partition: str
    # (run_id, branch) -> 已提交结果；同一键最多一条，保证幂等。
    commits: dict[tuple[str, str], _BranchCommit] = field(default_factory=dict)


class PartitionCoordinator:
    """线程安全的分区写入协调器。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._partitions: dict[tuple[str, str], _Partition] = {}
        # (run_id, target_table, target_partition) -> 期望参与的分支名
        self._expected: dict[tuple[str, str, str], set[str]] = {}
        self._receipts: dict[tuple[str, str, str, str], BranchReceipt] = {}
        self._attempts: list[dict[str, Any]] = []
        self._sequence = 0

    # ------------------------------------------------------------------
    # 生命周期 / 观测
    # ------------------------------------------------------------------
    def reset(self) -> None:
        with self._lock:
            self._partitions.clear()
            self._expected.clear()
            self._receipts.clear()
            self._attempts.clear()
            self._sequence = 0
            self._condition.notify_all()

    def register_expected(
        self,
        target_table: str,
        target_partition: str,
        branches: Iterable[str],
        *,
        run_id: str,
    ) -> set[str]:
        """登记某个运行在目标分区里期望出现的分支集合。"""
        key = (run_id, target_table, target_partition)
        with self._lock:
            expected = self._expected.setdefault(key, set())
            expected.update(str(branch) for branch in branches if str(branch))
            return set(expected)

    def expected_branches(
        self, target_table: str, target_partition: str, *, run_id: str
    ) -> set[str]:
        with self._lock:
            return set(self._expected.get((run_id, target_table, target_partition), set()))

    def receipt(self, receipt_id: str) -> dict[str, Any] | None:
        with self._lock:
            for receipt in self._receipts.values():
                if receipt.receipt_id == receipt_id:
                    return receipt.as_dict()
        return None

    def receipts_for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                receipt.as_dict()
                for receipt in sorted(
                    self._receipts.values(), key=lambda item: item.committed_at
                )
                if receipt.run_id == run_id
            ]

    def attempt_history(self, run_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(entry)
                for entry in self._attempts
                if run_id is None or entry.get("run_id") == run_id
            ]

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def commit(
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
    ) -> tuple[BranchReceipt, bool]:
        """提交一条分支结果，返回 ``(回执, 是否为本次首份确认)``。

        幂等键为 ``(run_id, branch)``。重试与原尝试并发到达时，锁内只会有一份
        结果落盘；后到者复用首份回执，``accepted`` 为 ``False``。
        """
        rows = [copy.deepcopy(row) for row in rows]
        idempotency_key = (run_id, branch, target_table, target_partition)
        with self._condition:
            existing = self._receipts.get(idempotency_key)
            if existing is not None:
                self._record_attempt(
                    run_id=run_id, branch=branch, pipeline=pipeline,
                    target_table=target_table, target_partition=target_partition,
                    attempt=attempt, outcome="duplicate",
                    receipt_id=existing.receipt_id,
                )
                return existing, False

            partition = self._partitions.setdefault(
                (target_table, target_partition),
                _Partition(table=target_table, partition=target_partition),
            )
            self._sequence += 1
            committed_at = int(time.time() * 1000)
            receipt = BranchReceipt(
                receipt_id=f"rcpt-{self._sequence:04d}-{run_id}-{branch}",
                run_id=run_id,
                branch=branch,
                pipeline=pipeline,
                target_table=target_table,
                target_partition=target_partition,
                attempt=attempt,
                row_count=len(rows),
                checksum=_checksum(rows),
                committed_at=committed_at,
                metadata=dict(metadata or {}),
            )
            partition.commits[(run_id, branch)] = _BranchCommit(
                receipt=receipt, rows=rows
            )
            self._receipts[idempotency_key] = receipt
            self._record_attempt(
                run_id=run_id, branch=branch, pipeline=pipeline,
                target_table=target_table, target_partition=target_partition,
                attempt=attempt, outcome="accepted",
                receipt_id=receipt.receipt_id,
            )
            self._condition.notify_all()
            return receipt, True

    def _record_attempt(self, **entry: Any) -> None:
        entry["recorded_at"] = int(time.time() * 1000)
        self._attempts.append(entry)

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------
    def read_partition(
        self,
        target_table: str,
        target_partition: str,
        *,
        run_id: str,
        expected_branches: Iterable[str] | None = None,
        wait: bool = False,
        timeout: float = 0.0,
    ) -> dict[str, Any]:
        """读取一个完整分区。

        只有期望分支全部提交后才返回行数据；超时或分支缺失时 ``complete`` 为
        ``False`` 且 ``rows`` 恒为空列表，绝不会把已提交分支的半套结果交给汇合。
        """
        deadline = time.monotonic() + max(0.0, timeout)
        with self._condition:
            if expected_branches is not None:
                # 调用方显式声明了参与集合时以它为准，避免把无关分支算进来。
                expected = {str(item) for item in expected_branches if str(item)}
            else:
                expected = self._expected.get(
                    (run_id, target_table, target_partition), set()
                )

            if wait:
                while True:
                    missing = expected - self._committed_branches_locked(
                        target_table, target_partition, run_id
                    )
                    if not missing:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)

            committed = self._committed_branches_locked(
                target_table, target_partition, run_id
            )
            missing = sorted(expected - committed)
            timed_out = bool(wait and missing)
            complete = len(missing) == 0 and (
                bool(expected) or bool(committed)
            )

            partition = self._partitions.get((target_table, target_partition))
            branch_meta: list[dict[str, Any]] = []
            receipts: list[dict[str, Any]] = []
            branch_counts: dict[str, int] = {}
            rows: list[dict[str, Any]] = []
            if partition is not None:
                for (commit_run, branch), commit in sorted(
                    partition.commits.items(), key=lambda item: (item[0][1], item[0][0])
                ):
                    if commit_run != run_id:
                        continue
                    if expected and branch not in expected:
                        continue
                    branch_meta.append({
                        "branch": branch,
                        "pipeline": commit.receipt.pipeline,
                        "receipt_id": commit.receipt.receipt_id,
                        "row_count": commit.receipt.row_count,
                        "checksum": commit.receipt.checksum,
                        "attempt": commit.receipt.attempt,
                        "committed_at": commit.receipt.committed_at,
                    })
                    receipts.append(commit.receipt.as_dict())
                    branch_counts[branch] = len(commit.rows)
                    if complete:
                        # 仅在完整时暴露行数据，且返回深拷贝防止外部改写。
                        rows.extend(copy.deepcopy(commit.rows))

            return {
                "target_table": target_table,
                "target_partition": target_partition,
                "run_id": run_id,
                "complete": complete,
                "timed_out": timed_out,
                "expected_branches": sorted(expected),
                "committed_branches": sorted(committed),
                "missing_branches": missing,
                "row_count": len(rows) if complete else 0,
                "rows": rows,
                "branch_counts": branch_counts if complete else {},
                "branches": branch_meta,
                "receipts": receipts if complete else [],
            }

    def _committed_branches_locked(
        self, target_table: str, target_partition: str, run_id: str
    ) -> set[str]:
        partition = self._partitions.get((target_table, target_partition))
        if partition is None:
            return set()
        return {
            branch
            for (commit_run, branch) in partition.commits
            if commit_run == run_id
        }

    def partition_summary(
        self, target_table: str, target_partition: str, *, run_id: str
    ) -> dict[str, Any]:
        """给编排层/接口返回的分区体检摘要（不含行数据）。"""
        snapshot = self.read_partition(
            target_table, target_partition, run_id=run_id, wait=False
        )
        return {
            "target_table": target_table,
            "target_partition": target_partition,
            "complete": snapshot["complete"],
            "expected_branches": snapshot["expected_branches"],
            "committed_branches": snapshot["committed_branches"],
            "missing_branches": snapshot["missing_branches"],
            "branch_counts": snapshot["branch_counts"],
            "row_count": snapshot["row_count"],
        }


# 进程内单例：结果写入器与 DAG 编排共享同一份分区视图。
partition_coordinator = PartitionCoordinator()


def reset_partition_store() -> None:
    partition_coordinator.reset()
