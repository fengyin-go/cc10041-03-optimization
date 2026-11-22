"""批量管道引擎：DAG 分支并行编排、回执驱动的汇合、回填续做、输出审计。

真实数据流：
  batch_pipeline → pipeline_dag → 分支并行计算 → result_writer(写入回执)
  → join_config(按回执汇合完整分区) → output_audit → checkpoint_mgr

编排语义：
  * 入度为 0 的节点作为分支并行启动；有上游依赖的节点等上游回执齐备才放行。
  * 汇合节点读取上游分支写出的目标分区，只读完整分区，绝不读半成品。
  * 同一 ``(run_id, branch)`` 的重试与原尝试并发到达只确认一份结果。
  * 与 ``join_config`` 的关联键定义对应：汇合语义由 DAG 依赖边表达。
"""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any

from app.engine.partition_store import PartitionCoordinator, partition_coordinator
from app.engine.result_writer import BranchResultWriter
from app.store import store


@dataclass
class BatchPipelineEngine:
    """批量管道编排与调度执行器。"""

    pipeline_key: str = "batch_pipeline"
    dag_key: str = "pipeline_dag"
    backfill_key: str = "backfill_handler"
    output_audit_key: str = "output_audit"
    checkpoint_key: str = "checkpoint_mgr"
    result_writer_key: str = "result_writer"
    lineage_key: str = "lineage_tracker"

    coordinator: PartitionCoordinator = partition_coordinator
    max_workers: int = 8
    join_timeout: float = 5.0
    # 故障注入：{管道名: 需要失败的尝试次数}。第一次尝试失败、重试成功，
    # 用于验证重试与原尝试并发相遇时只确认一份分支结果。
    fail_first_attempts: dict[str, int] = field(default_factory=dict)

    run_id: str = ""
    tasks_completed: int = 0
    tasks_failed: int = 0
    execution_log: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._fault_plan: dict[str, int] = {}
        self._plan_parents: dict[str, list[str]] = {}
        self._plan_join_nodes: set[str] = set()

    # ------------------------------------------------------------------
    # DAG 解析
    # ------------------------------------------------------------------
    def run_dag(
        self,
        *,
        dag_name: str | None = None,
        fault_plan: dict[str, int] | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """解析 DAG 依赖，独立分支并行执行，汇合节点按回执读取完整分区。"""
        self.run_id = run_id or f"batch-{int(time.time() * 1000)}"
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.execution_log = []
        self._fault_plan = dict(fault_plan or self.fail_first_attempts)

        dag = self._resolve_dag(dag_name)
        if not dag:
            return self._empty("未找到可发布的DAG")

        plan = self._parse_dag(dag)
        if not plan["nodes"]:
            return self._empty("DAG 未配置任何节点")

        self._plan_parents = plan["parents"]
        self._plan_join_nodes = set(plan["join_nodes"])
        self._register_expected_branches(plan)
        results = self._execute_plan(plan)

        self._write_audit()
        self._track_lineage(dag)
        return {
            "run_id": self.run_id,
            "dag_name": dag.get("图名称", ""),
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "total_tasks": len(plan["nodes"]),
            "execution_log": self.execution_log,
            "branches": results["branches"],
            "joins": results["joins"],
            "receipts": self.coordinator.receipts_for_run(self.run_id),
        }

    def _resolve_dag(self, dag_name: str | None) -> dict[str, Any] | None:
        dags = store.rows(self.dag_key)
        published = [d for d in dags if d.get("status") == "已发布"]
        if not published:
            return None
        if dag_name:
            for d in published:
                if d.get("图名称") == dag_name:
                    return d
            return None
        return published[0]

    def _parse_dag(self, dag: dict[str, Any]) -> dict[str, Any]:
        """把节点列表/依赖关系解析成邻接表、入度表和汇合节点集合。"""
        nodes_raw = dag.get("节点列表", "")
        edges_raw = dag.get("依赖关系", "")
        nodes = [n.strip() for n in str(nodes_raw).split(",") if n.strip()]
        # 去重且保序，避免配置里重复写同一节点导致重复调度。
        nodes = list(dict.fromkeys(nodes))

        edges: list[tuple[str, str]] = []
        for edge in str(edges_raw).split(","):
            parts = [part.strip() for part in edge.strip().split("->")]
            if len(parts) == 2 and parts[0] and parts[1]:
                if parts[0] in nodes and parts[1] in nodes:
                    edges.append((parts[0], parts[1]))

        parents: dict[str, list[str]] = {node: [] for node in nodes}
        children: dict[str, list[str]] = {node: [] for node in nodes}
        for src, dst in edges:
            parents[dst].append(src)
            children[src].append(dst)

        # 汇合节点：有 2 个及以上上游的分支终点。
        join_nodes = {node for node in nodes if len(parents[node]) >= 2}
        return {
            "nodes": nodes,
            "parents": parents,
            "children": children,
            "join_nodes": join_nodes,
            "has_cycle": self._has_cycle(nodes, children),
        }

    def _has_cycle(
        self, nodes: list[str], children: dict[str, list[str]]
    ) -> bool:
        state: dict[str, int] = {node: 0 for node in nodes}

        def visit(node: str) -> bool:
            state[node] = 1
            for child in children.get(node, []):
                if state.get(child) == 1:
                    return True
                if state.get(child) == 0 and visit(child):
                    return True
            state[node] = 2
            return False

        return any(state[node] == 0 and visit(node) for node in nodes)

    def _topological_sort(self, dag: dict[str, Any]) -> list[str]:
        """按依赖关系拓扑排序 DAG 节点（保留给外部调用与兼容旧行为）。"""
        plan = self._parse_dag(dag)
        if plan["has_cycle"]:
            return plan["nodes"]
        in_degree = {node: len(plan["parents"][node]) for node in plan["nodes"]}
        queue = [node for node in plan["nodes"] if in_degree[node] == 0]
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for child in plan["children"][node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
        return order if len(order) == len(plan["nodes"]) else plan["nodes"]

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def _register_expected_branches(self, plan: dict[str, Any]) -> None:
        """在执行前登记每个目标分区期望参与的分支。

        期望集合只统计「写分区结果的节点」：非汇合的分支节点。汇合节点本身
        写回的是汇合产物分区，单独登记，避免与上游分支混在同一个期望集合里。
        """
        by_partition: dict[tuple[str, str], set[str]] = {}
        for node in plan["nodes"]:
            if node in plan["join_nodes"]:
                continue
            pipeline = self._pipeline_for(node)
            if pipeline is None:
                continue
            table, partition = self._target_of(pipeline)
            by_partition.setdefault((table, partition), set()).add(node)
        for (table, partition), branches in by_partition.items():
            self.coordinator.register_expected(
                table, partition, branches, run_id=self.run_id
            )

    def _execute_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        if plan["has_cycle"]:
            for node in plan["nodes"]:
                self._log_failure(node, "DAG 存在环，无法调度")
            return {"branches": [], "joins": []}

        parents = plan["parents"]
        children = plan["children"]
        nodes = plan["nodes"]

        pending: set[str] = set(nodes)
        completed: dict[str, dict[str, Any]] = {}
        branch_logs: list[dict[str, Any]] = []
        join_logs: list[dict[str, Any]] = []

        def ready(node: str) -> bool:
            return all(parent in completed for parent in parents[node])

        def run_node(node: str) -> dict[str, Any]:
            if node in plan["join_nodes"]:
                return self._execute_join(node, parents[node])
            return self._execute_branch(node)

        # 独立分支在线程池中并行；节点就绪（上游全部完成）后立即提交。
        with ThreadPoolExecutor(max_workers=max(1, self.max_workers)) as pool:
            futures: dict[Any, str] = {}
            for node in nodes:
                if not parents[node]:
                    futures[pool.submit(run_node, node)] = node
                    pending.discard(node)

            while futures:
                done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
                for future in done:
                    node = futures.pop(future)
                    result = future.result()
                    completed[node] = result
                    if result.get("kind") == "join":
                        join_logs.append(result)
                    else:
                        branch_logs.append(result)
                    self._record_result(node, result)
                    for child in children[node]:
                        if child in pending and ready(child):
                            pending.discard(child)
                            futures[pool.submit(run_node, child)] = child

            for node in sorted(pending):
                self._log_failure(node, "上游分支未全部完成，节点不启动")

        branch_logs.sort(key=lambda item: item.get("node", ""))
        join_logs.sort(key=lambda item: item.get("node", ""))
        return {"branches": branch_logs, "joins": join_logs}

    def _pipeline_for(self, node: str) -> dict[str, Any] | None:
        pipelines = store.rows(self.pipeline_key)
        matched = [p for p in pipelines if p.get("管道名称") == node]
        return matched[0] if matched else None

    def _target_of(self, pipeline: dict[str, Any]) -> tuple[str, str]:
        table = str(pipeline.get("输出目标") or pipeline.get("管道名称") or "未配置目标")
        partition = str(
            pipeline.get("目标分区")
            or pipeline.get("输出分区")
            or "默认分区"
        )
        return table, partition

    def _execute_branch(self, node: str) -> dict[str, Any]:
        """执行一条分支：计算 → 写入 → 交付回执；首次失败后自动重试。"""
        started_at = int(time.time() * 1000)
        pipeline = self._pipeline_for(node)
        if pipeline is None:
            result = {
                "node": node,
                "kind": "branch",
                "status": "skipped",
                "reason": "未找到对应管道配置",
                "started_at_ms": started_at,
            }
            return result
        table, partition = self._target_of(pipeline)
        writer_cfg = self._writer_config()
        writer = BranchResultWriter(self.coordinator)

        attempts: list[dict[str, Any]] = []
        max_attempts = 2
        receipt: dict[str, Any] | None = None
        accepted = False
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            started = time.time()
            if self._should_inject_failure(node, attempt):
                attempts.append({
                    "attempt": attempt,
                    "outcome": "failed",
                    "reason": "模拟写入失败",
                    "elapsed_ms": round((time.time() - started) * 1000, 3),
                })
                with self._lock:
                    pipeline["status"] = "运行失败"
                    pipeline["abnormal"] = True
                    pipeline["调度状态"] = f"{self.run_id} 第{attempt}次尝试失败，等待重试"
                continue

            rows = self._compute_rows(pipeline, table, partition, attempt)
            receipt, accepted = writer.deliver(
                run_id=self.run_id,
                branch=node,
                pipeline=str(pipeline.get("管道名称", node)),
                target_table=table,
                target_partition=partition,
                rows=rows,
                attempt=attempt,
                metadata={
                    "engine": pipeline.get("计算引擎", ""),
                    "writer": writer_cfg.get("配置名称", ""),
                },
            )
            attempts.append({
                "attempt": attempt,
                "outcome": "accepted" if accepted else "duplicate",
                "receipt_id": receipt["receipt_id"],
                "row_count": receipt["row_count"],
                "elapsed_ms": round((time.time() - started) * 1000, 3),
            })
            break

        if receipt is None:
            return {
                "node": node,
                "kind": "branch",
                "status": "failed",
                "pipeline": pipeline.get("管道名称", ""),
                "target_table": table,
                "target_partition": partition,
                "attempts": attempts,
                "started_at_ms": started_at,
                "finished_at_ms": int(time.time() * 1000),
            }

        with self._lock:
            pipeline["status"] = "已完成"
            pipeline["abnormal"] = False
            pipeline["调度状态"] = f"已被{self.run_id}执行"
        return {
            "node": node,
            "kind": "branch",
            "status": "completed",
            "pipeline": pipeline.get("管道名称", ""),
            "target_table": table,
            "target_partition": partition,
            "receipt_id": receipt["receipt_id"],
            "receipt_accepted": accepted,
            "row_count": receipt["row_count"],
            "attempts": attempts,
            "started_at_ms": started_at,
            "finished_at_ms": int(time.time() * 1000),
        }

    def _execute_join(self, node: str, upstream: list[str]) -> dict[str, Any]:
        """汇合节点：等上游分支回执齐备，读取完整的分区输入后再计算。

        汇合读取的是「上游分支写出的分区」，而不是自己的输出目标。若多条上游
        分支写同一目标分区，则该分区只有在上游回执全部到齐后才可读。
        """
        pipeline = self._pipeline_for(node)
        inputs = self._join_inputs(upstream)
        snapshots: list[dict[str, Any]] = []
        for table, partition, expected in inputs:
            snapshot = self.coordinator.read_partition(
                table,
                partition,
                run_id=self.run_id,
                expected_branches=expected,
                wait=True,
                timeout=self.join_timeout,
            )
            snapshots.append(snapshot)

        blocked = [snap for snap in snapshots if not snap["complete"]]
        if blocked:
            return {
                "node": node,
                "kind": "join",
                "status": "blocked",
                "reason": "上游分支回执未齐备，汇合不读取半成品分区",
                "inputs": [
                    {
                        "target_table": snap["target_table"],
                        "target_partition": snap["target_partition"],
                        "expected_branches": snap["expected_branches"],
                        "missing_branches": snap["missing_branches"],
                    }
                    for snap in snapshots
                ],
                "missing_branches": sorted({
                    branch for snap in blocked for branch in snap["missing_branches"]
                }),
            }

        joined_rows = [row for snap in snapshots for row in snap["rows"]]
        joined = {
            "node": node,
            "kind": "join",
            "status": "completed",
            "pipeline": pipeline.get("管道名称", "") if pipeline else "",
            "inputs": [
                {
                    "target_table": snap["target_table"],
                    "target_partition": snap["target_partition"],
                    "expected_branches": snap["expected_branches"],
                    "committed_branches": snap["committed_branches"],
                    "branch_counts": snap["branch_counts"],
                    "row_count": snap["row_count"],
                    "receipts": [item["receipt_id"] for item in snap["receipts"]],
                }
                for snap in snapshots
            ],
            "branch_counts": {
                key: value
                for snap in snapshots
                for key, value in snap["branch_counts"].items()
            },
            "row_count": len(joined_rows),
            "receipts": [
                item["receipt_id"] for snap in snapshots for item in snap["receipts"]
            ],
        }

        if pipeline is not None:
            with self._lock:
                pipeline["status"] = "已完成"
                pipeline["abnormal"] = False
                pipeline["调度状态"] = (
                    f"已被{self.run_id}汇合，读取{len(joined_rows)}行"
                )
            # 汇合产物也落一份回执，供下游节点继续按回执等待。
            join_table, join_partition = self._target_of(pipeline)
            self.coordinator.register_expected(
                join_table, join_partition, {node}, run_id=self.run_id
            )
            receipt, _ = self.coordinator.commit(
                run_id=self.run_id,
                branch=node,
                pipeline=str(pipeline.get("管道名称", node)),
                target_table=join_table,
                target_partition=join_partition,
                rows=joined_rows,
                attempt=1,
                metadata={
                    "joined_from": sorted(
                        branch for snap in snapshots for branch in snap["committed_branches"]
                    ),
                    "kind": "join",
                },
            )
            joined["target_table"] = join_table
            joined["target_partition"] = join_partition
            joined["receipt_id"] = receipt.receipt_id
        return joined

    def _join_inputs(
        self, upstream: list[str]
    ) -> list[tuple[str, str, set[str]]]:
        """按上游分支的输出目标归组，得到汇合应读取的完整分区列表。"""
        grouped: dict[tuple[str, str], set[str]] = {}
        for parent in upstream:
            pipeline = self._pipeline_for(parent)
            if pipeline is None:
                continue
            table, partition = self._target_of(pipeline)
            grouped.setdefault((table, partition), set()).add(parent)
        return [
            (table, partition, branches)
            for (table, partition), branches in sorted(grouped.items())
        ]

    def _compute_rows(
        self, pipeline: dict[str, Any], table: str, partition: str, attempt: int
    ) -> list[dict[str, Any]]:
        """根据管道输入源产生分支结果行（确定性，便于核对参与量）。"""
        name = str(pipeline.get("管道名称", ""))
        seed = deterministic_seed(name)
        rng = random.Random(seed)
        row_total = rng.randint(3, 9)
        rows: list[dict[str, Any]] = []
        for index in range(row_total):
            rows.append({
                "branch": name,
                "target_table": table,
                "target_partition": partition,
                "row_index": index,
                "input_source": pipeline.get("输入源", ""),
                "engine": pipeline.get("计算引擎", ""),
                "computed_at_ms": 1_700_000_000_000 + seed + index,
                "attempt": attempt,
            })
        # 加一点计算耗时，模拟真实批处理，让并行与串行的差异可观测。
        time.sleep(0.01)
        return rows

    def _writer_config(self) -> dict[str, Any]:
        writers = store.rows(self.result_writer_key)
        active = [w for w in writers if w.get("status") == "已生效"]
        return active[0] if active else (writers[0] if writers else {})

    def _should_inject_failure(self, node: str, attempt: int) -> bool:
        planned = self._fault_plan.get(node, 0)
        return attempt <= int(planned)

    # ------------------------------------------------------------------
    # 结果记录
    # ------------------------------------------------------------------
    def _record_result(self, node: str, result: dict[str, Any]) -> None:
        with self._lock:
            self.execution_log.append(result)
            if result.get("status") == "completed":
                self.tasks_completed += 1
            else:
                self.tasks_failed += 1

    def _log_failure(self, node: str, reason: str) -> None:
        self._record_result(
            node,
            {"node": node, "kind": "branch", "status": "blocked", "reason": reason},
        )

    def _write_audit(self) -> None:
        """把批量执行结果写入审计记录（按分支逐条记录）。"""
        audit_rows = store.rows(self.output_audit_key)
        for log in self.execution_log:
            row_count = log.get("row_count")
            if row_count is None:
                row_count = log.get("branch_counts", {}).get(log.get("node", ""), 0)
            audit_rows.append({
                "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
                "管道名称": log.get("node", ""),
                "输出目标": log.get("target_table", log.get("output_target", "")),
                "输出时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "输出行数": str(row_count),
                "校验结果": "校验通过" if log.get("status") == "completed" else "校验失败",
                "status": "已记录" if log.get("status") == "completed" else "校验失败",
                "pending": False,
                "abnormal": log.get("status") != "completed",
                "run_id": self.run_id,
                "目标分区": log.get("target_partition", ""),
                "节点类型": log.get("kind", "branch"),
                "回执": log.get("receipt_id", ""),
            })

    def _track_lineage(self, dag: dict[str, Any]) -> None:
        """按真实依赖边记录 DAG 执行的计算血缘。"""
        lineage_rows = store.rows(self.lineage_key)
        plan = self._parse_dag(dag)
        for node in plan["nodes"]:
            upstream = plan["parents"][node] or ["原始源"]
            downstream = plan["children"][node] or ["最终目标"]
            lineage_rows.append({
                "id": max((int(r.get("id", 0)) for r in lineage_rows), default=0) + 1,
                "输入表": ",".join(upstream),
                "转换管道": node,
                "输出表": ",".join(downstream),
                "血缘类型": "DAG编排",
                "记录时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "已建立",
                "pending": False,
                "abnormal": False,
                "run_id": self.run_id,
            })

    # ------------------------------------------------------------------
    # 回填
    # ------------------------------------------------------------------
    def run_backfill(self, *, time_range: str | None = None) -> dict[str, Any]:
        """执行历史回填任务，支持断点续做。"""
        tasks = store.rows(self.backfill_key)
        active = [t for t in tasks if t.get("status") in ("待调度", "回填失败")]
        if not active:
            return self._empty("无待回填任务")
        task = active[0]
        task["status"] = "回填中"
        task["执行状态"] = f"在{time.strftime('%H:%M:%S')}恢复回填"
        task["回填时间范围"] = time_range or task.get("回填时间范围", "")
        task["处理进度"] = f"已完成{self.tasks_completed + 1}个批次"
        return {
            "backfill": True,
            "task_name": task.get("任务名称", ""),
            "time_range": task.get("回填时间范围", ""),
            "progress": task.get("处理进度", ""),
            "resumed_from": task.get("执行状态", ""),
        }

    def _empty(self, reason: str) -> dict[str, Any]:
        return {
            "run_id": self.run_id or "none",
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_tasks": 0,
            "execution_log": [],
            "branches": [],
            "joins": [],
            "receipts": [],
            "reason": reason,
        }


def deterministic_seed(name: str) -> int:
    """分支结果的确定性种子，保证重试与原尝试算出的行集合一致。"""
    total = 0
    for char in name:
        total = (total * 131 + ord(char)) % 1_000_003
    return total
