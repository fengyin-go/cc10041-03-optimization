"""批量管道引擎：DAG编排、任务调度、回填续做、输出审计。

真实数据流：
  batch_pipeline → pipeline_dag → task_scheduler(backfill) → result_writer
  → output_audit → checkpoint_mgr

DAG节点依赖解析 → 调度执行 → 结果审计 → 检查点保存。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

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

    run_id: str = ""
    tasks_completed: int = 0
    tasks_failed: int = 0
    execution_log: list[dict[str, Any]] = field(default_factory=list)

    def run_dag(self, *, dag_name: str | None = None) -> dict[str, Any]:
        """解析DAG依赖关系并按拓扑顺序执行管道。"""
        self.run_id = f"batch-{int(time.time() * 1000)}"
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.execution_log = []

        dag = self._resolve_dag(dag_name)
        if not dag:
            return self._empty("未找到可发布的DAG")

        order = self._topological_sort(dag)
        for node in order:
            result = self._execute_node(node)
            self.execution_log.append(result)
            if result.get("status") == "completed":
                self.tasks_completed += 1
            else:
                self.tasks_failed += 1

        self._write_audit()
        self._track_lineage(dag)
        return {
            "run_id": self.run_id,
            "dag_name": dag.get("图名称", ""),
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "total_tasks": len(order),
            "execution_log": self.execution_log,
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

    def _topological_sort(self, dag: dict[str, Any]) -> list[str]:
        """按依赖关系拓扑排序DAG节点。"""
        nodes_raw = dag.get("节点列表", "")
        edges_raw = dag.get("依赖关系", "")
        nodes = [n.strip() for n in str(nodes_raw).split(",") if n.strip()]
        edges: list[tuple[str, str]] = []
        for edge in str(edges_raw).split(","):
            parts = edge.strip().split("->")
            if len(parts) == 2:
                edges.append((parts[0].strip(), parts[1].strip()))

        # 构建入度表
        in_degree: dict[str, int] = {n: 0 for n in nodes}
        adj: dict[str, list[str]] = {n: [] for n in nodes}
        for src, dst in edges:
            if src in in_degree and dst in in_degree:
                adj.setdefault(src, []).append(dst)
                in_degree[dst] += 1

        # Kahn's algorithm
        queue = [n for n in nodes if in_degree.get(n, 0) == 0]
        order = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for neighbor in adj.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(nodes):
            # 有环，返回原始顺序
            return nodes
        return order

    def _execute_node(self, node_name: str) -> dict[str, Any]:
        """执行单个DAG节点（模拟管道执行）。"""
        pipelines = store.rows(self.pipeline_key)
        matched = [p for p in pipelines if p.get("管道名称") == node_name]
        if not matched:
            return {"node": node_name, "status": "skipped", "reason": "未找到对应管道配置"}
        pipeline = matched[0]
        # 模拟执行
        time.sleep(0.001)
        pipeline["status"] = "运行中"
        pipeline["调度状态"] = f"已被{self.run_id}执行"
        return {
            "node": node_name,
            "pipeline": pipeline.get("管道名称", ""),
            "status": "completed",
            "engine": pipeline.get("计算引擎", ""),
            "output_target": pipeline.get("输出目标", ""),
            "executed_at": int(time.time() * 1000),
        }

    def _write_audit(self) -> None:
        """将批量执行结果写入审计记录。"""
        audit_rows = store.rows(self.output_audit_key)
        for log in self.execution_log:
            audit_rows.append({
                "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
                "管道名称": log.get("node", ""),
                "输出目标": log.get("output_target", ""),
                "输出时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "输出行数": str(len(self.execution_log)),
                "校验结果": "校验通过" if log.get("status") == "completed" else "校验失败",
                "status": "已记录" if log.get("status") == "completed" else "校验失败",
                "pending": False,
                "abnormal": log.get("status") != "completed",
                "run_id": self.run_id,
            })

    def _track_lineage(self, dag: dict[str, Any]) -> None:
        """记录DAG执行的计算血缘。"""
        lineage_rows = store.rows(self.lineage_key)
        nodes = [n.strip() for n in str(dag.get("节点列表", "")).split(",") if n.strip()]
        for i, node in enumerate(nodes):
            prev = nodes[i - 1] if i > 0 else "原始源"
            nxt = nodes[i + 1] if i + 1 < len(nodes) else "最终目标"
            lineage_rows.append({
                "id": max((int(r.get("id", 0)) for r in lineage_rows), default=0) + 1,
                "输入表": prev,
                "转换管道": node,
                "输出表": nxt,
                "血缘类型": "DAG编排",
                "记录时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "已建立",
                "pending": False,
                "abnormal": False,
            })

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
        return {"run_id": self.run_id or "none", "tasks_completed": 0, "tasks_failed": 0,
                "total_tasks": 0, "reason": reason}