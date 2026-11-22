"""批量 DAG 并行分支与回执驱动汇合的行为测试。

覆盖：
  * 三条可同时运行的管道并行启动，两条写同一目标、一条写另一张表；
  * 共享目标的一条分支首次写入失败后重试，第三条仍可产出；
  * 同目标两条分支都参与汇合，逐条核对参与量；
  * 重试与原尝试并发到达只确认一份分支结果；
  * 汇合不读取另一条分支尚未写完的分区数据；
  * 有上游依赖的管道等输入齐备才启动。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_root))

from app.engine.batch_pipeline import BatchPipelineEngine
from app.engine.partition_store import reset_partition_store
from app.seed import SEED_ROWS
from app.store import store


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def reset_world() -> None:
    """重置业务数据与分区仓库，保证每个用例从干净状态开始。"""
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    reset_partition_store()


def _pipeline_row(
    name: str,
    *,
    target_table: str,
    target_partition: str = "dt=2026-11-22",
    input_source: str | None = None,
) -> dict:
    return {
        "id": 0,
        "status": "待启动",
        "pending": True,
        "abnormal": False,
        "管道名称": name,
        "输入源": input_source or f"{name}-输入",
        "输出目标": target_table,
        "目标分区": target_partition,
        "计算引擎": "Spark",
        "调度状态": "待调度",
    }


def install_batch(
    pipelines: list[dict],
    *,
    dag_name: str = "并行汇合DAG",
    nodes: list[str],
    edges: list[str],
) -> None:
    """写入三条管道与一张已发布 DAG，替换对应模块的样例数据。"""
    rows = []
    for index, pipeline in enumerate(pipelines, start=1):
        row = dict(pipeline)
        row["id"] = index
        rows.append(row)
    store._tables["batch_pipeline"] = rows
    store._tables["pipeline_dag"] = [{
        "id": 1,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "图名称": dag_name,
        "节点列表": ",".join(nodes),
        "依赖关系": ",".join(edges),
        "触发方式": "手动",
        "调度状态": "待调度",
    }]
    # 结果写入器需要一个已生效配置；沿用样例中的第 2 条。
    for writer in store.rows("result_writer"):
        writer["status"] = "已生效" if writer.get("id") == 2 else "已停用"


def three_branch_batch(*, join_target: str = "汇总宽表") -> None:
    """A、B 写同一张表，C 写另一张表，A+B 汇合到汇总宽表。"""
    install_batch(
        [
            _pipeline_row("管道A", target_table="订单明细表"),
            _pipeline_row("管道B", target_table="订单明细表"),
            _pipeline_row("管道C", target_table="用户画像表"),
            _pipeline_row("汇合节点", target_table=join_target),
        ],
        nodes=["管道A", "管道B", "管道C", "汇合节点"],
        edges=["管道A->汇合节点", "管道B->汇合节点"],
    )


def branch_by_node(result: dict, node: str) -> dict:
    for branch in result["branches"]:
        if branch["node"] == node:
            return branch
    raise AssertionError(f"未找到分支 {node}，日志={result['branches']}")


# ---------------------------------------------------------------------------
# 主场景：三条并行，两条同目标，一条失败重试
# ---------------------------------------------------------------------------

def test_three_pipelines_run_in_parallel():
    """三条无依赖管道应并行启动，而不是按顺序串行。"""
    reset_world()
    three_branch_batch()

    result = BatchPipelineEngine().run_dag(fault_plan={})

    assert result["tasks_failed"] == 0, result["execution_log"]
    branches = {item["node"]: item for item in result["branches"]}
    assert set(branches) == {"管道A", "管道B", "管道C"}

    # 三条分支启动时间应彼此接近（并行），而不是逐个错开整个执行时长。
    starts = [branches[name]["started_at_ms"] for name in ("管道A", "管道B", "管道C")]
    assert max(starts) - min(starts) < 500, f"三条分支未并行启动: {starts}"

    # 至少两条分支的执行区间重叠。
    intervals = sorted(
        (branches[name]["started_at_ms"], branches[name]["finished_at_ms"])
        for name in ("管道A", "管道B", "管道C")
    )
    overlap = any(
        left[1] >= right[0] for left, right in zip(intervals, intervals[1:])
    )
    assert overlap, f"分支执行区间无重叠，仍是串行: {intervals}"


def test_shared_target_failure_retry_keeps_other_branches():
    """共享目标的一条写入失败后重试，第三条仍产出，两条同目标都参与汇合。"""
    reset_world()
    three_branch_batch()

    result = BatchPipelineEngine().run_dag(
        run_id="run-retry",
        fault_plan={"管道A": 1},
    )

    branch_a = branch_by_node(result, "管道A")
    branch_b = branch_by_node(result, "管道B")
    branch_c = branch_by_node(result, "管道C")

    # A 第一次失败、第二次重试成功。
    assert branch_a["status"] == "completed", branch_a
    assert [item["outcome"] for item in branch_a["attempts"]] == ["failed", "accepted"]
    assert branch_a["receipt_accepted"] is True

    # B、C 不受影响，照常产出。
    assert branch_b["status"] == "completed" and branch_b["row_count"] > 0
    assert branch_c["status"] == "completed" and branch_c["row_count"] > 0

    # 同目标 A、B 只落一份回执/结果。
    receipts = result["receipts"]
    a_receipts = [item for item in receipts if item["branch"] == "管道A"]
    assert len(a_receipts) == 1, a_receipts

    # 汇合节点完成，且两路都参与。
    joins = [item for item in result["joins"] if item["node"] == "汇合节点"]
    assert len(joins) == 1, result["joins"]
    joined = joins[0]
    assert joined["status"] == "completed", joined
    join_input = joined["inputs"][0]
    assert join_input["target_table"] == "订单明细表"
    assert set(join_input["committed_branches"]) == {"管道A", "管道B"}
    assert join_input["branch_counts"]["管道A"] == branch_a["row_count"]
    assert join_input["branch_counts"]["管道B"] == branch_b["row_count"]
    assert joined["row_count"] == branch_a["row_count"] + branch_b["row_count"]

    # C 写的是另一张表，不进入这次汇合。
    assert "管道C" not in joined["branch_counts"]
    c_receipts = [item for item in receipts if item["branch"] == "管道C"]
    assert len(c_receipts) == 1
    assert c_receipts[0]["target_table"] == "用户画像表"


def test_join_waits_for_complete_partition_not_partial_rows():
    """汇合只读完整分区：缺一条分支时行数据为空，绝不给半成品。"""
    reset_world()
    three_branch_batch()
    engine = BatchPipelineEngine()
    # 用一次全新的运行标识模拟「B 还没写完」的中间态。
    run_id = "run-partial"

    # 先让 A 提交，B 尚未提交时读取共享分区。
    from app.engine.result_writer import BranchResultWriter

    writer = BranchResultWriter(engine.coordinator)
    writer.deliver(
        run_id=run_id,
        branch="管道A",
        pipeline="管道A",
        target_table="订单明细表",
        target_partition="dt=2026-11-22",
        rows=[{"branch": "管道A"}],
        attempt=1,
    )
    partial = engine.coordinator.read_partition(
        "订单明细表",
        "dt=2026-11-22",
        run_id=run_id,
        expected_branches={"管道A", "管道B"},
    )
    assert partial["complete"] is False
    assert partial["rows"] == [], "缺分支时不应返回任何行"
    assert partial["missing_branches"] == ["管道B"]
    assert partial["branch_counts"] == {}

    # 超时等待也不给半成品。
    timed = engine.coordinator.read_partition(
        "订单明细表",
        "dt=2026-11-22",
        run_id=run_id,
        expected_branches={"管道A", "管道B"},
        wait=True,
        timeout=0.05,
    )
    assert timed["complete"] is False and timed["timed_out"] is True
    assert timed["rows"] == []


def test_upstream_dependency_gates_downstream_start():
    """有上游依赖的管道必须等上游完成（输入可用）后才启动。"""
    reset_world()
    install_batch(
        [
            _pipeline_row("上游管道", target_table="源表"),
            _pipeline_row("下游管道", target_table="派生表"),
        ],
        dag_name="依赖链DAG",
        nodes=["上游管道", "下游管道"],
        edges=["上游管道->下游管道"],
    )

    result = BatchPipelineEngine().run_dag()
    assert result["tasks_failed"] == 0, result["execution_log"]

    upstream = branch_by_node(result, "上游管道")
    downstream = branch_by_node(result, "下游管道")
    assert downstream["started_at_ms"] >= upstream["finished_at_ms"], (
        "下游管道在上游完成前启动",
        upstream,
        downstream,
    )
    # 上游回执在下游启动前已可查。
    assert upstream["receipt_id"]
    receipt = BatchPipelineEngine().coordinator.receipt(upstream["receipt_id"])
    assert receipt is not None and receipt["state"] == "committed"


# ---------------------------------------------------------------------------
# 回执与幂等
# ---------------------------------------------------------------------------

def test_concurrent_retry_confirms_single_branch_result():
    """重试与原尝试并发到达时只确认一份分支结果。"""
    reset_world()
    from app.engine.result_writer import BranchResultWriter

    writer = BranchResultWriter()
    rows = [{"branch": "管道A", "row_index": index} for index in range(5)]
    outcomes: list[tuple[str, dict]] = []
    barrier = threading.Barrier(8)

    def attempt(index: int) -> None:
        barrier.wait()
        receipt, accepted = writer.deliver(
            run_id="run-race",
            branch="管道A",
            pipeline="管道A",
            target_table="订单明细表",
            target_partition="dt=2026-11-22",
            rows=rows,
            attempt=index + 1,
        )
        outcomes.append((receipt["receipt_id"], {"accepted": accepted}))

    threads = [threading.Thread(target=attempt, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    accepted = [item for item in outcomes if item[1]["accepted"]]
    assert len(accepted) == 1, f"应只确认一份结果，实际 {len(accepted)}"
    assert len({receipt_id for receipt_id, _ in outcomes}) == 1, "回执必须唯一"

    snapshot = writer.coordinator.read_partition(
        "订单明细表",
        "dt=2026-11-22",
        run_id="run-race",
        expected_branches={"管道A"},
    )
    assert snapshot["complete"] is True
    assert snapshot["row_count"] == len(rows), "重复提交不应叠加行数"
    assert len(snapshot["receipts"]) == 1


def test_receipts_and_attempts_are_inspectable_per_run():
    """一次运行的分支回执与重试尝试都可逐条核对。"""
    reset_world()
    three_branch_batch()
    result = BatchPipelineEngine().run_dag(
        run_id="run-audit", fault_plan={"管道B": 1}
    )

    receipts = result["receipts"]
    assert {item["branch"] for item in receipts} == {"管道A", "管道B", "管道C", "汇合节点"}

    audit_rows = [row for row in store.rows("output_audit") if row.get("run_id") == "run-audit"]
    assert audit_rows, "应产生本轮审计记录"
    for row in audit_rows:
        assert "目标分区" in row and "节点类型" in row

    branch_b = branch_by_node(result, "管道B")
    assert branch_b["attempts"][0]["outcome"] == "failed"
    assert branch_b["attempts"][-1]["outcome"] == "accepted"


def test_cycle_dag_does_not_schedule_anything():
    """有环 DAG 不调度任何节点，避免死锁。"""
    reset_world()
    install_batch(
        [
            _pipeline_row("节点一", target_table="表一"),
            _pipeline_row("节点二", target_table="表二"),
        ],
        dag_name="环DAG",
        nodes=["节点一", "节点二"],
        edges=["节点一->节点二", "节点二->节点一"],
    )

    result = BatchPipelineEngine().run_dag()
    assert result["tasks_completed"] == 0
    assert result["tasks_failed"] == 2
    assert all(item["status"] == "blocked" for item in result["execution_log"])


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
