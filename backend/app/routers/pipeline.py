"""流批一体管道API：流式处理、批量编排、检查点恢复、Schema兼容、快照管理。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.engine import StreamPipeline, BatchPipelineEngine, PipelineMonitorEngine, DataQualityEngine
from app.engine.partition_store import partition_coordinator

router = APIRouter(prefix="/api/pipeline", tags=["数据处理管道"])


@router.post("/stream/run")
def run_stream_pipeline(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """执行一次流处理管线：消费→清洗→窗口→聚合→迟到→写入→检查点。"""
    pipeline = StreamPipeline()
    return pipeline.run(batch_id=batch_id)


@router.get("/stream/checkpoint/latest")
def get_latest_checkpoint() -> dict[str, Any]:
    """获取最新检查点状态。"""
    pipeline = StreamPipeline()
    return pipeline.recover_from_checkpoint()


@router.post("/batch/dag/run")
def run_batch_dag(
    dag_name: str | None = Query(default=None),
    run_id: str | None = Query(default=None),
    fail_branch: str | None = Query(default=None),
    fail_attempts: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """执行批量DAG编排管道：独立分支并行，汇合节点按写入回执读取完整分区。"""
    engine = BatchPipelineEngine()
    fault_plan = {fail_branch: fail_attempts} if fail_branch and fail_attempts else None
    return engine.run_dag(dag_name=dag_name, run_id=run_id, fault_plan=fault_plan)


@router.get("/batch/partition")
def read_batch_partition(
    target_table: str = Query(...),
    target_partition: str = Query(default="默认分区"),
    run_id: str = Query(...),
    expected_branches: str | None = Query(default=None),
) -> dict[str, Any]:
    """按写入回执读取一个分区；未齐备时 complete=false 且不返回行数据。"""
    expected = (
        [item.strip() for item in expected_branches.split(",") if item.strip()]
        if expected_branches else None
    )
    return partition_coordinator.read_partition(
        target_table, target_partition, run_id=run_id, expected_branches=expected,
    )


@router.get("/batch/receipts")
def list_batch_receipts(run_id: str = Query(...)) -> dict[str, Any]:
    """列出一个运行的全部分支写入回执与并发尝试记录。"""
    return {
        "run_id": run_id,
        "receipts": partition_coordinator.receipts_for_run(run_id),
        "attempts": partition_coordinator.attempt_history(run_id),
    }


@router.get("/batch/partitions/{target_table}")
def list_batch_partition_branches(
    target_table: str,
    run_id: str = Query(...),
    target_partition: str = Query(default="默认分区"),
) -> dict[str, Any]:
    """按分支逐条查看某目标分区在本次运行的参与量与完整性。"""
    return partition_coordinator.partition_summary(
        target_table, target_partition, run_id=run_id
    )


@router.post("/batch/backfill")
def run_backfill(time_range: str | None = Query(default=None)) -> dict[str, Any]:
    """执行历史回填任务。"""
    engine = BatchPipelineEngine()
    return engine.run_backfill(time_range=time_range)


@router.get("/monitor/metrics")
def collect_pipeline_metrics() -> dict[str, Any]:
    """采集管道运行时指标。"""
    engine = PipelineMonitorEngine()
    return engine.collect_metrics()


@router.post("/quality/schema/check")
def check_schema_compatibility(
    schema_name: str = Query(...),
    new_fields: str = Query(...),
) -> dict[str, Any]:
    """检查Schema兼容性。"""
    engine = DataQualityEngine()
    fields = [f.strip() for f in new_fields.split(",") if f.strip()]
    return engine.check_schema_compatibility(schema_name=schema_name, new_fields=fields)


@router.post("/quality/iceberg/snapshot")
def create_iceberg_snapshot(table_name: str = Query(...)) -> dict[str, Any]:
    """为Iceberg表创建快照。"""
    engine = DataQualityEngine()
    result = engine.create_snapshot(table_name)
    if not result.get("snapshot_created"):
        raise HTTPException(status_code=404, detail=result.get("reason", "未知错误"))
    return result


@router.post("/quality/sample")
def execute_sampling(
    target_table: str = Query(...),
    sample_ratio: float = Query(default=0.1, ge=0.0, le=1.0),
) -> dict[str, Any]:
    """执行数据采样。"""
    engine = DataQualityEngine()
    return engine.execute_sampling(target_table=target_table, sample_ratio=sample_ratio)


@router.get("/quality/health")
def check_health(
    records_processed: int = Query(default=100),
    records_failed: int = Query(default=0),
) -> dict[str, Any]:
    """检查管道健康状态。"""
    engine = PipelineMonitorEngine()
    return engine.check_pipeline_health(records_processed, records_failed)