"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.stream_source import StreamSourceService
from app.services.transform_rule import TransformRuleService
from app.services.batch_pipeline import BatchPipelineService
from app.services.join_config import JoinConfigService
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.error_handler import ErrorHandlerService
from app.services.result_writer import ResultWriterService
from app.services.late_data import LateDataService
from app.services.window_config import WindowConfigService
from app.services.aggregation_rule import AggregationRuleService
from app.services.data_cleanse import DataCleanseService
from app.services.pipeline_monitor import PipelineMonitorService
from app.services.backfill_handler import BackfillHandlerService
from app.services.output_audit import OutputAuditService
from app.services.schema_registry import SchemaRegistryService
from app.services.data_partition import DataPartitionService
from app.services.pipeline_dag import PipelineDagService
from app.services.iceberg_table import IcebergTableService
from app.services.data_sample import DataSampleService
from app.services.lineage_tracker import LineageTrackerService

SERVICES: dict[str, BaseModuleService] = {
    "stream_source": StreamSourceService(),
    "transform_rule": TransformRuleService(),
    "batch_pipeline": BatchPipelineService(),
    "join_config": JoinConfigService(),
    "checkpoint_mgr": CheckpointMgrService(),
    "error_handler": ErrorHandlerService(),
    "result_writer": ResultWriterService(),
    "late_data": LateDataService(),
    "window_config": WindowConfigService(),
    "aggregation_rule": AggregationRuleService(),
    "data_cleanse": DataCleanseService(),
    "pipeline_monitor": PipelineMonitorService(),
    "backfill_handler": BackfillHandlerService(),
    "output_audit": OutputAuditService(),
    "schema_registry": SchemaRegistryService(),
    "data_partition": DataPartitionService(),
    "pipeline_dag": PipelineDagService(),
    "iceberg_table": IcebergTableService(),
    "data_sample": DataSampleService(),
    "lineage_tracker": LineageTrackerService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
