"""数据质量引擎：Schema兼容性检查、Iceberg快照校验、数据采样验证。"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from app.store import store


@dataclass
class DataQualityEngine:
    schema_key: str = "schema_registry"
    iceberg_key: str = "iceberg_table"
    sample_key: str = "data_sample"
    partition_key: str = "data_partition"

    def check_schema_compatibility(self, *, schema_name: str, new_fields: list[str]) -> dict[str, Any]:
        """检查新增字段与已注册Schema的兼容性。"""
        schemas = store.rows(self.schema_key)
        registered = [s for s in schemas if s.get("status") == "已注册" and s.get("Schema名称") == schema_name]
        if not registered:
            return {"compatible": True, "reason": "首次注册，无需兼容性检查", "schema_name": schema_name, "new_fields": new_fields}
        existing = registered[0]
        compat_type = existing.get("兼容类型", "backward")
        existing_fields = str(existing.get("字段列表", "")).split(",")

        result = {
            "schema_name": schema_name,
            "existing_version": existing.get("版本号", ""),
            "existing_fields": [f.strip() for f in existing_fields if f.strip()],
            "new_fields": new_fields,
            "compatibility_type": compat_type,
            "checked_at": int(time.time() * 1000),
        }

        if compat_type == "backward":
            # 向后兼容：新字段不能删除已有字段
            removed = [f for f in result["existing_fields"] if f not in new_fields]
            result["compatible"] = len(removed) == 0
            result["removed_fields"] = removed
            result["added_fields"] = [f for f in new_fields if f not in result["existing_fields"]]
            if not result["compatible"]:
                result["reason"] = f"向后兼容失败：删除了已有字段 {removed}"
            else:
                result["reason"] = "向后兼容检查通过"
        elif compat_type == "forward":
            # 向前兼容：新字段可以新增但不能删除已有字段
            removed = [f for f in result["existing_fields"] if f not in new_fields]
            result["compatible"] = len(removed) == 0
            result["removed_fields"] = removed
            result["added_fields"] = [f for f in new_fields if f not in result["existing_fields"]]
            if not result["compatible"]:
                result["reason"] = f"向前兼容失败：删除了已有字段 {removed}"
            else:
                result["reason"] = "向前兼容检查通过"
        else:
            result["compatible"] = True
            result["reason"] = f"兼容类型 {compat_type} 不做限制"
            result["removed_fields"] = [f for f in result["existing_fields"] if f not in new_fields]
            result["added_fields"] = [f for f in new_fields if f not in result["existing_fields"]]
        return result

    def create_snapshot(self, table_name: str) -> dict[str, Any]:
        """为Iceberg表创建快照。"""
        tables = store.rows(self.iceberg_key)
        matched = [t for t in tables if t.get("表名") == table_name and t.get("status") == "已注册"]
        if not matched:
            return {"snapshot_created": False, "reason": f"表 {table_name} 未注册"}
        tbl = matched[0]
        snapshots_field = tbl.get("当前快照数", "0")
        try:
            current_count = int("".join(c for c in str(snapshots_field) if c.isdigit()) or "0")
        except (ValueError, TypeError):
            current_count = 0
        new_count = current_count + 1
        tbl["当前快照数"] = str(new_count)
        return {
            "snapshot_created": True,
            "table_name": table_name,
            "snapshot_id": f"s{new_count}-{int(time.time() * 1000)}",
            "snapshot_count": new_count,
            "created_at": int(time.time() * 1000),
            "storage_format": tbl.get("存储格式", ""),
            "partition_definition": tbl.get("分区定义", ""),
        }

    def execute_sampling(self, *, target_table: str, sample_ratio: float = 0.1) -> dict[str, Any]:
        """执行数据采样。"""
        configs = store.rows(self.sample_key)
        active = [c for c in configs if c.get("status") == "待执行"]
        if not active:
            return {"sampled": False, "reason": "无待执行的采样配置"}
        cfg = active[0]
        cfg["status"] = "已执行"
        cfg["采样状态"] = f"已采样{target_table}"
        partitions = store.rows(self.partition_key)
        active_part = [p for p in partitions if p.get("status") == "已启用"]
        return {
            "sampled": True,
            "config_name": cfg.get("配置名称", ""),
            "target_table": target_table,
            "sample_ratio": sample_ratio,
            "sample_method": cfg.get("采样方式", ""),
            "active_partitions": len(active_part),
            "sampled_at": int(time.time() * 1000),
        }