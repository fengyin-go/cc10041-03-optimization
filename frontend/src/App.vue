<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">流批一体数据处理中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">统一管理流式加工和批量计算的数据处理平台，覆盖数据清洗、转换、关联和结果落地</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "流式数据源", path: "/stream_source" }, { label: "转换规则", path: "/transform_rule" }, { label: "批量计算管道", path: "/batch_pipeline" }, { label: "数据关联合并", path: "/join_config" }, { label: "检查点管理", path: "/checkpoint_mgr" }, { label: "异常处理策略", path: "/error_handler" }, { label: "结果写入器", path: "/result_writer" }, { label: "迟到数据处理", path: "/late_data" }, { label: "窗口配置", path: "/window_config" }, { label: "聚合计算规则", path: "/aggregation_rule" }, { label: "数据清洗链", path: "/data_cleanse" }, { label: "管道监控", path: "/pipeline_monitor" }, { label: "历史回填", path: "/backfill_handler" }, { label: "输出审计", path: "/output_audit" }, { label: "Schema注册", path: "/schema_registry" }, { label: "数据分区", path: "/data_partition" }, { label: "管道编排DAG", path: "/pipeline_dag" }, { label: "Iceberg表管理", path: "/iceberg_table" }, { label: "数据采样", path: "/data_sample" }, { label: "计算血缘追踪", path: "/lineage_tracker" }]
</script>
