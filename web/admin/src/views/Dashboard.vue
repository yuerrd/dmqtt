<template>
  <div>
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="6" v-for="card in statCards" :key="card.label">
        <el-card shadow="hover">
          <div style="text-align: center">
            <div style="font-size: 32px; font-weight: bold; color: #409eff">{{ card.value }}</div>
            <div style="color: #909399; margin-top: 8px">{{ card.label }}</div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- Per-node stats breakdown -->
    <el-card v-if="clusterNodes.length > 1" style="margin-bottom: 20px">
      <template #header>{{ t('dashboard.nodeOverview') }}</template>
      <el-table :data="clusterNodes" size="small" stripe>
        <el-table-column prop="node_id" :label="t('common.node')">
          <template #default="{ row }">
            <el-tag size="small" :type="row.node_id === selfNodeId ? 'success' : 'info'">{{ row.node_id }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="connections" :label="t('dashboard.connections')" />
        <el-table-column prop="subscriptions" :label="t('dashboard.subscriptions')" />
        <el-table-column prop="retained" :label="t('dashboard.retained')" />
      </el-table>
    </el-card>

    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="12">
        <el-card :header="t('dashboard.connectionsTrend')">
          <v-chart :option="connectionsChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card :header="t('dashboard.memoryTrend')">
          <v-chart :option="memoryChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16">
      <el-col :span="12">
        <el-card :header="t('dashboard.goroutineTrend')">
          <v-chart :option="goroutineChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card :header="t('dashboard.subscriptionsTrend')">
          <v-chart :option="subsChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
    </el-row>
    <div v-if="error" style="margin-top: 16px">
      <el-alert :title="error" type="error" show-icon />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent } from 'echarts/components'
import { fetchStats, fetchClusterStats, type Stats, type ClusterNodeStats } from '../api/stats'
import { usePolling } from '../composables/usePolling'
import { useTimeSeriesBuffer } from '../composables/useTimeSeriesBuffer'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, TitleComponent])

const { t } = useI18n()
const { data: stats, error } = usePolling(fetchStats, 5000)
const { points, push } = useTimeSeriesBuffer(60)

const clusterNodes = ref<ClusterNodeStats[]>([])
const selfNodeId = ref('')
const totalConnections = ref(0)
const totalSubscriptions = ref(0)

// Fetch cluster-wide stats
async function loadClusterStats() {
  try {
    const cs = await fetchClusterStats()
    clusterNodes.value = cs.nodes || []
    selfNodeId.value = cs.self
    totalConnections.value = cs.total_connections
    totalSubscriptions.value = cs.total_subscriptions
  } catch { /* ignore */ }
}

loadClusterStats()

watch(stats, (val) => {
  if (!val) return
  loadClusterStats()
  push({
    time: new Date().toLocaleTimeString(),
    connected_clients: totalConnections.value || val.connected_clients,
    total_subscriptions: totalSubscriptions.value || val.active_subscriptions,
    memory_alloc_mb: Math.round(val.memory_alloc_bytes / 1024 / 1024),
    goroutines: val.goroutines,
  })
})

const statCards = computed(() => {
  const s = stats.value
  if (!s) return []
  return [
    { label: t('dashboard.onlineDevices'), value: totalConnections.value || s.connected_clients },
    { label: t('dashboard.activeSubscriptions'), value: totalSubscriptions.value || s.active_subscriptions },
    { label: t('dashboard.retainedMessages'), value: s.retained_messages },
    { label: t('dashboard.clusterNodes'), value: clusterNodes.value.length || s.cluster_nodes },
  ]
})

function makeLineOption(labelFn: () => string, field: string, unit = '') {
  return computed(() => ({
    animation: false,
    tooltip: { trigger: 'axis' as const },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category' as const, data: points.value.map((p) => p.time), show: false },
    yAxis: { type: 'value' as const, name: unit },
    series: [
      {
        name: labelFn(),
        type: 'line' as const,
        data: points.value.map((p) => p[field]),
        smooth: true,
        areaStyle: { opacity: 0.15 },
        showSymbol: false,
      },
    ],
  }))
}

const connectionsChartOption = makeLineOption(() => t('dashboard.connections'), 'connected_clients')
const memoryChartOption = makeLineOption(() => t('dashboard.memory'), 'memory_alloc_mb', 'MB')
const goroutineChartOption = makeLineOption(() => 'Goroutines', 'goroutines')
const subsChartOption = makeLineOption(() => t('dashboard.subscriptions'), 'total_subscriptions')
</script>