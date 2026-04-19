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
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="12">
        <el-card header="连接数趋势">
          <v-chart :option="connectionsChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card header="内存使用趋势">
          <v-chart :option="memoryChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16">
      <el-col :span="12">
        <el-card header="Goroutine 趋势">
          <v-chart :option="goroutineChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card header="CPU 使用率">
          <v-chart :option="cpuChartOption" style="height: 250px" autoresize />
        </el-card>
      </el-col>
    </el-row>
    <div v-if="error" style="margin-top: 16px">
      <el-alert :title="error" type="error" show-icon />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, TitleComponent } from 'echarts/components'
import { fetchStats, type Stats } from '../api/stats'
import { usePolling } from '../composables/usePolling'
import { useTimeSeriesBuffer } from '../composables/useTimeSeriesBuffer'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, TitleComponent])

const { data: stats, error } = usePolling(fetchStats, 5000)
const { points, push } = useTimeSeriesBuffer(60)

watch(stats, (val) => {
  if (!val) return
  push({
    time: new Date().toLocaleTimeString(),
    connected_clients: val.connected_clients,
    memory_alloc_mb: Math.round(val.memory_alloc_bytes / 1024 / 1024),
    goroutines: val.goroutines,
    uptime_seconds: val.uptime_seconds,
  })
})

const statCards = computed(() => {
  const s = stats.value
  if (!s) return []
  return [
    { label: '在线设备', value: s.connected_clients },
    { label: '活跃订阅', value: s.active_subscriptions },
    { label: '保留消息', value: s.retained_messages },
    { label: '集群节点', value: s.cluster_nodes },
  ]
})

function makeLineOption(label: string, field: string, unit = '') {
  return computed(() => ({
    animation: false,
    tooltip: { trigger: 'axis' as const },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category' as const, data: points.value.map((p) => p.time), show: false },
    yAxis: { type: 'value' as const, name: unit },
    series: [
      {
        name: label,
        type: 'line' as const,
        data: points.value.map((p) => p[field]),
        smooth: true,
        areaStyle: { opacity: 0.15 },
        showSymbol: false,
      },
    ],
  }))
}

const connectionsChartOption = makeLineOption('连接数', 'connected_clients')
const memoryChartOption = makeLineOption('内存', 'memory_alloc_mb', 'MB')
const goroutineChartOption = makeLineOption('Goroutines', 'goroutines')
const cpuChartOption = makeLineOption('CPU', 'uptime_seconds', 's')
</script>