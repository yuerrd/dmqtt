<template>
  <div>
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="8">
        <el-card>
          <div style="text-align: center">
            <div style="font-size: 28px; font-weight: bold; color: #67c23a">{{ uptimeDisplay }}</div>
            <div style="color: #909399; margin-top: 4px">运行时间</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card>
          <div style="text-align: center">
            <div style="font-size: 28px; font-weight: bold; color: #409eff">{{ latestGoroutines }}</div>
            <div style="color: #909399; margin-top: 4px">Goroutines</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card>
          <div style="text-align: center">
            <div style="font-size: 28px; font-weight: bold; color: #e6a23c">{{ latestGcPause }}</div>
            <div style="color: #909399; margin-top: 4px">GC 总暂停</div>
          </div>
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="12">
        <el-card header="内存分配 (Alloc)">
          <v-chart :option="allocChartOption" style="height: 280px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card header="内存总量 (Sys)">
          <v-chart :option="sysChartOption" style="height: 280px" autoresize />
        </el-card>
      </el-col>
    </el-row>
    <el-row :gutter="16">
      <el-col :span="12">
        <el-card header="Goroutine 数量">
          <v-chart :option="goroutineChartOption" style="height: 280px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card header="GC 暂停时间">
          <v-chart :option="gcChartOption" style="height: 280px" autoresize />
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
import { GridComponent, TooltipComponent } from 'echarts/components'
import { fetchStats } from '../api/stats'
import { usePolling } from '../composables/usePolling'
import { useTimeSeriesBuffer } from '../composables/useTimeSeriesBuffer'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent])

const { data: stats, error } = usePolling(fetchStats, 5000)
const { points, push } = useTimeSeriesBuffer(60)

watch(stats, (val) => {
  if (!val) return
  push({
    time: new Date().toLocaleTimeString(),
    alloc_mb: Math.round(val.memory_alloc_bytes / 1024 / 1024 * 10) / 10,
    sys_mb: Math.round(val.memory_sys_bytes / 1024 / 1024 * 10) / 10,
    goroutines: val.goroutines,
    gc_pause_ms: Math.round(val.gc_pause_total_ns / 1e6 * 100) / 100,
  })
})

const uptimeDisplay = computed(() => {
  if (!stats.value) return '-'
  const s = stats.value.uptime_seconds
  const d = Math.floor(s / 86400)
  const h = Math.floor((s % 86400) / 3600)
  const m = Math.floor((s % 3600) / 60)
  if (d > 0) return `${d}d ${h}h ${m}m`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m ${s % 60}s`
})

const latestGoroutines = computed(() => stats.value?.goroutines ?? '-')
const latestGcPause = computed(() => {
  if (!stats.value) return '-'
  return `${(stats.value.gc_pause_total_ns / 1e6).toFixed(1)}ms`
})

function makeAreaOption(label: string, field: string, unit: string, color: string) {
  return computed(() => ({
    animation: false,
    tooltip: { trigger: 'axis' as const },
    grid: { left: 60, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category' as const, data: points.value.map((p) => p.time), show: false },
    yAxis: { type: 'value' as const, name: unit },
    series: [
      {
        name: label,
        type: 'line' as const,
        data: points.value.map((p) => p[field]),
        smooth: true,
        showSymbol: false,
        areaStyle: { opacity: 0.2, color },
        lineStyle: { color },
        itemStyle: { color },
      },
    ],
  }))
}

const allocChartOption = makeAreaOption('Alloc', 'alloc_mb', 'MB', '#409eff')
const sysChartOption = makeAreaOption('Sys', 'sys_mb', 'MB', '#67c23a')
const goroutineChartOption = makeAreaOption('Goroutines', 'goroutines', '', '#e6a23c')
const gcChartOption = makeAreaOption('GC Pause', 'gc_pause_ms', 'ms', '#f56c6c')
</script>