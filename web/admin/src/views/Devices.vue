<template>
  <div>
    <el-card>
      <template #header>
        <div style="display: flex; align-items: center; gap: 12px">
          <el-input
            v-model="searchText"
            :placeholder="t('devices.searchPlaceholder')"
            clearable
            style="width: 300px"
            @clear="loadDevices"
            @keyup.enter="loadDevices"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
          <el-button type="primary" @click="loadDevices">{{ t('common.search') }}</el-button>
          <el-button @click="handleRefresh">{{ t('common.refresh') }}</el-button>
        </div>
      </template>

      <el-table :data="filteredDevices" v-loading="loading" stripe style="width: 100%" @row-click="(row: DeviceSummary) => showDetail(row.client_id)">
        <el-table-column prop="client_id" :label="t('devices.deviceId')" min-width="150">
          <template #default="{ row }">
            <span style="color: #409eff; cursor: pointer">{{ row.client_id }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('devices.nodeLocation')" min-width="100" v-if="clusterMode">
          <template #default="{ row }">
            <el-tag size="small" :type="row.node_id === currentNodeId ? 'success' : 'info'">{{ row.node_id }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="t('devices.subscribedTopics')" min-width="200">
          <template #default="{ row }">
            <template v-if="row.subscriptions && row.subscriptions.length">
              <el-tag v-for="tp in row.subscriptions" :key="tp" size="small" style="margin: 2px" type="warning">{{ tp }}</el-tag>
            </template>
            <span v-else style="color: #999">{{ t('common.none') }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="remote_addr" :label="t('devices.ipAddress')" min-width="140" />
        <el-table-column :label="t('devices.protocolVersion')" width="100">
          <template #default="{ row }">
            {{ formatProtocol(row.protocol_version) }}
          </template>
        </el-table-column>
        <el-table-column :label="t('devices.connectTime')" min-width="160">
          <template #default="{ row }">
            {{ formatTime(row.connected_at) }}
          </template>
        </el-table-column>
        <el-table-column :label="t('common.operations')" width="100" fixed="right">
          <template #default="{ row }">
            <el-popconfirm
              :title="t('devices.confirmDisconnect')"
              :confirm-button-text="t('common.confirm')"
              :cancel-button-text="t('common.cancel')"
              @confirm="handleDisconnect(row.client_id)"
            >
              <template #reference>
                <el-button type="danger" size="small" text>{{ t('devices.disconnect') }}</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>

      <div style="margin-top: 12px; color: #999; font-size: 13px">
        {{ t('devices.totalDevices', { count: filteredDevices.length }) }}
      </div>
    </el-card>

    <el-drawer v-model="drawerVisible" :title="t('devices.deviceDetail', { id: selectedId })" size="400px">
      <div v-if="deviceDetail" v-loading="detailLoading">
        <el-descriptions :column="1" border>
          <el-descriptions-item :label="t('devices.deviceId')">{{ deviceDetail.client_id }}</el-descriptions-item>
          <el-descriptions-item :label="t('devices.username')">{{ deviceDetail.username }}</el-descriptions-item>
          <el-descriptions-item :label="t('devices.ipAddress')">{{ deviceDetail.remote_addr }}</el-descriptions-item>
          <el-descriptions-item :label="t('devices.protocolVersion')">{{ formatProtocol(deviceDetail.protocol_version) }}</el-descriptions-item>
          <el-descriptions-item :label="t('devices.connectTime')">{{ formatTime(deviceDetail.connected_at) }}</el-descriptions-item>
          <el-descriptions-item :label="t('devices.keepAlive')">{{ deviceDetail.keep_alive }}s</el-descriptions-item>
          <el-descriptions-item :label="t('common.status')">
            <el-tag :type="deviceDetail.connected ? 'success' : 'danger'">
              {{ deviceDetail.connected ? t('common.online') : t('common.offline') }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
        <div v-if="deviceDetail.session" style="margin-top: 16px">
          <h4>{{ t('devices.sessionInfo') }}</h4>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="Clean Start">{{ deviceDetail.session.clean_start }}</el-descriptions-item>
            <el-descriptions-item label="过期时间">{{ deviceDetail.session.expiry_interval }}s</el-descriptions-item>
          </el-descriptions>
          <h4 style="margin-top: 12px">{{ t('devices.subscription') }}</h4>
          <el-table :data="subscriptionList" size="small" style="margin-top: 8px">
            <el-table-column prop="topic" :label="t('devices.topic')" />
            <el-table-column prop="qos" label="QoS" width="60" />
          </el-table>
        </div>
      </div>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onUnmounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { Search } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { fetchClusterDevices, fetchDevice, disconnectDevice, type DeviceSummary, type DeviceDetail } from '../api/devices'

const { t } = useI18n()
const searchText = ref('')
const devices = ref<DeviceSummary[]>([])
const loading = ref(false)
const clusterMode = ref(false)
const currentNodeId = ref('')

const drawerVisible = ref(false)
const selectedId = ref('')
const deviceDetail = ref<DeviceDetail | null>(null)
const detailLoading = ref(false)

const filteredDevices = computed(() => {
  if (!searchText.value) return devices.value
  const q = searchText.value.toLowerCase()
  return devices.value.filter((d) =>
    d.client_id.toLowerCase().includes(q) ||
    (d.subscriptions || []).some(t => t.toLowerCase().includes(q))
  )
})

const subscriptionList = computed(() => {
  if (!deviceDetail.value?.session?.subscriptions) return []
  return Object.entries(deviceDetail.value.session.subscriptions).map(([topic, qos]) => ({ topic, qos }))
})

async function loadDevices() {
  loading.value = true
  try {
    const res = await fetchClusterDevices()
    devices.value = res.devices || []
    currentNodeId.value = res.self
    const nodeIds = new Set(devices.value.map(d => d.node_id).filter(Boolean))
    clusterMode.value = nodeIds.size > 1
    if (consecutiveErrors > 0) {
      consecutiveErrors = 0
      clearInterval(pollTimer)
      pollInterval = 5000
      pollTimer = setInterval(loadDevices, pollInterval)
      ElMessage.success(t('devices.connectionRestored'))
    }
  } catch {
    consecutiveErrors++
    if (consecutiveErrors === 1) {
      ElMessage.error(t('devices.loadFailed'))
      clearInterval(pollTimer)
      pollInterval = Math.min(pollInterval * 2, 30000)
      pollTimer = setInterval(loadDevices, pollInterval)
    }
  } finally {
    loading.value = false
  }
}

// Auto-refresh with error recovery
let pollInterval = 5000
let pollTimer = setInterval(loadDevices, pollInterval)
let consecutiveErrors = 0
onUnmounted(() => clearInterval(pollTimer))

function handleRefresh() {
  searchText.value = ''
  loadDevices()
}

async function handleDisconnect(id: string) {
  try {
    await disconnectDevice(id)
    ElMessage.success(t('devices.disconnected', { id }))
    loadDevices()
  } catch {
    ElMessage.error(t('devices.disconnectFailed'))
  }
}

async function showDetail(id: string) {
  selectedId.value = id
  drawerVisible.value = true
  detailLoading.value = true
  try {
    deviceDetail.value = await fetchDevice(id)
  } catch {
    ElMessage.error(t('devices.loadDetailFailed'))
  } finally {
    detailLoading.value = false
  }
}

function formatProtocol(v: number): string {
  const map: Record<number, string> = { 3: 'v3.1', 4: 'v3.1.1', 5: 'v5.0' }
  return map[v] || `v${v}`
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleString()
}

loadDevices()
</script>
