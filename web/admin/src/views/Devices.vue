<template>
  <div>
    <el-card>
      <template #header>
        <div style="display: flex; align-items: center; gap: 12px">
          <el-input
            v-model="searchText"
            placeholder="搜索设备ID"
            clearable
            style="width: 300px"
            @clear="loadDevices"
            @keyup.enter="loadDevices"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
          <el-button type="primary" @click="loadDevices">搜索</el-button>
          <el-button @click="handleRefresh">刷新</el-button>
        </div>
      </template>

      <el-table :data="filteredDevices" v-loading="loading" stripe style="width: 100%" @row-click="(row: DeviceSummary) => showDetail(row.client_id)">
        <el-table-column prop="client_id" label="设备ID" min-width="150">
          <template #default="{ row }">
            <span style="color: #409eff; cursor: pointer">{{ row.client_id }}</span>
          </template>
        </el-table-column>
        <el-table-column label="所在节点" min-width="100" v-if="clusterMode">
          <template #default="{ row }">
            <el-tag size="small" :type="row.node_id === currentNodeId ? 'success' : 'info'">{{ row.node_id }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="订阅Topic" min-width="200">
          <template #default="{ row }">
            <template v-if="row.subscriptions && row.subscriptions.length">
              <el-tag v-for="t in row.subscriptions" :key="t" size="small" style="margin: 2px" type="warning">{{ t }}</el-tag>
            </template>
            <span v-else style="color: #999">无</span>
          </template>
        </el-table-column>
        <el-table-column prop="remote_addr" label="IP地址" min-width="140" />
        <el-table-column label="协议版本" width="100">
          <template #default="{ row }">
            {{ formatProtocol(row.protocol_version) }}
          </template>
        </el-table-column>
        <el-table-column label="连接时间" min-width="160">
          <template #default="{ row }">
            {{ formatTime(row.connected_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-popconfirm
              title="确定要断开此设备？"
              confirm-button-text="确定"
              cancel-button-text="取消"
              @confirm="handleDisconnect(row.client_id)"
            >
              <template #reference>
                <el-button type="danger" size="small" text>断开</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>

      <div style="margin-top: 12px; color: #999; font-size: 13px">
        共 {{ filteredDevices.length }} 个设备
      </div>
    </el-card>

    <el-drawer v-model="drawerVisible" :title="'设备详情: ' + selectedId" size="400px">
      <div v-if="deviceDetail" v-loading="detailLoading">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="设备ID">{{ deviceDetail.client_id }}</el-descriptions-item>
          <el-descriptions-item label="用户名">{{ deviceDetail.username }}</el-descriptions-item>
          <el-descriptions-item label="IP地址">{{ deviceDetail.remote_addr }}</el-descriptions-item>
          <el-descriptions-item label="协议版本">{{ formatProtocol(deviceDetail.protocol_version) }}</el-descriptions-item>
          <el-descriptions-item label="连接时间">{{ formatTime(deviceDetail.connected_at) }}</el-descriptions-item>
          <el-descriptions-item label="KeepAlive">{{ deviceDetail.keep_alive }}s</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="deviceDetail.connected ? 'success' : 'danger'">
              {{ deviceDetail.connected ? '在线' : '离线' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
        <div v-if="deviceDetail.session" style="margin-top: 16px">
          <h4>会话信息</h4>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="Clean Start">{{ deviceDetail.session.clean_start }}</el-descriptions-item>
            <el-descriptions-item label="过期时间">{{ deviceDetail.session.expiry_interval }}s</el-descriptions-item>
          </el-descriptions>
          <h4 style="margin-top: 12px">订阅</h4>
          <el-table :data="subscriptionList" size="small" style="margin-top: 8px">
            <el-table-column prop="topic" label="Topic" />
            <el-table-column prop="qos" label="QoS" width="60" />
          </el-table>
        </div>
      </div>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Search } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { fetchClusterDevices, fetchDevice, disconnectDevice, type DeviceSummary, type DeviceDetail } from '../api/devices'

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
    // Detect cluster mode: more than one unique node_id
    const nodeIds = new Set(devices.value.map(d => d.node_id).filter(Boolean))
    clusterMode.value = nodeIds.size > 1
  } catch {
    ElMessage.error('加载设备列表失败')
  } finally {
    loading.value = false
  }
}

function handleRefresh() {
  searchText.value = ''
  loadDevices()
}

async function handleDisconnect(id: string) {
  try {
    await disconnectDevice(id)
    ElMessage.success(`已断开设备 ${id}`)
    loadDevices()
  } catch {
    ElMessage.error('断开失败')
  }
}

async function showDetail(id: string) {
  selectedId.value = id
  drawerVisible.value = true
  detailLoading.value = true
  try {
    deviceDetail.value = await fetchDevice(id)
  } catch {
    ElMessage.error('加载详情失败')
  } finally {
    detailLoading.value = false
  }
}

function formatProtocol(v: number): string {
  const map: Record<number, string> = { 3: 'v3.1', 4: 'v3.1.1', 5: 'v5.0' }
  return map[v] || `v${v}`
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN')
}

loadDevices()
</script>