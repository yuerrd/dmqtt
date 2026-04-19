<template>
  <div>
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="8" v-for="node in nodes" :key="node.id">
        <el-card shadow="hover">
          <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px">
            <el-tag :type="node.id === selfId ? 'success' : 'info'" size="small">
              {{ node.id === selfId ? '本节点' : '在线' }}
            </el-tag>
            <span style="font-weight: bold; font-size: 16px">{{ node.id }}</span>
          </div>
          <el-descriptions :column="1" size="small">
            <el-descriptions-item label="地址">{{ node.host }}</el-descriptions-item>
            <el-descriptions-item label="MQTT端口">{{ node.mqttPort }}</el-descriptions-item>
            <el-descriptions-item label="HTTP端口">{{ node.httpPort || '-' }}</el-descriptions-item>
            <el-descriptions-item label="Gossip">{{ node.gossipPort }}</el-descriptions-item>
          </el-descriptions>
          <div style="margin-top: 12px" v-if="node.httpPort && node.id !== selfId">
            <el-button type="primary" size="small" text @click="openNodeAdmin(node)">
              打开管理界面 →
            </el-button>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card header="迁移任务">
      <el-table :data="migrations" v-loading="migLoading" stripe>
        <el-table-column prop="id" label="ID" min-width="120" />
        <el-table-column prop="device_id" label="设备" min-width="120" />
        <el-table-column prop="source_node" label="源节点" min-width="100" />
        <el-table-column prop="target_node" label="目标节点" min-width="100" />
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTagType(row.status)">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="进度" width="120">
          <template #default="{ row }">
            <el-progress :percentage="row.progress || 0" :stroke-width="8" />
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100">
          <template #default="{ row }">
            <el-popconfirm
              title="确定取消此迁移？"
              @confirm="handleCancel(row.id)"
            >
              <template #reference>
                <el-button type="warning" size="small" text :disabled="row.status !== 'running'">取消</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-if="!migLoading && migrations.length === 0" description="暂无迁移任务" />
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchNodes, type NodeInfo } from '../api/nodes'
import { fetchMigrations, cancelMigration, type Migration } from '../api/migrations'

const nodes = ref<NodeInfo[]>([])
const selfId = ref('')
const migrations = ref<Migration[]>([])
const migLoading = ref(false)

function openNodeAdmin(node: NodeInfo) {
  window.open(`http://${node.host}:${node.httpPort}/admin/`, '_blank')
}

function statusTagType(status: string): string {
  const map: Record<string, string> = {
    running: 'primary',
    completed: 'success',
    failed: 'danger',
    cancelled: 'warning',
  }
  return map[status] || 'info'
}

async function loadNodes() {
  try {
    const res = await fetchNodes()
    nodes.value = res.nodes || []
    selfId.value = res.self
  } catch {
    ElMessage.error('加载节点失败')
  }
}

async function loadMigrations() {
  migLoading.value = true
  try {
    migrations.value = await fetchMigrations()
  } catch {
    // standalone mode returns 503; silently show empty list
    migrations.value = []
  } finally {
    migLoading.value = false
  }
}

async function handleCancel(id: string) {
  try {
    await cancelMigration(id)
    ElMessage.success('已取消迁移')
    loadMigrations()
  } catch {
    ElMessage.error('取消失败')
  }
}

onMounted(() => {
  loadNodes()
  loadMigrations()
})
</script>