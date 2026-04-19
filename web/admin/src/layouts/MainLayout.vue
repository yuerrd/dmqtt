<template>
  <el-container style="height: 100vh">
    <el-aside :width="isCollapsed ? '64px' : '200px'" style="transition: width 0.3s">
      <div style="height: 60px; display: flex; align-items: center; justify-content: center; font-size: 18px; font-weight: bold; color: #409eff">
        <span v-if="!isCollapsed">DMQTT</span>
        <span v-else>D</span>
      </div>
      <el-menu
        :default-active="route.path"
        router
        :collapse="isCollapsed"
        style="border-right: none"
      >
        <el-menu-item index="/">
          <el-icon><Monitor /></el-icon>
          <template #title>仪表盘</template>
        </el-menu-item>
        <el-menu-item index="/devices">
          <el-icon><Connection /></el-icon>
          <template #title>设备管理</template>
        </el-menu-item>
        <el-menu-item index="/cluster">
          <el-icon><Grid /></el-icon>
          <template #title>集群监控</template>
        </el-menu-item>
        <el-menu-item index="/metrics">
          <el-icon><DataLine /></el-icon>
          <template #title>系统指标</template>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header style="display: flex; align-items: center; border-bottom: 1px solid #e4e7ed">
        <el-icon style="cursor: pointer; font-size: 20px" @click="isCollapsed = !isCollapsed">
          <Fold v-if="!isCollapsed" />
          <Expand v-else />
        </el-icon>
        <span style="margin-left: 16px; font-size: 16px; font-weight: 500">{{ pageTitle }}</span>
      </el-header>
      <el-main style="background: #f5f7fa">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute } from 'vue-router'
import { Monitor, Connection, Grid, DataLine, Fold, Expand } from '@element-plus/icons-vue'

const route = useRoute()
const isCollapsed = ref(false)

const titleMap: Record<string, string> = {
  '/': '仪表盘',
  '/devices': '设备管理',
  '/cluster': '集群监控',
  '/metrics': '系统指标',
}
const pageTitle = computed(() => titleMap[route.path] || 'DMQTT')
</script>