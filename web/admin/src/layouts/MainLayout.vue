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
          <template #title>{{ t('nav.dashboard') }}</template>
        </el-menu-item>
        <el-menu-item index="/devices">
          <el-icon><Connection /></el-icon>
          <template #title>{{ t('nav.devices') }}</template>
        </el-menu-item>
        <el-menu-item index="/cluster">
          <el-icon><Grid /></el-icon>
          <template #title>{{ t('nav.cluster') }}</template>
        </el-menu-item>
        <el-menu-item index="/metrics">
          <el-icon><DataLine /></el-icon>
          <template #title>{{ t('nav.metrics') }}</template>
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
        <el-button-group size="small" style="margin-left: auto">
          <el-button :type="locale === 'zh' ? 'primary' : 'default'" @click="switchLang('zh')">中文</el-button>
          <el-button :type="locale === 'en' ? 'primary' : 'default'" @click="switchLang('en')">EN</el-button>
        </el-button-group>
        <el-tag v-if="nodeId" type="success" size="small" style="margin-left: 12px">{{ t('common.node') }}: {{ nodeId }}</el-tag>
      </el-header>
      <el-main style="background: #f5f7fa">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { Monitor, Connection, Grid, DataLine, Fold, Expand } from '@element-plus/icons-vue'
import { fetchStats } from '../api/stats'

const route = useRoute()
const { t, locale } = useI18n()
const isCollapsed = ref(false)
const nodeId = ref('')

function switchLang(lang: string) {
  locale.value = lang
  localStorage.setItem('dmqtt-locale', lang)
}

onMounted(async () => {
  try {
    const stats = await fetchStats()
    nodeId.value = stats.node_id || ''
  } catch {}
})

const pageTitle = computed(() => {
  const map: Record<string, string> = {
    '/': t('nav.dashboard'),
    '/devices': t('nav.devices'),
    '/cluster': t('nav.cluster'),
    '/metrics': t('nav.metrics'),
  }
  return map[route.path] || 'DMQTT'
})
</script>