import { createRouter, createWebHistory } from 'vue-router'
import MainLayout from '../layouts/MainLayout.vue'

const router = createRouter({
  history: createWebHistory('/admin/'),
  routes: [
    {
      path: '/',
      component: MainLayout,
      children: [
        { path: '', component: () => import('../views/Dashboard.vue') },
        { path: 'devices', component: () => import('../views/Devices.vue') },
        { path: 'cluster', component: () => import('../views/Cluster.vue') },
        { path: 'metrics', component: () => import('../views/Metrics.vue') },
      ],
    },
  ],
})

export default router