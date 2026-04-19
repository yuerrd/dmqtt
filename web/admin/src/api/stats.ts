import client from './client'

export interface Stats {
  connected_clients: number
  active_subscriptions: number
  retained_messages: number
  cluster_nodes: number
  uptime_seconds: number
  goroutines: number
  memory_alloc_bytes: number
  memory_sys_bytes: number
  gc_pause_total_ns: number
}

export async function fetchStats(): Promise<Stats> {
  const { data } = await client.get<Stats>('/stats')
  return data
}