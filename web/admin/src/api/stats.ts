import client from './client'

export interface Stats {
  node_id: string
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

export interface ClusterNodeStats {
  node_id: string
  connections: number
  subscriptions: number
  retained: number
}

export interface ClusterStats {
  nodes: ClusterNodeStats[]
  total_connections: number
  total_subscriptions: number
  self: string
}

export async function fetchStats(): Promise<Stats> {
  const { data } = await client.get<Stats>('/stats')
  return data
}

export async function fetchClusterStats(): Promise<ClusterStats> {
  const { data } = await client.get<ClusterStats>('/cluster/stats')
  return data
}