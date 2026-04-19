import client from './client'

export interface DeviceSummary {
  client_id: string
  username: string
  remote_addr: string
  protocol_version: number
  connected_at: string
  keep_alive: number
  subscriptions: string[]
  node_id?: string
}

export interface DeviceListResponse {
  devices: DeviceSummary[]
  total: number
  page: number
  per_page: number
  node_id: string
}

export interface DeviceDetail extends DeviceSummary {
  connected: boolean
  session?: {
    clean_start: boolean
    expiry_interval: number
    subscriptions: Record<string, number>
  }
}

export async function fetchDevices(page = 1, perPage = 20): Promise<DeviceListResponse> {
  const { data } = await client.get<DeviceListResponse>('/devices', {
    params: { page, per_page: perPage },
  })
  return data
}

export async function fetchAllNodesDevices(nodes: { host: string; httpPort: number; id: string }[]): Promise<DeviceSummary[]> {
  const all: DeviceSummary[] = []
  const fetches = nodes.map(async (node) => {
    try {
      const url = `http://${node.host}:${node.httpPort}/api/v1/devices?page=1&per_page=100`
      const resp = await fetch(url)
      if (!resp.ok) return
      const data = await resp.json()
      for (const d of data.devices || []) {
        d.node_id = node.id
        all.push(d)
      }
    } catch { /* ignore unreachable nodes */ }
  })
  await Promise.all(fetches)
  return all
}

export async function fetchDevice(id: string): Promise<DeviceDetail> {
  const { data } = await client.get<DeviceDetail>(`/devices/${id}`)
  return data
}

export async function disconnectDevice(id: string): Promise<void> {
  await client.post(`/devices/${id}/disconnect`)
}