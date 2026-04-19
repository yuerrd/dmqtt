import client from './client'

export interface DeviceSummary {
  client_id: string
  username: string
  remote_addr: string
  protocol_version: number
  connected_at: string
  keep_alive: number
}

export interface DeviceListResponse {
  devices: DeviceSummary[]
  total: number
  page: number
  per_page: number
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

export async function fetchDevice(id: string): Promise<DeviceDetail> {
  const { data } = await client.get<DeviceDetail>(`/devices/${id}`)
  return data
}

export async function disconnectDevice(id: string): Promise<void> {
  await client.post(`/devices/${id}/disconnect`)
}