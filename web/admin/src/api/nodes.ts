import client from './client'

export interface NodeInfo {
  id: string
  host: string
  gossipPort: number
  transportPort: number
  mqttPort: number
  httpPort: number
  role: string
}

export interface NodesResponse {
  nodes: NodeInfo[]
  self: string
}

export async function fetchNodes(): Promise<NodesResponse> {
  const { data } = await client.get<NodesResponse>('/nodes')
  return data
}