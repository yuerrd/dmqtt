import client from './client'

export interface NodeInfo {
  id: string
  addr: string
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