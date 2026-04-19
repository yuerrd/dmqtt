import client from './client'

export interface Migration {
  id: string
  device_id: string
  source_node: string
  target_node: string
  status: string
  progress: number
  started_at: string
}

export async function fetchMigrations(): Promise<Migration[]> {
  const { data } = await client.get<{ migrations: Migration[] }>('/cluster/migrations')
  return data.migrations || []
}

export async function cancelMigration(id: string): Promise<void> {
  await client.post(`/cluster/migrations/${id}/cancel`)
}