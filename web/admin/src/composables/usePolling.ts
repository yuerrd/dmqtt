import { ref, onMounted, onUnmounted, type Ref } from 'vue'

export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs = 5000
): { data: Ref<T | null>; error: Ref<string | null>; loading: Ref<boolean>; refresh: () => Promise<void> } {
  const data = ref<T | null>(null) as Ref<T | null>
  const error = ref<string | null>(null)
  const loading = ref(true)
  let timer: ReturnType<typeof setInterval> | null = null

  async function refresh() {
    try {
      data.value = await fetcher()
      error.value = null
    } catch (e: any) {
      error.value = e.message || 'Request failed'
    } finally {
      loading.value = false
    }
  }

  onMounted(() => {
    refresh()
    timer = setInterval(refresh, intervalMs)
  })

  onUnmounted(() => {
    if (timer) clearInterval(timer)
  })

  return { data, error, loading, refresh }
}