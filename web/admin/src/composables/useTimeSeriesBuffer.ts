import { ref, type Ref } from 'vue'

export interface TimeSeriesPoint {
  time: string
  [key: string]: any
}

export function useTimeSeriesBuffer(maxPoints = 60): {
  points: Ref<TimeSeriesPoint[]>
  push: (point: TimeSeriesPoint) => void
  clear: () => void
} {
  const points = ref<TimeSeriesPoint[]>([])

  function push(point: TimeSeriesPoint) {
    points.value = [...points.value.slice(-(maxPoints - 1)), point]
  }

  function clear() {
    points.value = []
  }

  return { points, push, clear }
}