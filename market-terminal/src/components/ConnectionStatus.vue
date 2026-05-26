<script setup lang="ts">
import type { ConnectionStatus } from '@/types/market'

const props = defineProps<{
  status: ConnectionStatus
  error: string | null
}>()

const labels: Record<ConnectionStatus, string> = {
  disconnected: '未连接',
  connecting: '连接中',
  connected: '已连接',
  error: '异常',
}
</script>

<template>
  <div class="connection-status" :class="`is-${props.status}`" :title="props.error ?? labels[props.status]">
    <span class="connection-status__dot" />
  </div>
</template>
