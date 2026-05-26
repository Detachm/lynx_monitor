<script setup lang="ts">
import type { BigTradeAlert } from '@/types/market'
import { formatCompactNumber, formatPrice } from '@/utils/format'

defineProps<{
  alerts: BigTradeAlert[]
}>()

function formatTime(timestamp: string): string {
  return new Intl.DateTimeFormat('zh-HK', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date(timestamp))
}
</script>

<template>
  <section class="monitor-panel">
    <header class="panel-header">
      <div>
        <h2>大额交易</h2>
      </div>
    </header>
    <div class="table-scroll">
      <table class="data-table">
        <thead>
          <tr>
            <th>时间</th>
            <th>方向</th>
            <th>价格</th>
            <th>股数</th>
            <th>券商 / 参与者</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="alert in alerts" :key="alert.id" :class="{ 'is-highlighted': alert.isHighlighted }">
            <td>{{ formatTime(alert.timestamp) }}</td>
            <td class="side-cell">
              <span class="side-pill" :class="`is-${alert.side}`">{{ alert.side }}</span>
            </td>
            <td>{{ formatPrice(alert.price) }}</td>
            <td>{{ formatCompactNumber(alert.volume) }}</td>
            <td>
              <strong>{{ alert.brokerName }}</strong>
              <span v-if="alert.remark">{{ alert.remark }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
