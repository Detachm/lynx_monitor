<script setup lang="ts">
import { X } from '@lucide/vue'

import type { HoldingHistoryPoint } from '@/types/market'
import { formatCompactNumber, formatPercent, formatSignedNumber } from '@/utils/format'

defineProps<{
  open: boolean
  symbol: string
  participantName: string
  days: number
  history: HoldingHistoryPoint[]
}>()

const emit = defineEmits<{
  close: []
  changeDays: [days: number]
}>()
</script>

<template>
  <Teleport to="body">
    <div v-if="open" class="modal-backdrop" @click.self="emit('close')">
      <section class="history-modal" role="dialog" aria-modal="true">
        <header class="modal-header">
          <div>
            <h2>{{ participantName }}</h2>
            <p>{{ symbol }} · 最近 {{ days }} 日持仓</p>
          </div>
          <button class="icon-button" type="button" aria-label="关闭" @click="emit('close')">
            <X :size="18" />
          </button>
        </header>

        <div class="segmented-control">
          <button type="button" :class="{ 'is-active': days === 7 }" @click="emit('changeDays', 7)">7 日</button>
          <button type="button" :class="{ 'is-active': days === 30 }" @click="emit('changeDays', 30)">30 日</button>
          <button type="button" :class="{ 'is-active': days === 60 }" @click="emit('changeDays', 60)">60 日</button>
        </div>

        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th>日期</th>
                <th>持股</th>
                <th>占比</th>
                <th>变动</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in history" :key="row.date">
                <td>{{ row.date }}</td>
                <td>{{ formatCompactNumber(row.shares) }}</td>
                <td>{{ formatPercent(row.percent) }}</td>
                <td :class="row.change >= 0 ? 'is-up' : 'is-down'">{{ formatSignedNumber(row.change) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  </Teleport>
</template>
