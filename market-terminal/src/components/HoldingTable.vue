<script setup lang="ts">
import { History } from '@lucide/vue'

import type { HoldingEntry } from '@/types/market'
import { formatCompactNumber, formatPercent, formatSignedNumber } from '@/utils/format'

const props = defineProps<{
  holding: HoldingEntry[]
}>()

const emit = defineEmits<{
  participant: [participantName: string]
}>()
</script>

<template>
  <section class="monitor-panel">
    <header class="panel-header">
      <div>
        <h2>CCASS 持仓</h2>
      </div>
    </header>
    <div class="table-scroll">
      <table class="data-table">
        <thead>
          <tr>
            <th>参与者</th>
            <th>持股</th>
            <th>占比</th>
            <th>变动</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in holding" :key="entry.participantCode" :class="{ 'is-highlighted': entry.isHighlighted }">
            <td>
              <button class="participant-button" type="button" @click="emit('participant', entry.participantName)">
                <History :size="14" />
                <span>{{ entry.participantName }}</span>
              </button>
            </td>
            <td>{{ formatCompactNumber(entry.shares) }}</td>
            <td>{{ formatPercent(entry.percent) }}</td>
            <td :class="entry.change >= 0 ? 'is-up' : 'is-down'">
              {{ formatSignedNumber(entry.change) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
