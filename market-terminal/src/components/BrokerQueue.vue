<script setup lang="ts">
import { computed, ref } from 'vue'

import type { BrokerQueueEntry } from '@/types/market'
import { formatCompactNumber, formatPrice } from '@/utils/format'

const props = defineProps<{
  askQueues: BrokerQueueEntry[]
  bidQueues: BrokerQueueEntry[]
}>()

const depthOptions = [10, 100, 1000] as const
const depth = ref<(typeof depthOptions)[number]>(10)
const visibleAskQueues = computed(() => props.askQueues.slice(0, depth.value))
const visibleBidQueues = computed(() => props.bidQueues.slice(0, depth.value))
</script>

<template>
  <section class="monitor-panel">
    <header class="panel-header">
      <div>
        <h2>券商队列</h2>
      </div>
      <div class="broker-depth-control" aria-label="券商队列档位">
        <button
          v-for="option in depthOptions"
          :key="option"
          type="button"
          :class="{ 'is-active': depth === option }"
          @click="depth = option"
        >
          {{ option === 10 ? '十' : option === 100 ? '百' : '千' }}
        </button>
      </div>
    </header>
    <div class="queue-grid">
      <div class="queue-table">
        <h3>卖盘</h3>
        <table class="data-table is-compact">
          <colgroup>
            <col class="queue-col-position" />
            <col class="queue-col-name" />
            <col class="queue-col-price" />
            <col class="queue-col-volume" />
          </colgroup>
          <tbody>
            <tr v-for="entry in visibleAskQueues" :key="entry.id">
              <td>{{ entry.position }}</td>
              <td>{{ entry.participantName }}</td>
              <td>{{ formatPrice(entry.price) }}</td>
              <td>{{ formatCompactNumber(entry.volume) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="queue-table">
        <h3>买盘</h3>
        <table class="data-table is-compact">
          <colgroup>
            <col class="queue-col-position" />
            <col class="queue-col-name" />
            <col class="queue-col-price" />
            <col class="queue-col-volume" />
          </colgroup>
          <tbody>
            <tr v-for="entry in visibleBidQueues" :key="entry.id">
              <td>{{ entry.position }}</td>
              <td>{{ entry.participantName }}</td>
              <td>{{ formatPrice(entry.price) }}</td>
              <td>{{ formatCompactNumber(entry.volume) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>
