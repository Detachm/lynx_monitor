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
const maxAvailableDepth = computed(() => {
  const depthPositions = [...props.askQueues, ...props.bidQueues]
    .filter((entry) => entry.depthAvailable && entry.levelVolume !== null && entry.levelVolume !== undefined)
    .map((entry) => entry.depthPosition ?? entry.position)
  if (depthPositions.length > 0) {
    return Math.max(...depthPositions)
  }
  return Math.max(props.askQueues.length, props.bidQueues.length)
})
const activeDepth = computed(() => Math.min(depth.value, Math.max(10, maxAvailableDepth.value || depth.value)))
const visibleAskQueues = computed(() => props.askQueues.slice(0, activeDepth.value))
const visibleBidQueues = computed(() => props.bidQueues.slice(0, activeDepth.value))

function depthLabel(option: number): string {
  if (option === 10) {
    return '十'
  }
  if (option === 100) {
    return '百'
  }
  return '千'
}

function formatQueueVolume(volume: number | null): string {
  return volume === null ? '--' : formatCompactNumber(volume)
}

function isDepthOptionEnabled(option: number): boolean {
  return option === 10 || maxAvailableDepth.value >= option
}

function chooseDepth(option: (typeof depthOptions)[number]) {
  if (isDepthOptionEnabled(option)) {
    depth.value = option
  }
}
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
          :class="{ 'is-active': activeDepth === option }"
          :disabled="!isDepthOptionEnabled(option)"
          @click="chooseDepth(option)"
        >
          {{ depthLabel(option) }}
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
            <col class="queue-col-volume" />
          </colgroup>
          <tbody>
            <tr v-for="entry in visibleAskQueues" :key="entry.id">
              <td>{{ entry.position }}</td>
              <td>{{ entry.participantName }}</td>
              <td>{{ formatPrice(entry.price) }}</td>
              <td>{{ formatQueueVolume(entry.volume) }}</td>
              <td>{{ entry.levelVolume == null ? '--' : formatCompactNumber(entry.levelVolume) }}</td>
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
            <col class="queue-col-volume" />
          </colgroup>
          <tbody>
            <tr v-for="entry in visibleBidQueues" :key="entry.id">
              <td>{{ entry.position }}</td>
              <td>{{ entry.participantName }}</td>
              <td>{{ formatPrice(entry.price) }}</td>
              <td>{{ formatQueueVolume(entry.volume) }}</td>
              <td>{{ entry.levelVolume == null ? '--' : formatCompactNumber(entry.levelVolume) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>
