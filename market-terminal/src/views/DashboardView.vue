<script setup lang="ts">
import { Activity, Plus } from '@lucide/vue'
import { computed, onMounted, ref, watch } from 'vue'

import AlertsTable from '@/components/AlertsTable.vue'
import BrokerQueue from '@/components/BrokerQueue.vue'
import ConnectionStatus from '@/components/ConnectionStatus.vue'
import HoldingHistoryModal from '@/components/HoldingHistoryModal.vue'
import HoldingTable from '@/components/HoldingTable.vue'
import MarketChart from '@/components/MarketChart.vue'
import StockTabs from '@/components/StockTabs.vue'
import { DEFAULT_DATA_SOURCE_CONFIG, dataSourceOptionsFromRuntimeConfig } from '@/config/dataSource'
import { useMarketStore } from '@/stores/market'
import { normalizeStockSymbol } from '@/utils/symbol'

const market = useMarketStore()
const symbolInput = ref('')
const pendingSymbol = ref('')
const historyOpen = ref(false)
const historySymbol = ref('')
const historyParticipant = ref('')
const historyDays = ref(30)

const activeState = computed(() => market.activeSymbolState)
const dataSourceOptions = dataSourceOptionsFromRuntimeConfig(DEFAULT_DATA_SOURCE_CONFIG)
const historyRows = computed(() =>
  historySymbol.value && historyParticipant.value
    ? market.getHoldingHistory(historySymbol.value, historyParticipant.value, historyDays.value)
    : [],
)

onMounted(() => {
  if (market.connectionStatus === 'disconnected') {
    void market.initialize(DEFAULT_DATA_SOURCE_CONFIG.defaultMode, dataSourceOptions)
  }
})

async function subscribeInputSymbol() {
  if (!symbolInput.value.trim()) {
    return
  }

  const symbol = normalizeStockSymbol(symbolInput.value)
  pendingSymbol.value = symbol
  try {
    const subscription = market.subscribeSymbol(symbol)
    market.setActiveSymbol(symbol)
    await subscription
    market.setActiveSymbol(symbol)
    symbolInput.value = ''
  } finally {
    pendingSymbol.value = ''
  }
}

async function openHoldingHistory(participantName: string) {
  if (!market.activeSymbol) {
    return
  }

  historySymbol.value = market.activeSymbol
  historyParticipant.value = participantName
  historyOpen.value = true
  await market.requestHoldingHistory(historySymbol.value, participantName, historyDays.value)
}

watch(historyDays, async (days) => {
  if (!historyOpen.value || !historySymbol.value || !historyParticipant.value) {
    return
  }

  await market.requestHoldingHistory(historySymbol.value, historyParticipant.value, days)
})

</script>

<template>
  <main class="terminal-shell">
    <header class="terminal-toolbar">
      <div class="brand-lockup">
        <Activity :size="22" />
      </div>

      <form class="symbol-form" @submit.prevent="subscribeInputSymbol">
        <input v-model="symbolInput" type="text" inputmode="numeric" placeholder="输入代码，如 700" aria-label="股票代码" />
        <button class="command-button" type="submit" :disabled="Boolean(pendingSymbol)">
          <Plus :size="16" />
          <span>{{ pendingSymbol ? '加载中' : '订阅' }}</span>
        </button>
      </form>

      <StockTabs
        :symbols="market.subscribedSymbols"
        :active-symbol="market.activeSymbol"
        :symbol-states="market.symbols"
        @select="market.setActiveSymbol"
        @close="market.unsubscribeSymbol"
      />

      <ConnectionStatus :status="market.connectionStatus" :error="market.connectionError" />
    </header>

    <div v-if="activeState" class="dashboard-grid">
      <MarketChart
        :symbol="market.activeSymbol"
        :snapshot="activeState.snapshot"
        :ticks="activeState.ticks"
      />
      <AlertsTable :alerts="activeState.alerts" />
      <BrokerQueue :ask-queues="activeState.askQueues" :bid-queues="activeState.bidQueues" />
      <HoldingTable :holding="activeState.holding" @participant="openHoldingHistory" />
    </div>

    <section v-else class="empty-state">
      <h2>未订阅股票</h2>
    </section>

    <HoldingHistoryModal
      :open="historyOpen"
      :symbol="historySymbol"
      :participant-name="historyParticipant"
      :days="historyDays"
      :history="historyRows"
      @close="historyOpen = false"
      @change-days="historyDays = $event"
    />
  </main>
</template>
