<script setup lang="ts">
import { X } from '@lucide/vue'

import type { StockSymbol, SymbolState } from '@/types/market'

defineProps<{
  symbols: StockSymbol[]
  activeSymbol: StockSymbol
  symbolStates: Record<StockSymbol, SymbolState>
}>()

const emit = defineEmits<{
  select: [symbol: StockSymbol]
  close: [symbol: StockSymbol]
}>()
</script>

<template>
  <div class="stock-tabs" role="tablist">
    <div
      v-for="symbol in symbols"
      :key="symbol"
      class="stock-tab"
      :class="{ 'is-active': symbol === activeSymbol }"
      role="tab"
      :aria-selected="symbol === activeSymbol"
      @click="emit('select', symbol)"
    >
      <button class="stock-tab__main" type="button">
        <span class="stock-tab__symbol">{{ symbol }}</span>
        <span class="stock-tab__name">
          {{
            symbolStates[symbol]?.subscriptionStatus === 'loading'
              ? '加载中'
              : symbolStates[symbol]?.subscriptionError
                ? symbolStates[symbol]?.subscriptionError
                : (symbolStates[symbol]?.snapshot?.name ?? '等待数据')
          }}
        </span>
      </button>
      <span v-if="symbolStates[symbol]?.unreadAlerts" class="stock-tab__badge">
        {{ symbolStates[symbol].unreadAlerts }}
      </span>
      <button
        class="icon-button stock-tab__close"
        type="button"
        :aria-label="`取消订阅 ${symbol}`"
        @click.stop="emit('close', symbol)"
      >
        <X :size="14" />
      </button>
    </div>
  </div>
</template>
