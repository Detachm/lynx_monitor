<script setup lang="ts">
import * as echarts from 'echarts'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

import type { MarketSnapshot, PriceTick } from '@/types/market'
import { formatCompactNumber, formatPercent, formatPrice } from '@/utils/format'

const props = defineProps<{
  symbol: string
  snapshot: MarketSnapshot | null
  ticks: PriceTick[]
}>()

const chartElement = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null
let renderTimer: ReturnType<typeof window.setTimeout> | null = null
let lastRenderedAt = 0

const changeClass = computed(() => {
  const change = props.snapshot?.change ?? 0
  return change > 0 ? 'is-up' : change < 0 ? 'is-down' : 'is-flat'
})

const displayName = computed(() => props.snapshot?.name || props.symbol)

function renderChart() {
  if (!chart || props.ticks.length === 0) {
    return
  }

  lastRenderedAt = Date.now()
  const labels = props.ticks.map((tick) =>
    new Intl.DateTimeFormat('zh-HK', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(new Date(tick.timestamp)),
  )

  chart.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: [
      { left: 56, right: 18, top: 18, height: '58%' },
      { left: 56, right: 18, bottom: 22, height: '18%' },
    ],
    tooltip: {
      trigger: 'axis',
      borderWidth: 0,
      backgroundColor: 'rgba(37, 38, 34, 0.92)',
      textStyle: { color: '#fff' },
      valueFormatter: (value: unknown) => (typeof value === 'number' ? formatPrice(value) : `${value}`),
    },
    xAxis: [
      {
        type: 'category',
        data: labels,
        boundaryGap: false,
        axisLine: { lineStyle: { color: '#b6b0a5' } },
        axisLabel: { color: '#6f6a60', interval: 'auto' },
      },
      {
        type: 'category',
        data: labels,
        gridIndex: 1,
        boundaryGap: false,
        axisLine: { lineStyle: { color: '#b6b0a5' } },
        axisTick: { show: false },
        axisLabel: { show: false },
      },
    ],
    yAxis: [
      {
        type: 'value',
        scale: true,
        axisLabel: { color: '#6f6a60', formatter: (value: number) => formatPrice(value) },
        splitLine: { lineStyle: { color: '#e5dfd3' } },
      },
      {
        type: 'value',
        gridIndex: 1,
        axisLabel: { color: '#6f6a60', formatter: (value: number) => formatCompactNumber(value) },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: '价格',
        type: 'line',
        data: props.ticks.map((tick) => tick.price),
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: '#1f8a5f' },
        areaStyle: { color: 'rgba(31, 138, 95, 0.1)' },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: props.ticks.map((tick) => tick.volume),
        itemStyle: { color: '#96754a' },
        barWidth: '60%',
      },
    ],
  })
}

function scheduleRender() {
  if (!chart) {
    return
  }

  if (renderTimer) {
    window.clearTimeout(renderTimer)
  }

  const wait = Math.max(0, 250 - (Date.now() - lastRenderedAt))
  renderTimer = window.setTimeout(renderChart, wait)
}

function resizeChart() {
  chart?.resize()
}

onMounted(() => {
  if (!chartElement.value) {
    return
  }

  chart = echarts.init(chartElement.value)
  scheduleRender()
  window.addEventListener('resize', resizeChart)
})

onBeforeUnmount(() => {
  if (renderTimer) {
    window.clearTimeout(renderTimer)
  }
  window.removeEventListener('resize', resizeChart)
  chart?.dispose()
  chart = null
})

watch(() => [props.symbol, props.ticks, props.snapshot], scheduleRender, { deep: true })
</script>

<template>
  <section class="monitor-panel market-chart-panel">
    <header class="panel-header">
      <div>
        <h2>{{ displayName }}</h2>
      </div>
      <div v-if="snapshot" class="quote-summary" :class="changeClass">
        <strong>{{ formatPrice(snapshot.price) }}</strong>
        <span>{{ formatPrice(snapshot.change) }} / {{ formatPercent(snapshot.changePercent) }}</span>
      </div>
    </header>
    <div ref="chartElement" class="market-chart" />
  </section>
</template>
