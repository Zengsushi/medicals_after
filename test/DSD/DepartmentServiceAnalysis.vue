<template>
  <div class="department-analysis-container">
    <div class="header">
      <h2>📊 科室服务分析</h2>
      <div class="controls">
        <select v-model="selectedDepartments" multiple class="department-filter" @change="updateChart">
          <option v-for="dept in allDepartments" :key="dept" :value="dept">
            {{ dept }}
          </option>
        </select>
        <button :class="['chart-type-btn', { active: chartType === 'radar' }]" @click="switchChartType('radar')">
          🎯 雷达图
        </button>
        <button :class="['chart-type-btn', { active: chartType === 'parallel' }]" @click="switchChartType('parallel')">
          📈 平行坐标图
        </button>
      </div>
    </div>

    <div class="main-content">
      <div class="chart-section">
        <div ref="chartRef" class="chart-container"></div>
      </div>

      <div class="table-section">
        <h3>详细数据</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>科室</th>
              <th>医生数</th>
              <th>问诊量</th>
              <th>平均价格(元)</th>
              <th>推荐评分</th>
              <th>在线率(%)</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(item, index) in displayData" :key="index" :style="{ backgroundColor: getColor(index, 0.1) }">
              <td><span class="dept-badge" :style="{ backgroundColor: getColor(index) }"></span>{{ item.department }}</td>
              <td>{{ item.doctor_count }}</td>
              <td>{{ item.consultation_count }}</td>
              <td>¥{{ item.avg_consultation_price }}</td>
              <td>{{ item.avg_recommendation_star }} ⭐</td>
              <td>{{ item.online_ratio }}%</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="footer-info">
      <p>数据来源: medicals.ads_department_service_analysis | 展示TOP{{ displayData.length }}个科室</p>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

const chartRef = ref(null)
let chartInstance = null
const chartType = ref('radar')
const selectedDepartments = ref([])
const rawData = ref([])

const COLORS = [
  '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
  '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#4a90d9'
]

const allDepartments = computed(() => {
  return rawData.value.map(item => item.department)
})

const displayData = computed(() => {
  if (selectedDepartments.value.length > 0) {
    return rawData.value.filter(item => selectedDepartments.value.includes(item.department))
  }
  return rawData.value.slice(0, 5)
})

function getColor(index, alpha = 1) {
  const color = COLORS[index % COLORS.length]
  if (alpha < 1) {
    const hex = color.replace('#', '')
    const r = parseInt(hex.substr(0, 2), 16)
    const g = parseInt(hex.substr(2, 2), 16)
    const b = parseInt(hex.substr(4, 2), 16)
    return `rgba(${r}, ${g}, ${b}, ${alpha})`
  }
  return color
}

function normalizeData(value, field) {
  const values = rawData.value.map(item => item[field])
  const max = Math.max(...values)
  const min = Math.min(...values)
  if (max === min) return 100
  return ((value - min) / (max - min)) * 100
}

function initMockData() {
  rawData.value = [
    { department: '内科', doctor_count: 45, consultation_count: 12800, avg_consultation_price: 85.5, avg_recommendation_star: 4.8, online_ratio: 92 },
    { department: '外科', doctor_count: 38, consultation_count: 9500, avg_consultation_price: 120.0, avg_recommendation_star: 4.6, online_ratio: 88 },
    { department: '儿科', doctor_count: 32, consultation_count: 15200, avg_consultation_price: 95.0, avg_recommendation_star: 4.9, online_ratio: 95 },
    { department: '妇产科', doctor_count: 28, consultation_count: 8900, avg_consultation_price: 110.0, avg_recommendation_star: 4.7, online_ratio: 85 },
    { department: '皮肤科', doctor_count: 22, consultation_count: 6700, avg_consultation_price: 78.0, avg_recommendation_star: 4.5, online_ratio: 82 },
    { department: '骨科', doctor_count: 25, consultation_count: 5400, avg_consultation_price: 135.0, avg_recommendation_star: 4.4, online_ratio: 78 },
    { department: '眼科', doctor_count: 18, consultation_count: 4200, avg_consultation_price: 150.0, avg_recommendation_star: 4.6, online_ratio: 80 },
    { department: '耳鼻喉科', doctor_count: 20, consultation_count: 3800, avg_consultation_price: 65.0, avg_recommendation_star: 4.3, online_ratio: 76 },
    { department: '口腔科', doctor_count: 30, consultation_count: 7100, avg_consultation_price: 180.0, avg_recommendation_star: 4.7, online_ratio: 90 },
    { department: '神经科', doctor_count: 24, consultation_count: 4900, avg_consultation_price: 145.0, avg_recommendation_star: 4.5, online_ratio: 83 }
  ]
}

function getRadarOption() {
  const indicators = [
    { name: '医生数', max: 50 },
    { name: '问诊量', max: 16000 },
    { name: '平均价格', max: 200 },
    { name: '推荐评分', max: 5 },
    { name: '在线率', max: 100 }
  ]

  const series = displayData.value.map((item, index) => ({
    name: item.department,
    value: [
      item.doctor_count,
      item.consultation_count,
      item.avg_consultation_price,
      item.avg_recommendation_star,
      item.online_ratio
    ],
    symbol: 'circle',
    symbolSize: 8,
    lineStyle: {
      width: 2,
      color: getColor(index)
    },
    areaStyle: {
      color: getColor(index, 0.15)
    },
    itemStyle: {
      color: getColor(index)
    }
  }))

  return {
    title: {
      text: '科室服务多维度对比',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold'
      }
    },
    tooltip: {
      trigger: 'item',
      formatter: function(params) {
        const data = params.data
        let html = `<strong>${data.name}</strong><br/>`
        html += `医生数: ${data.value[0]} 人<br/>`
        html += `问诊量: ${data.value[1].toLocaleString()} 次<br/>`
        html += `平均价格: ¥${data.value[2]}<br/>`
        html += `推荐评分: ${data.value[3]} ⭐<br/>`
        html += `在线率: ${data.value[4]}%`
        return html
      }
    },
    legend: {
      data: displayData.value.map(d => d.department),
      bottom: 10,
      type: 'scroll'
    },
    radar: {
      indicator: indicators,
      shape: 'polygon',
      splitNumber: 5,
      axisName: {
        color: '#333',
        fontSize: 12
      },
      splitLine: {
        lineStyle: {
          color: ['#eee']
        }
      },
      splitArea: {
        areaStyle: {
          color: ['rgba(114, 172, 209, 0.02)', 'rgba(114, 172, 209, 0.04)', 
                  'rgba(114, 172, 209, 0.06)', 'rgba(114, 172, 209, 0.08)', 
                  'rgba(114, 172, 209, 0.1)']
        }
      },
      axisLine: {
        lineStyle: {
          color: 'rgba(211, 253, 250, 0.2)'
        }
      }
    },
    series: [{
      type: 'radar',
      data: series
    }]
  }
}

function getParallelOption() {
  const schema = [
    { name: '医生数', index: 0, text: '医生数', type: 'category' },
    { name: '问诊量', index: 1, text: '问诊量', type: 'category' },
    { name: '平均价格', index: 2, text: '平均价格(元)', type: 'category' },
    { name: '推荐评分', index: 3, text: '推荐评分', type: 'category' },
    { name: '在线率', index: 4, text: '在线率(%)', type: 'category' }
  ]

  const data = displayData.value.map((item, index) => ({
    value: [item.doctor_count, item.consultation_count, item.avg_consultation_price, item.avg_recommendation_star, item.online_ratio],
    name: item.department,
    lineStyle: {
      width: 3,
      color: getColor(index),
      opacity: 0.8
    }
  }))

  return {
    title: {
      text: '科室服务平行坐标分析',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold'
      }
    },
    tooltip: {
      trigger: 'item',
      formatter: function(params) {
        const data = params.data
        let html = `<strong>${data.name}</strong><br/>`
        html += `医生数: ${data.value[0]} 人<br/>`
        html += `问诊量: ${data.value[1].toLocaleString()} 次<br/>`
        html += `平均价格: ¥${data.value[2]}<br/>`
        html += `推荐评分: ${data.value[3]} ⭐<br/>`
        html += `在线率: ${data.value[4]}%`
        return html
      }
    },
    legend: {
      data: displayData.value.map(d => d.department),
      bottom: 10,
      type: 'scroll'
    },
    parallelAxis: schema.map(item => ({
      dim: item.index,
      name: item.name,
      inverse: false,
      max: item.index === 1 ? 16000 : item.index === 2 ? 200 : item.index === 4 ? 100 : (item.index === 3 ? 5 : 50),
      nameLocation: 'end',
      nameGap: 20,
      nameTextStyle: {
        fontSize: 12
      },
      axisLabel: {
        fontSize: 10
      }
    })),
    parallel: {
      left: 80,
      right: 100,
      bottom: 80,
      top: 80,
      parallelAxisDefault: {
        type: 'value',
        nameLocation: 'end',
        nameGap: 20,
        axisLine: {
          lineStyle: {
            color: '#ddd'
          }
        },
        splitLine: {
          show: false
        },
        axisTick: {
          show: false
        },
        axisLabel: {
          fontSize: 10
        }
      }
    },
    series: [{
      name: '科室数据',
      type: 'parallel',
      lineStyle: {
        width: 2
      },
      data: data.map(d => d.value),
      emphasis: {
        lineStyle: {
          width: 4
        }
      }
    }]
  }
}

function updateChart() {
  if (!chartInstance) return
  
  const option = chartType.value === 'radar' ? getRadarOption() : getParallelOption()
  chartInstance.setOption(option, true)
}

function switchChartType(type) {
  chartType.value = type
  nextTick(() => {
    updateChart()
  })
}

function initChart() {
  if (!chartRef.value) return
  
  chartInstance = echarts.init(chartRef.value)
  updateChart()
  
  window.addEventListener('resize', handleResize)
}

function handleResize() {
  chartInstance?.resize()
}

onMounted(async () => {
  initMockData()
  await nextTick()
  initChart()
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
})
</script>

<style scoped>
.department-analysis-container {
  padding: 20px;
  background: #f5f7fa;
  border-radius: 12px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  padding-bottom: 15px;
  border-bottom: 2px solid #e4e7ed;
}

.header h2 {
  margin: 0;
  color: #303133;
  font-size: 22px;
  font-weight: 600;
}

.controls {
  display: flex;
  gap: 12px;
  align-items: center;
}

.department-filter {
  padding: 8px 12px;
  border: 2px solid #dcdfe6;
  border-radius: 6px;
  background: white;
  font-size: 13px;
  min-width: 150px;
  height: auto;
  cursor: pointer;
  transition: all 0.3s ease;
}

.department-filter:focus {
  outline: none;
  border-color: #409eff;
  box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.1);
}

.chart-type-btn {
  padding: 8px 16px;
  border: 2px solid #dcdfe6;
  border-radius: 6px;
  background: white;
  color: #606266;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.3s ease;
  font-weight: 500;
}

.chart-type-btn:hover {
  border-color: #409eff;
  color: #409eff;
}

.chart-type-btn.active {
  background: #409eff;
  color: white;
  border-color: #409eff;
}

.main-content {
  display: grid;
  grid-template-columns: 1fr 400px;
  gap: 20px;
  margin-bottom: 20px;
}

.chart-section {
  background: white;
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}

.chart-container {
  width: 100%;
  height: 500px;
}

.table-section {
  background: white;
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  overflow-y: auto;
  max-height: 540px;
}

.table-section h3 {
  margin: 0 0 15px 0;
  color: #303133;
  font-size: 16px;
  font-weight: 600;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.data-table thead {
  position: sticky;
  top: 0;
  z-index: 10;
}

.data-table th {
  background: #f5f7fa;
  color: #606266;
  font-weight: 600;
  padding: 12px 10px;
  text-align: left;
  border-bottom: 2px solid #ebeef5;
  white-space: nowrap;
}

.data-table td {
  padding: 10px;
  border-bottom: 1px solid #ebeef5;
  color: #606266;
  transition: background-color 0.2s ease;
}

.data-table tr:hover td {
  background-color: #f5f7fa !important;
}

.dept-badge {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 8px;
  vertical-align: middle;
}

.footer-info {
  text-align: center;
  color: #909399;
  font-size: 12px;
  padding-top: 15px;
  border-top: 1px solid #e4e7ed;
}

@media (max-width: 1200px) {
  .main-content {
    grid-template-columns: 1fr;
  }
  
  .table-section {
    max-height: 400px;
  }
}
</style>
