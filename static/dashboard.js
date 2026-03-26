(function () {
    const chartDom = document.getElementById('rul-chart');
    if (!chartDom || typeof echarts === 'undefined') return;

    const chart = echarts.init(chartDom);
    const ui = {
        startProducerBtn: document.getElementById('start-producer-btn'),
        resetEngineBtn: document.getElementById('reset-engine-btn'),
        prevEngineBtn: document.getElementById('prev-engine-btn'),
        nextEngineBtn: document.getElementById('next-engine-btn'),
        applyEngineBtn: document.getElementById('apply-engine-btn'),
        engineInput: document.getElementById('engine-id-input'),
        engineText: document.getElementById('engine-id-text'),
        cycleText: document.getElementById('current-cycle-text'),
        visiblePointsText: document.getElementById('visible-points-text'),
        modelStatusText: document.getElementById('model-status-text'),
        dataSourceText: document.getElementById('data-source-text'),
        streamRowsText: document.getElementById('stream-rows-text'),
        actualRulText: document.getElementById('actual-rul-text'),
        predictedRulText: document.getElementById('predicted-rul-text'),
        sensor1Text: document.getElementById('sensor-1-text'),
        sensor2Text: document.getElementById('sensor-2-text'),
        sensor3Text: document.getElementById('sensor-3-text'),
        sensor4Text: document.getElementById('sensor-4-text'),
    };

    let dashboardState = {
        engine_id: 1,
        engine_count: 100,
        current_cycle: 0,
        available_points: 0,
    };
    let pollTimer = null;

    function showToast(message, type = 'success') {
        if (!message) return;
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        document.body.appendChild(toast);
        requestAnimationFrame(() => toast.classList.add('show'));
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 2600);
    }

    function renderChart(payload) {
        const cycles = payload.chart.cycles || [];
        const actual = payload.chart.actual_rul || [];
        const predicted = payload.chart.predicted_rul || [];

        chart.setOption({
            backgroundColor: 'transparent',
            tooltip: { trigger: 'axis' },
            legend: { top: 12, textStyle: { color: '#4b5563' } },
            grid: { left: 52, right: 24, top: 64, bottom: 42 },
            xAxis: {
                type: 'category',
                name: '周期',
                data: cycles,
                axisLine: { lineStyle: { color: '#cbd5e1' } },
                axisLabel: { color: '#6b7280' },
            },
            yAxis: {
                type: 'value',
                name: '剩余寿命',
                axisLine: { lineStyle: { color: '#cbd5e1' } },
                splitLine: { lineStyle: { color: '#eef2f7' } },
                axisLabel: { color: '#6b7280' },
            },
            series: [
                {
                    name: '实际寿命',
                    type: 'line',
                    data: actual,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: { width: 3, type: 'dashed', color: '#111827' },
                },
                {
                    name: '预测剩余寿命',
                    type: 'line',
                    data: predicted,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: { color: '#ff5d5d', width: 3 },
                    itemStyle: { color: '#ff5d5d' },
                },
            ],
        }, true);
    }

    function updatePanel(payload) {
        dashboardState = payload.state;
        ui.engineInput.value = dashboardState.engine_id;
        ui.engineText.textContent = `#${dashboardState.engine_id}`;
        ui.cycleText.textContent = `${dashboardState.current_cycle}`;
        ui.visiblePointsText.textContent = `${dashboardState.available_points}`;

        ui.modelStatusText.textContent = dashboardState.model_loaded
            ? `${dashboardState.model_name}`
            : `${dashboardState.model_name}${dashboardState.model_error ? '（' + dashboardState.model_error + '）' : ''}`;
        ui.dataSourceText.textContent = dashboardState.data_source;
        ui.streamRowsText.textContent = `${dashboardState.stream_rows}`;
        ui.actualRulText.textContent = `${payload.summary.latest_actual_rul}`;
        ui.predictedRulText.textContent = `${payload.summary.latest_predicted_rul}`;

        const sensors = payload.summary.latest_sensors || {};
        ui.sensor1Text.textContent = sensors.T24 ?? '-';
        ui.sensor2Text.textContent = sensors.T30 ?? '-';
        ui.sensor3Text.textContent = sensors.P30 ?? '-';
        ui.sensor4Text.textContent = sensors.Nf ?? '-';

        renderChart(payload);
    }

    async function requestJSON(url, options = {}) {
        const response = await fetch(url, {
            headers: { 'Content-Type': 'application/json' },
            ...options,
        });
        const result = await response.json();
        if (!response.ok || !result.ok) {
            throw new Error(result.message || '请求失败');
        }
        return result;
    }

    function startPolling() {
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = setInterval(refreshDashboard, 1000);
    }

    async function refreshDashboard() {
        try {
            const payload = await requestJSON('/api/dashboard/state');
            updatePanel(payload);
        } catch (error) {
            showToast(error.message || '刷新失败', 'error');
        }
    }

    async function loadInitialState() {
        await refreshDashboard();
        startPolling();
    }

    async function switchEngine(engineId) {
        try {
            const payload = await requestJSON('/api/dashboard/switch-engine', {
                method: 'POST',
                body: JSON.stringify({ engine_id: engineId }),
            });
            updatePanel(payload);
            showToast(payload.message || '切换成功');
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    async function resetEngine() {
        try {
            const payload = await requestJSON('/api/dashboard/reset', {
                method: 'POST',
                body: JSON.stringify({}),
            });
            updatePanel(payload);
            showToast(payload.message || '已重置');
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    async function startProducer() {
        if (!ui.startProducerBtn) return;
        try {
            const payload = await requestJSON('/api/producer/start', {
                method: 'POST',
                body: JSON.stringify({}),
            });
            ui.startProducerBtn.textContent = '数据输出运行中';
            ui.startProducerBtn.disabled = true;
            showToast(payload.message || '已启动数据输出任务');
        } catch (error) {
            showToast(error.message || '启动失败', 'error');
        }
    }

    if (ui.startProducerBtn) {
        if (ui.startProducerBtn.textContent.includes('运行中')) {
            ui.startProducerBtn.disabled = true;
        }
        ui.startProducerBtn.addEventListener('click', startProducer);
    }
    ui.resetEngineBtn.addEventListener('click', resetEngine);
    ui.applyEngineBtn.addEventListener('click', () => switchEngine(Number(ui.engineInput.value)));
    ui.prevEngineBtn.addEventListener('click', () => {
        const nextId = Math.max(1, Number(ui.engineInput.value || 1) - 1);
        switchEngine(nextId);
    });
    ui.nextEngineBtn.addEventListener('click', () => {
        const nextId = Math.min(dashboardState.engine_count || 100, Number(ui.engineInput.value || 1) + 1);
        switchEngine(nextId);
    });

    window.addEventListener('resize', () => chart.resize());
    loadInitialState();
})();
