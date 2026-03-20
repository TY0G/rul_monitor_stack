(function () {
    const chartDom = document.getElementById('rul-chart');
    if (!chartDom || typeof echarts === 'undefined') return;

    const chart = echarts.init(chartDom, 'dark');
    const ui = {
        toggleRunBtn: document.getElementById('toggle-run-btn'),
        resetEngineBtn: document.getElementById('reset-engine-btn'),
        prevEngineBtn: document.getElementById('prev-engine-btn'),
        nextEngineBtn: document.getElementById('next-engine-btn'),
        applyEngineBtn: document.getElementById('apply-engine-btn'),
        applySpeedBtn: document.getElementById('apply-speed-btn'),
        engineInput: document.getElementById('engine-id-input'),
        speedInput: document.getElementById('speed-input'),
        engineText: document.getElementById('engine-id-text'),
        cycleText: document.getElementById('current-cycle-text'),
        visiblePointsText: document.getElementById('visible-points-text'),
        speedText: document.getElementById('speed-text'),
        modelStatusText: document.getElementById('model-status-text'),
        dataSourceText: document.getElementById('data-source-text'),
        producerStatusText: document.getElementById('producer-status-text'),
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
        visible_points: 0,
        available_points: 0,
        running: false,
        speed_ms: 800,
    };
    let tickTimer = null;
    let busy = false;

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

        const option = {
            backgroundColor: 'transparent',
            tooltip: { trigger: 'axis' },
            legend: {
                top: 12,
                textStyle: { color: '#d8e6ff' }
            },
            grid: {
                left: 52,
                right: 24,
                top: 64,
                bottom: 42
            },
            xAxis: {
                type: 'category',
                name: '周期',
                data: cycles,
                axisLine: {
                    lineStyle: { color: 'rgba(226, 236, 255, 0.35)' }
                },
                axisLabel: { color: '#c9d9f7' }
            },
            yAxis: {
                type: 'value',
                name: '剩余寿命',
                axisLine: {
                    lineStyle: { color: 'rgba(226, 236, 255, 0.35)' }
                },
                splitLine: {
                    lineStyle: { color: 'rgba(255,255,255,0.08)' }
                },
                axisLabel: { color: '#c9d9f7' }
            },
            series: [
                {
                    name: '实际寿命',
                    type: 'line',
                    data: actual,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {
                        width: 3,
                        type: 'dashed',
                        color: '#f0f3fa'
                    }
                },
                {
                    name: '预测剩余寿命',
                    type: 'line',
                    data: predicted,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {
                        color: '#ff5d5d',
                        width: 3
                    },
                    itemStyle: {
                        color: '#ff5d5d'
                    }
                }
            ]
        };

        chart.setOption(option, true);
    }

    function updatePanel(payload) {
        dashboardState = payload.state;
        ui.engineInput.value = dashboardState.engine_id;
        ui.speedInput.value = dashboardState.speed_ms;

        ui.engineText.textContent = `#${dashboardState.engine_id}`;
        ui.cycleText.textContent = `${dashboardState.current_cycle}`;
        ui.visiblePointsText.textContent = `${dashboardState.visible_points} / ${dashboardState.available_points}`;
        ui.speedText.textContent = `${dashboardState.speed_ms} ms`;
        ui.modelStatusText.textContent = dashboardState.model_loaded
            ? `${dashboardState.model_name}`
            : `${dashboardState.model_name}${dashboardState.model_error ? '（' + dashboardState.model_error + '）' : ''}`;
        ui.dataSourceText.textContent = dashboardState.data_source;
        ui.producerStatusText.textContent = dashboardState.producer_enabled ? '已开启' : '已关闭';
        ui.streamRowsText.textContent = `${dashboardState.stream_rows}`;
        ui.actualRulText.textContent = `${payload.summary.latest_actual_rul}`;
        ui.predictedRulText.textContent = `${payload.summary.latest_predicted_rul}`;

        const sensors = payload.summary.latest_sensors || {};
        ui.sensor1Text.textContent = sensors.T24 ?? '-';
        ui.sensor2Text.textContent = sensors.T30 ?? '-';
        ui.sensor3Text.textContent = sensors.P30 ?? '-';
        ui.sensor4Text.textContent = sensors.Nf ?? '-';

        ui.toggleRunBtn.textContent = dashboardState.running ? '暂停' : '启动';
        renderChart(payload);
    }

    async function requestJSON(url, options = {}) {
        const response = await fetch(url, {
            headers: { 'Content-Type': 'application/json' },
            ...options
        });
        const result = await response.json();
        if (!response.ok || !result.ok) {
            throw new Error(result.message || '请求失败');
        }
        return result;
    }

    function stopTicker() {
        if (tickTimer) {
            clearTimeout(tickTimer);
            tickTimer = null;
        }
    }

    function scheduleTick() {
        stopTicker();
        if (!dashboardState.running) return;
        tickTimer = setTimeout(runTick, dashboardState.speed_ms);
    }

    async function runTick() {
        if (busy) return;
        busy = true;
        try {
            const payload = await requestJSON('/api/dashboard/tick', {
                method: 'POST',
                body: JSON.stringify({})
            });
            updatePanel(payload);
            if (payload.message && !dashboardState.running) {
                showToast(payload.message, 'success');
            }
        } catch (error) {
            showToast(error.message || '刷新失败', 'error');
            dashboardState.running = false;
        } finally {
            busy = false;
            scheduleTick();
        }
    }

    async function loadInitialState() {
        try {
            const payload = await requestJSON('/api/dashboard/state');
            updatePanel(payload);
            scheduleTick();
        } catch (error) {
            showToast(error.message || '主页初始化失败', 'error');
        }
    }

    async function switchEngine(engineId) {
        try {
            const payload = await requestJSON('/api/dashboard/switch-engine', {
                method: 'POST',
                body: JSON.stringify({ engine_id: engineId })
            });
            updatePanel(payload);
            stopTicker();
            showToast(payload.message || '切换成功');
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    async function toggleRun() {
        try {
            const payload = await requestJSON('/api/dashboard/toggle-run', {
                method: 'POST',
                body: JSON.stringify({})
            });
            updatePanel(payload);
            showToast(payload.message || '状态已更新');
            scheduleTick();
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    async function applySpeed() {
        const speed = Number(ui.speedInput.value);
        try {
            const payload = await requestJSON('/api/dashboard/set-speed', {
                method: 'POST',
                body: JSON.stringify({ speed_ms: speed })
            });
            updatePanel(payload);
            showToast(payload.message || '速度已更新');
            scheduleTick();
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    async function resetEngine() {
        try {
            const payload = await requestJSON('/api/dashboard/reset', {
                method: 'POST',
                body: JSON.stringify({})
            });
            updatePanel(payload);
            stopTicker();
            showToast(payload.message || '已重置');
        } catch (error) {
            showToast(error.message, 'error');
        }
    }

    ui.toggleRunBtn.addEventListener('click', toggleRun);
    ui.applySpeedBtn.addEventListener('click', applySpeed);
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
