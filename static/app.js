(function () {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabPanes = document.querySelectorAll('.tab-pane');

    tabButtons.forEach((button) => {
        button.addEventListener('click', () => {
            const target = button.dataset.tab;
            tabButtons.forEach((item) => item.classList.remove('active'));
            tabPanes.forEach((item) => item.classList.remove('active'));
            button.classList.add('active');
            const pane = document.getElementById(target);
            if (pane) pane.classList.add('active');
        });
    });

    function showToast(message, type = 'success') {
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

    function startCountdown(button, seconds) {
        const originalText = button.dataset.originalText || button.textContent;
        button.dataset.originalText = originalText;
        button.disabled = true;
        let remaining = seconds;
        button.textContent = `${remaining}s 后重发`;

        const timer = setInterval(() => {
            remaining -= 1;
            if (remaining <= 0) {
                clearInterval(timer);
                button.disabled = false;
                button.textContent = originalText;
                return;
            }
            button.textContent = `${remaining}s 后重发`;
        }, 1000);
    }

    document.querySelectorAll('.send-code-btn').forEach((button) => {
        button.addEventListener('click', async () => {
            const form = button.closest('form');
            const selector = button.dataset.emailSelector;
            const emailInput = form ? form.querySelector(selector) : null;
            const email = emailInput ? emailInput.value.trim() : '';
            const purpose = button.dataset.purpose;

            if (!email) {
                showToast('请先输入邮箱地址。', 'error');
                if (emailInput) emailInput.focus();
                return;
            }

            try {
                button.disabled = true;
                const originalText = button.textContent;
                button.textContent = '发送中...';

                const response = await fetch('/send-code', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ email, purpose })
                });
                const result = await response.json();

                if (!response.ok || !result.ok) {
                    showToast(result.message || '发送失败，请稍后再试。', 'error');
                    button.disabled = false;
                    button.textContent = originalText;
                    return;
                }

                showToast(result.message || '验证码已发送。', 'success');
                button.dataset.originalText = originalText;
                startCountdown(button, 60);
            } catch (error) {
                showToast('请求失败，请检查网络或后端服务。', 'error');
                button.disabled = false;
                button.textContent = button.dataset.originalText || '发送验证码';
            }
        });
    });
})();
