"""Flask 入口文件。

本次重构目标：
1) 将认证、数据处理、模型推理等逻辑拆分到 services 模块。
2) app.py 只保留配置与路由编排，降低单文件复杂度。
"""

import os
from datetime import timedelta
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

from services.auth_service import (
    can_resend_code,
    close_db,
    complete_login,
    create_code,
    create_user,
    get_user_by_email,
    get_user_by_id,
    init_db,
    is_valid_email,
    login_required,
    send_email_code,
    update_password,
    verify_code,
)
from services.rul_service import (
    MODEL_BUNDLE,
    build_dashboard_payload,
    clear_engine_alert,
    get_alerted_engines,
    get_dashboard_state,
    init_rul_context,
    save_alerted_engines,
    save_dashboard_state,
    safe_float,
    safe_int,
    prepare_runtime_dirs,
)

BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "site.db"
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
MODEL_PATH = MODELS_DIR / "best_rul_model.pkl"
SCALER_PATH = MODELS_DIR / "scaler.pkl"
TRAIN_PATH = DATA_DIR / "train_FD001.txt"
TEST_PATH = DATA_DIR / "test_FD001.txt"
RUL_PATH = DATA_DIR / "RUL_FD001.txt"
RUNTIME_DIR = BASE_DIR / "runtime"
CONTROL_DIR = RUNTIME_DIR / "control"
PRODUCER_CONTROL_FLAG = CONTROL_DIR / "producer.enabled"
STREAM_OUTPUT_DIR = RUNTIME_DIR / "stream_output" / "parsed"
STREAM_CHECKPOINT_DIR = RUNTIME_DIR / "checkpoints" / "spark_rul"
DEFAULT_VISIBLE_POINTS = 12

COL_NAMES = [
    "unit_number",
    "time_cycles",
    "setting_1",
    "setting_2",
    "setting_3",
    "T2",
    "T24",
    "T30",
    "T50",
    "P2",
    "P15",
    "P30",
    "Nf",
    "Nc",
    "epr",
    "Ps30",
    "phi",
    "NRf",
    "NRc",
    "BPR",
    "farB",
    "htBleed",
    "Nf_dmd",
    "PCNfR_dmd",
    "W31",
    "W32",
]

app = Flask(__name__)
app.teardown_appcontext(close_db)

# --------------------------
# 全局配置
# --------------------------
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-to-a-random-secret-key")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)
app.config["SITE_NAME"] = os.getenv("SITE_NAME", "这里填写网站名")
app.config["EMAIL_CODE_EXPIRE_MINUTES"] = 5
app.config["EMAIL_CODE_RESEND_SECONDS"] = 60
app.config["ENGINE_COUNT"] = 100
app.config["DEFAULT_ENGINE_ID"] = 1
app.config["DEFAULT_VISIBLE_POINTS"] = DEFAULT_VISIBLE_POINTS
app.config["MAIL_SERVER"] = os.getenv("MAIL_SERVER", "")
app.config["MAIL_PORT"] = int(os.getenv("MAIL_PORT", "465"))
app.config["MAIL_USERNAME"] = os.getenv("MAIL_USERNAME", "")
app.config["MAIL_PASSWORD"] = os.getenv("MAIL_PASSWORD", "")
app.config["MAIL_SENDER"] = os.getenv("MAIL_SENDER", app.config["MAIL_USERNAME"] or "noreply@example.com")
app.config["MAIL_USE_SSL"] = os.getenv("MAIL_USE_SSL", "true").lower() == "true"
app.config["MAIL_USE_TLS"] = os.getenv("MAIL_USE_TLS", "false").lower() == "true"
app.config["MAIL_DEBUG_PRINT"] = os.getenv("MAIL_DEBUG_PRINT", "true").lower() == "true"
app.config["STREAM_CONTROL_ENABLED"] = os.getenv("STREAM_CONTROL_ENABLED", "true").lower() == "true"
app.config["RUL_ALERT_THRESHOLD"] = int(os.getenv("RUL_ALERT_THRESHOLD", "40"))
app.config["ALERT_EMAIL"] = os.getenv("ALERT_EMAIL", "")

prepare_runtime_dirs(MODELS_DIR, CONTROL_DIR, STREAM_OUTPUT_DIR, STREAM_CHECKPOINT_DIR)

with app.app_context():
    init_db(DATABASE)
    init_rul_context(TRAIN_PATH, TEST_PATH, RUL_PATH, COL_NAMES, MODEL_PATH, SCALER_PATH)


# --------------------------
# 告警逻辑
# --------------------------
def send_rul_alert(recipient: str, engine_id: int, cycle: int, predicted_rul: float, actual_rul: float):
    """低 RUL 告警：邮件或终端打印。"""
    import smtplib
    from email.mime.text import MIMEText

    subject = f"[{app.config['SITE_NAME']}] 发动机 #{engine_id} 剩余寿命告警"
    body = f"""
您好：

发动机 #{engine_id} 已触发剩余寿命告警。

当前周期：{cycle}
预测剩余寿命：{predicted_rul}
实际剩余寿命：{actual_rul}
告警阈值：{app.config['RUL_ALERT_THRESHOLD']}

—— {app.config['SITE_NAME']}
""".strip()

    if (
        app.config["MAIL_DEBUG_PRINT"]
        or not recipient
        or not all([app.config["MAIL_SERVER"], app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"]])
    ):
        print("\n" + "=" * 50)
        print("[RUL 告警]")
        print(f"发动机编号: #{engine_id}")
        print(f"当前周期: {cycle}")
        print(f"预测剩余寿命: {predicted_rul}")
        print(f"实际剩余寿命: {actual_rul}")
        print(f"告警阈值: {app.config['RUL_ALERT_THRESHOLD']}")
        print(f"接收邮箱: {recipient or '未配置，已退回终端打印'}")
        print("=" * 50 + "\n")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = app.config["MAIL_SENDER"]
    msg["To"] = recipient

    if app.config["MAIL_USE_SSL"]:
        with smtplib.SMTP_SSL(app.config["MAIL_SERVER"], app.config["MAIL_PORT"], timeout=10) as server:
            server.login(app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"])
            server.sendmail(app.config["MAIL_SENDER"], [recipient], msg.as_string())
    else:
        with smtplib.SMTP(app.config["MAIL_SERVER"], app.config["MAIL_PORT"], timeout=10) as server:
            if app.config["MAIL_USE_TLS"]:
                server.starttls()
            server.login(app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"])
            server.sendmail(app.config["MAIL_SENDER"], [recipient], msg.as_string())


def trigger_low_rul_alert_if_needed(payload: dict):
    threshold = app.config["RUL_ALERT_THRESHOLD"]
    latest_predicted_rul = safe_float(payload["summary"].get("latest_predicted_rul"), 999999)
    engine_id = safe_int(payload["state"].get("engine_id"), 0)
    cycle = safe_int(payload["state"].get("current_cycle"), 0)
    latest_actual_rul = safe_float(payload["summary"].get("latest_actual_rul"), 0)

    if engine_id <= 0 or cycle <= 0 or latest_predicted_rul >= threshold:
        return

    alerted = get_alerted_engines()
    if engine_id in alerted:
        return

    recipient = app.config["ALERT_EMAIL"] or session.get("user_email", "")
    send_rul_alert(
        recipient=recipient,
        engine_id=engine_id,
        cycle=cycle,
        predicted_rul=round(latest_predicted_rul, 2),
        actual_rul=round(latest_actual_rul, 2),
    )
    alerted.add(engine_id)
    save_alerted_engines(alerted)


@app.context_processor
def inject_site_name():
    return {"SITE_NAME": app.config["SITE_NAME"]}


# --------------------------
# 页面路由
# --------------------------
@app.route("/")
def index():
    return redirect(url_for("home" if session.get("user_id") else "login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("home"))

    active_tab = request.args.get("tab", "password")
    if request.method == "POST":
        action = request.form.get("action")
        email = (request.form.get("email") or "").strip().lower()
        if not is_valid_email(email):
            flash("请输入正确的邮箱地址。", "error")
            return render_template("auth.html", active_tab=active_tab)

        user = get_user_by_email(DATABASE, email)
        if not user:
            flash("该邮箱尚未注册，请先注册。", "error")
            return render_template("auth.html", active_tab=active_tab)

        if action == "password_login":
            password = request.form.get("password", "")
            if not password:
                flash("请输入密码。", "error")
            elif not check_password_hash(user["password_hash"], password):
                flash("邮箱或密码错误。", "error")
            else:
                complete_login(user)
                flash("登录成功，欢迎回来。", "success")
                return redirect(url_for("home"))
            active_tab = "password"
        elif action == "code_login":
            code = request.form.get("code", "").strip()
            if not code:
                flash("请输入邮箱验证码。", "error")
            elif not verify_code(DATABASE, email, "login", code):
                flash("验证码错误或已过期。", "error")
            else:
                complete_login(user)
                flash("验证码登录成功。", "success")
                return redirect(url_for("home"))
            active_tab = "email-code"

    return render_template("auth.html", active_tab=active_tab)


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("home"))

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        code = (request.form.get("code") or "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not is_valid_email(email):
            flash("请输入正确的邮箱地址。", "error")
        elif get_user_by_email(DATABASE, email):
            flash("该邮箱已注册，请直接登录。", "warning")
        elif not code:
            flash("请输入验证码。", "error")
        elif not verify_code(DATABASE, email, "register", code):
            flash("验证码错误或已过期。", "error")
        elif len(password) < 6:
            flash("密码长度至少 6 位。", "error")
        elif password != confirm_password:
            flash("两次输入的密码不一致。", "error")
        else:
            create_user(DATABASE, email, password)
            flash("注册成功，请登录。", "success")
            return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        code = (request.form.get("code") or "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        user = get_user_by_email(DATABASE, email)
        if not is_valid_email(email):
            flash("请输入正确的邮箱地址。", "error")
        elif not user:
            flash("该邮箱尚未注册。", "error")
        elif not code:
            flash("请输入验证码。", "error")
        elif not verify_code(DATABASE, email, "reset", code):
            flash("验证码错误或已过期。", "error")
        elif len(password) < 6:
            flash("新密码长度至少 6 位。", "error")
        elif password != confirm_password:
            flash("两次输入的新密码不一致。", "error")
        else:
            update_password(DATABASE, email, password)
            flash("密码修改成功，请重新登录。", "success")
            return redirect(url_for("login"))

    return render_template("forgot_password.html")


@app.route("/send-code", methods=["POST"])
def send_code():
    data = request.get_json(silent=True) or request.form
    email = (data.get("email") or "").strip().lower()
    purpose = (data.get("purpose") or "").strip()

    if purpose not in {"register", "login", "reset"}:
        return jsonify({"ok": False, "message": "验证码用途不正确。"}), 400
    if not is_valid_email(email):
        return jsonify({"ok": False, "message": "请输入正确的邮箱地址。"}), 400

    user = get_user_by_email(DATABASE, email)
    if purpose == "register" and user:
        return jsonify({"ok": False, "message": "该邮箱已注册。"}), 400
    if purpose in {"login", "reset"} and not user:
        return jsonify({"ok": False, "message": "该邮箱尚未注册。"}), 400

    allow, wait_seconds = can_resend_code(DATABASE, email, purpose)
    if not allow:
        return jsonify({"ok": False, "message": f"发送过于频繁，请 {wait_seconds} 秒后重试。"}), 429

    code = create_code(DATABASE, email, purpose)
    try:
        send_email_code(email, purpose, code)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "message": f"发送验证码失败：{exc}"}), 500

    return jsonify({"ok": True, "message": "验证码已发送，请注意查收。"})


@app.route("/home")
@login_required
def home():
    user = get_user_by_id(DATABASE, session.get("user_id"))
    state = get_dashboard_state()
    return render_template(
        "home.html",
        user=user,
        initial_engine_id=state["engine_id"],
        engine_count=app.config["ENGINE_COUNT"],
        model_loaded=MODEL_BUNDLE["loaded"],
        model_name=MODEL_BUNDLE["name"],
        producer_started=PRODUCER_CONTROL_FLAG.exists(),
    )


@app.route("/logout")
def logout():
    session.clear()
    flash("您已退出登录。", "success")
    return redirect(url_for("login"))


# --------------------------
# Dashboard API
# --------------------------
@app.route("/api/dashboard/state")
@login_required
def dashboard_state():
    state = get_dashboard_state()
    payload = build_dashboard_payload(state, STREAM_OUTPUT_DIR)
    trigger_low_rul_alert_if_needed(payload)
    return jsonify(payload)


@app.route("/api/dashboard/switch-engine", methods=["POST"])
@login_required
def dashboard_switch_engine():
    data = request.get_json(silent=True) or {}
    engine_id = safe_int(data.get("engine_id"), app.config["DEFAULT_ENGINE_ID"])
    if not 1 <= engine_id <= app.config["ENGINE_COUNT"]:
        return jsonify({"ok": False, "message": f"发动机编号必须在 1 到 {app.config['ENGINE_COUNT']} 之间。"}), 400

    state = get_dashboard_state()
    state["engine_id"] = engine_id
    save_dashboard_state(state)
    clear_engine_alert(engine_id)

    payload = build_dashboard_payload(state, STREAM_OUTPUT_DIR, f"已切换到发动机 #{engine_id}。")
    trigger_low_rul_alert_if_needed(payload)
    return jsonify(payload)


@app.route("/api/dashboard/reset", methods=["POST"])
@login_required
def dashboard_reset():
    state = get_dashboard_state()
    current_engine_id = state["engine_id"]

    save_dashboard_state(state)
    clear_engine_alert(current_engine_id)

    return jsonify(build_dashboard_payload(state, STREAM_OUTPUT_DIR, "当前发动机已重置到起始观察窗口。"))


@app.route("/api/dashboard/tick", methods=["POST"])
@login_required
def dashboard_tick():
    state = get_dashboard_state()
    payload = build_dashboard_payload(state, STREAM_OUTPUT_DIR)
    trigger_low_rul_alert_if_needed(payload)
    return jsonify(payload)


@app.route("/api/producer/start", methods=["POST"])
@login_required
def start_producer():
    if PRODUCER_CONTROL_FLAG.exists():
        return jsonify({"ok": True, "started": True, "message": "数据输出任务已在运行中。"})

    PRODUCER_CONTROL_FLAG.parent.mkdir(parents=True, exist_ok=True)
    PRODUCER_CONTROL_FLAG.write_text("enabled\n", encoding="utf-8")
    return jsonify({"ok": True, "started": True, "message": "已启动数据输出任务。"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
