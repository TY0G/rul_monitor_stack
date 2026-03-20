import json
import os
import random
import re
import sqlite3
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from flask import (
    Flask,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler
from werkzeug.security import check_password_hash, generate_password_hash

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
PRODUCER_FLAG = CONTROL_DIR / "producer.enabled"
STREAM_OUTPUT_DIR = RUNTIME_DIR / "stream_output" / "parsed"
STREAM_CHECKPOINT_DIR = RUNTIME_DIR / "checkpoints" / "spark_rul"

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

SENSOR_SUMMARY_COLUMNS = ["T24", "T30", "P30", "Nf"]
DEFAULT_VISIBLE_POINTS = 12

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-to-a-random-secret-key")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)
app.config["SITE_NAME"] = os.getenv("SITE_NAME", "这里填写网站名")
app.config["EMAIL_CODE_EXPIRE_MINUTES"] = 5
app.config["EMAIL_CODE_RESEND_SECONDS"] = 60

app.config["ENGINE_COUNT"] = 100
app.config["DEFAULT_ENGINE_ID"] = 1
app.config["DEFAULT_PRINT_SPEED_MS"] = 800
app.config["MIN_PRINT_SPEED_MS"] = 100
app.config["MAX_PRINT_SPEED_MS"] = 5000
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

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

FD001_DATA = {}
MODEL_BUNDLE = {
    "loaded": False,
    "name": "未初始化",
    "error": None,
    "model": None,
    "scaler": None,
    "path": str(MODEL_PATH),
    "scaler_path": str(SCALER_PATH),
}
STREAM_CACHE = {"signature": None, "df": None}


def prepare_runtime_dirs():
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    STREAM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STREAM_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


prepare_runtime_dirs()


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_error=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS verification_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            code TEXT NOT NULL,
            purpose TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        """
    )
    db.commit()


with app.app_context():
    init_db()


def now_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("请先登录后再访问主页。", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


def is_valid_email(email: str) -> bool:
    return bool(EMAIL_REGEX.match((email or "").strip()))


def get_user_by_email(email: str):
    db = get_db()
    return db.execute("SELECT * FROM users WHERE email = ?", (email.strip().lower(),)).fetchone()


def get_user_by_id(user_id: int):
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def create_user(email: str, password: str):
    db = get_db()
    db.execute(
        "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
        (email.strip().lower(), generate_password_hash(password), now_str()),
    )
    db.commit()


def update_password(email: str, new_password: str):
    db = get_db()
    db.execute(
        "UPDATE users SET password_hash = ? WHERE email = ?",
        (generate_password_hash(new_password), email.strip().lower()),
    )
    db.commit()


def create_code(email: str, purpose: str) -> str:
    code = f"{random.randint(100000, 999999)}"
    db = get_db()
    db.execute(
        "UPDATE verification_codes SET used = 1 WHERE email = ? AND purpose = ? AND used = 0",
        (email.strip().lower(), purpose),
    )
    db.execute(
        "INSERT INTO verification_codes (email, code, purpose, expires_at, used, created_at) VALUES (?, ?, ?, ?, 0, ?)",
        (
            email.strip().lower(),
            code,
            purpose,
            (datetime.utcnow() + timedelta(minutes=app.config["EMAIL_CODE_EXPIRE_MINUTES"])).isoformat(
                timespec="seconds"
            ),
            now_str(),
        ),
    )
    db.commit()
    return code


def can_resend_code(email: str, purpose: str):
    db = get_db()
    row = db.execute(
        """
        SELECT created_at FROM verification_codes
        WHERE email = ? AND purpose = ?
        ORDER BY id DESC LIMIT 1
        """,
        (email.strip().lower(), purpose),
    ).fetchone()
    if not row:
        return True, 0
    seconds = int((datetime.utcnow() - parse_dt(row["created_at"])).total_seconds())
    wait_seconds = app.config["EMAIL_CODE_RESEND_SECONDS"] - seconds
    if wait_seconds > 0:
        return False, wait_seconds
    return True, 0


def verify_code(email: str, purpose: str, code: str) -> bool:
    db = get_db()
    row = db.execute(
        """
        SELECT * FROM verification_codes
        WHERE email = ? AND purpose = ? AND code = ? AND used = 0
        ORDER BY id DESC LIMIT 1
        """,
        (email.strip().lower(), purpose, code.strip()),
    ).fetchone()
    if not row:
        return False
    if parse_dt(row["expires_at"]) < datetime.utcnow():
        return False
    db.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))
    db.commit()
    return True


def send_email_code(email: str, purpose: str, code: str):
    purpose_map = {"register": "注册", "login": "登录", "reset": "重置密码"}
    subject = f"[{app.config['SITE_NAME']}] {purpose_map.get(purpose, '验证码')}验证码"
    body = f"""
您好：

您正在进行{purpose_map.get(purpose, '')}操作。
本次验证码为：{code}
有效期：{app.config['EMAIL_CODE_EXPIRE_MINUTES']} 分钟

如果这不是您的操作，请忽略本邮件。

—— {app.config['SITE_NAME']}
""".strip()

    if app.config["MAIL_DEBUG_PRINT"] or not all(
        [app.config["MAIL_SERVER"], app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"]]
    ):
        print("=" * 40)
        print("[调试模式] 邮箱验证码已生成")
        print(f"收件人: {email}")
        print(f"用途: {purpose}")
        print(f"验证码: {code}")
        print("=" * 40)
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = app.config["MAIL_SENDER"]
    msg["To"] = email

    if app.config["MAIL_USE_SSL"]:
        with smtplib.SMTP_SSL(app.config["MAIL_SERVER"], app.config["MAIL_PORT"], timeout=10) as server:
            server.login(app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"])
            server.sendmail(app.config["MAIL_SENDER"], [email], msg.as_string())
    else:
        with smtplib.SMTP(app.config["MAIL_SERVER"], app.config["MAIL_PORT"], timeout=10) as server:
            if app.config["MAIL_USE_TLS"]:
                server.starttls()
            server.login(app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"])
            server.sendmail(app.config["MAIL_SENDER"], [email], msg.as_string())


def get_alerted_engines():
    raw = session.get("alerted_engines") or []
    if not isinstance(raw, list):
        raw = []
    result = set()
    for item in raw:
        try:
            result.add(int(item))
        except (TypeError, ValueError):
            pass
    return result


def save_alerted_engines(alerted):
    session["alerted_engines"] = sorted(int(x) for x in alerted)


def clear_engine_alert(engine_id: int):
    alerted = get_alerted_engines()
    alerted.discard(int(engine_id))
    save_alerted_engines(alerted)


def send_rul_alert(recipient: str, engine_id: int, cycle: int, predicted_rul: float, actual_rul: float):
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

    # 没配置邮件，或者开启调试打印，就直接打到终端
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

    if engine_id <= 0 or cycle <= 0:
        return

    if latest_predicted_rul >= threshold:
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


def complete_login(user):
    session.clear()
    session.permanent = True
    session["user_id"] = user["id"]
    session["user_email"] = user["email"]
    session["dashboard_state"] = default_dashboard_state()


@app.context_processor
def inject_site_name():
    return {"SITE_NAME": app.config["SITE_NAME"]}


def default_dashboard_state():
    return {
        "engine_id": app.config["DEFAULT_ENGINE_ID"],
        "speed_ms": app.config["DEFAULT_PRINT_SPEED_MS"],
        "running": False,
        "visible_points": app.config["DEFAULT_VISIBLE_POINTS"],
    }


def get_dashboard_state():
    state = session.get("dashboard_state") or default_dashboard_state()
    engine_id = max(1, min(app.config["ENGINE_COUNT"], safe_int(state.get("engine_id"), 1)))
    speed_ms = safe_int(state.get("speed_ms"), app.config["DEFAULT_PRINT_SPEED_MS"])
    speed_ms = max(app.config["MIN_PRINT_SPEED_MS"], min(app.config["MAX_PRINT_SPEED_MS"], speed_ms))
    visible_points = max(1, safe_int(state.get("visible_points"), app.config["DEFAULT_VISIBLE_POINTS"]))
    running = bool(state.get("running"))
    clean_state = {
        "engine_id": engine_id,
        "speed_ms": speed_ms,
        "running": running,
        "visible_points": visible_points,
    }
    session["dashboard_state"] = clean_state
    session.modified = True
    return clean_state


def save_dashboard_state(state):
    session["dashboard_state"] = state
    session.modified = True


def read_fd001_file(path: Path, names):
    return pd.read_csv(path, sep=r"\s+", header=None, names=names, engine="python")



def prepare_fd001_data():
    train = read_fd001_file(TRAIN_PATH, COL_NAMES)
    test = read_fd001_file(TEST_PATH, COL_NAMES)
    rul_truth = read_fd001_file(RUL_PATH, ["RUL"])
    rul_truth["unit_number"] = np.arange(1, len(rul_truth) + 1)

    max_cycles = train.groupby("unit_number")["time_cycles"].max().rename("max_cycle")
    train = train.merge(max_cycles, on="unit_number")
    train["RUL"] = (train["max_cycle"] - train["time_cycles"]).clip(upper=125)
    train = train.drop(columns=["max_cycle"])

    constant_features = [col for col in train.columns if col != "RUL" and train[col].nunique() <= 1]
    train = train.drop(columns=constant_features)
    test = test.drop(columns=constant_features)

    feature_columns = [col for col in train.columns if col not in {"unit_number", "time_cycles", "RUL"}]
    test_last_cycles = test.groupby("unit_number")["time_cycles"].max().rename("last_observed_cycle")
    rul_truth_map = rul_truth.set_index("unit_number")["RUL"]
    eol_cycles = test_last_cycles + rul_truth_map
    test["actual_rul"] = test.apply(lambda row: float(eol_cycles.loc[row["unit_number"]] - row["time_cycles"]), axis=1)

    engine_frames = {}
    for engine_id, frame in test.groupby("unit_number"):
        engine_frames[int(engine_id)] = frame.sort_values("time_cycles").reset_index(drop=True)

    return {
        "train": train,
        "test": test,
        "rul_truth": rul_truth,
        "constant_features": constant_features,
        "feature_columns": feature_columns,
        "engine_frames": engine_frames,
        "test_last_cycles": test_last_cycles.to_dict(),
        "eol_cycles": eol_cycles.to_dict(),
    }



def train_default_model(prepared):
    train = prepared["train"].copy()
    feature_columns = prepared["feature_columns"]
    scaler = MinMaxScaler()
    x_train = scaler.fit_transform(train[feature_columns])
    y_train = train["RUL"].astype(float).to_numpy()

    model_name = "RandomForestRegressor（自动训练回退模型）"
    try:
        from xgboost import XGBRegressor

        model = XGBRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=6,
            n_jobs=-1,
            random_state=42,
            objective="reg:squarederror",
        )
        model_name = "XGBoost（根据 FD001 自动训练）"
    except Exception:  # noqa: BLE001
        model = RandomForestRegressor(n_estimators=240, max_depth=12, random_state=42, n_jobs=-1)

    model.fit(x_train, y_train)
    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    return model, scaler, model_name



def load_model_bundle(prepared):
    MODEL_BUNDLE["error"] = None
    try:
        if MODEL_PATH.exists() and SCALER_PATH.exists():
            model = joblib.load(MODEL_PATH)
            scaler = joblib.load(SCALER_PATH)
            if not hasattr(model, "predict"):
                raise TypeError("模型对象缺少 predict 方法")
            if not hasattr(scaler, "transform"):
                raise TypeError("Scaler 对象缺少 transform 方法")
            MODEL_BUNDLE.update(
                {
                    "loaded": True,
                    "name": "best_rul_model.pkl + scaler.pkl",
                    "model": model,
                    "scaler": scaler,
                }
            )
            return

        model, scaler, model_name = train_default_model(prepared)
        MODEL_BUNDLE.update({"loaded": True, "name": model_name, "model": model, "scaler": scaler})
    except Exception as exc:  # noqa: BLE001
        MODEL_BUNDLE.update(
            {
                "loaded": False,
                "name": "模型未加载",
                "error": str(exc),
                "model": None,
                "scaler": None,
            }
        )



def predict_frame(frame: pd.DataFrame) -> np.ndarray:
    feature_columns = FD001_DATA["feature_columns"]
    x = frame[feature_columns].copy()

    if MODEL_BUNDLE["loaded"] and MODEL_BUNDLE["model"] is not None and MODEL_BUNDLE["scaler"] is not None:
        x_scaled = MODEL_BUNDLE["scaler"].transform(x)
        pred = MODEL_BUNDLE["model"].predict(x_scaled)
        pred = np.asarray(pred, dtype=float)
        return np.maximum(pred, 0.0)

    actual = frame.get("actual_rul")
    if actual is not None:
        return np.asarray(actual, dtype=float)
    return np.zeros(len(frame), dtype=float)



def enrich_offline_engine_predictions():
    for engine_id, frame in FD001_DATA["engine_frames"].items():
        local_frame = frame.copy()
        local_frame["predicted_rul"] = predict_frame(local_frame).round(2)
        FD001_DATA["engine_frames"][engine_id] = local_frame



def stream_files_signature():
    files = []
    if STREAM_OUTPUT_DIR.exists():
        files = [
            p
            for p in STREAM_OUTPUT_DIR.rglob("*.json")
            if p.is_file() and "_spark_metadata" not in p.parts and not p.name.startswith("._")
        ]
    signature = tuple(sorted((str(p.relative_to(STREAM_OUTPUT_DIR)), int(p.stat().st_mtime), p.stat().st_size) for p in files))
    return files, signature



def read_stream_dataframe() -> pd.DataFrame:
    files, signature = stream_files_signature()
    if signature == STREAM_CACHE["signature"] and STREAM_CACHE["df"] is not None:
        return STREAM_CACHE["df"]

    if not files:
        df = pd.DataFrame()
    else:
        frames = []
        for file in files:
            try:
                frames.append(pd.read_json(file, lines=True))
            except ValueError:
                continue
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if not df.empty:
        numeric_cols = [
            "unit_number",
            "time_cycles",
            "actual_rul",
            *FD001_DATA["feature_columns"],
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["unit_number", "time_cycles"]).copy()
        df["unit_number"] = df["unit_number"].astype(int)
        df["time_cycles"] = df["time_cycles"].astype(int)
        df = df.sort_values(["unit_number", "time_cycles", "event_time"]).drop_duplicates(
            subset=["unit_number", "time_cycles"], keep="last"
        )
        if "actual_rul" not in df.columns:
            eol_cycles = FD001_DATA["eol_cycles"]
            df["actual_rul"] = df.apply(
                lambda row: float(eol_cycles.get(int(row["unit_number"]), row["time_cycles"]) - row["time_cycles"]), axis=1
            )
        df["predicted_rul"] = predict_frame(df).round(2)

    STREAM_CACHE["signature"] = signature
    STREAM_CACHE["df"] = df
    return df



def get_engine_source(engine_id: int):
    stream_df = read_stream_dataframe()
    if not stream_df.empty:
        engine_stream = stream_df[stream_df["unit_number"] == engine_id].sort_values("time_cycles").reset_index(drop=True)
        if not engine_stream.empty:
            return {
                "label": "Spark Structured Streaming / Kafka",
                "frame": engine_stream,
                "from_stream": True,
                "stream_rows": int(len(stream_df)),
            }

    frame = FD001_DATA["engine_frames"].get(engine_id, pd.DataFrame()).copy()
    return {
        "label": "离线 FD001 测试集回放",
        "frame": frame,
        "from_stream": False,
        "stream_rows": int(len(stream_df)) if not stream_df.empty else 0,
    }



def reset_producer_flag(enabled: bool):
    if not app.config["STREAM_CONTROL_ENABLED"]:
        return
    if enabled:
        PRODUCER_FLAG.parent.mkdir(parents=True, exist_ok=True)
        PRODUCER_FLAG.write_text("enabled\n", encoding="utf-8")
    else:
        if PRODUCER_FLAG.exists():
            PRODUCER_FLAG.unlink()



def build_sensor_snapshot(row: pd.Series):
    sensors = {}
    for column in SENSOR_SUMMARY_COLUMNS:
        sensors[column] = round(safe_float(row.get(column)), 4)
    return sensors



def build_dashboard_payload(state, message=None):
    source = get_engine_source(state["engine_id"])
    frame = source["frame"]
    available_points = int(len(frame))

    visible_points = min(state["visible_points"], available_points) if available_points else 0
    if available_points and visible_points <= 0:
        visible_points = min(app.config["DEFAULT_VISIBLE_POINTS"], available_points)

    visible_frame = frame.iloc[:visible_points].copy() if available_points else pd.DataFrame()
    latest_row = visible_frame.iloc[-1] if not visible_frame.empty else None
    total_cycle = int(frame["time_cycles"].max()) if available_points else 0

    return {
        "ok": True,
        "message": message,
        "state": {
            "engine_id": state["engine_id"],
            "engine_count": app.config["ENGINE_COUNT"],
            "running": state["running"],
            "speed_ms": state["speed_ms"],
            "current_cycle": int(latest_row["time_cycles"]) if latest_row is not None else 0,
            "visible_points": visible_points,
            "available_points": available_points,
            "total_cycles": total_cycle,
            "data_source": source["label"],
            "stream_enabled": source["from_stream"],
            "stream_rows": source["stream_rows"],
            "producer_enabled": PRODUCER_FLAG.exists(),
            "model_loaded": MODEL_BUNDLE["loaded"],
            "model_name": MODEL_BUNDLE["name"],
            "model_error": MODEL_BUNDLE["error"],
        },
        "chart": {
            "cycles": visible_frame["time_cycles"].astype(int).tolist() if not visible_frame.empty else [],
            "actual_rul": visible_frame["actual_rul"].round(2).tolist() if not visible_frame.empty else [],
            "predicted_rul": visible_frame["predicted_rul"].round(2).tolist() if not visible_frame.empty else [],
        },
        "summary": {
            "latest_actual_rul": round(safe_float(latest_row.get("actual_rul")), 2) if latest_row is not None else 0,
            "latest_predicted_rul": round(safe_float(latest_row.get("predicted_rul")), 2) if latest_row is not None else 0,
            "latest_sensors": build_sensor_snapshot(latest_row) if latest_row is not None else {},
            "dropped_features": FD001_DATA.get("constant_features", []),
            "feature_count": len(FD001_DATA.get("feature_columns", [])),
        },
    }


FD001_DATA = prepare_fd001_data()
load_model_bundle(FD001_DATA)
enrich_offline_engine_predictions()


@app.route("/")
def index():
    if session.get("user_id"):
        return redirect(url_for("home"))
    return redirect(url_for("login"))


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

        user = get_user_by_email(email)
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
            elif not verify_code(email, "login", code):
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
        elif get_user_by_email(email):
            flash("该邮箱已注册，请直接登录。", "warning")
        elif not code:
            flash("请输入验证码。", "error")
        elif not verify_code(email, "register", code):
            flash("验证码错误或已过期。", "error")
        elif len(password) < 6:
            flash("密码长度至少 6 位。", "error")
        elif password != confirm_password:
            flash("两次输入的密码不一致。", "error")
        else:
            create_user(email, password)
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

        user = get_user_by_email(email)
        if not is_valid_email(email):
            flash("请输入正确的邮箱地址。", "error")
        elif not user:
            flash("该邮箱尚未注册。", "error")
        elif not code:
            flash("请输入验证码。", "error")
        elif not verify_code(email, "reset", code):
            flash("验证码错误或已过期。", "error")
        elif len(password) < 6:
            flash("新密码长度至少 6 位。", "error")
        elif password != confirm_password:
            flash("两次输入的新密码不一致。", "error")
        else:
            update_password(email, password)
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

    user = get_user_by_email(email)
    if purpose == "register" and user:
        return jsonify({"ok": False, "message": "该邮箱已注册。"}), 400
    if purpose in {"login", "reset"} and not user:
        return jsonify({"ok": False, "message": "该邮箱尚未注册。"}), 400

    allow, wait_seconds = can_resend_code(email, purpose)
    if not allow:
        return jsonify({"ok": False, "message": f"发送过于频繁，请 {wait_seconds} 秒后重试。"}), 429

    code = create_code(email, purpose)
    try:
        send_email_code(email, purpose, code)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "message": f"发送验证码失败：{exc}"}), 500

    return jsonify({"ok": True, "message": "验证码已发送，请注意查收。"})


@app.route("/home")
@login_required
def home():
    user = get_user_by_id(session.get("user_id"))
    state = get_dashboard_state()
    return render_template(
        "home.html",
        user=user,
        initial_engine_id=state["engine_id"],
        speed_ms=state["speed_ms"],
        engine_count=app.config["ENGINE_COUNT"],
        model_loaded=MODEL_BUNDLE["loaded"],
        model_name=MODEL_BUNDLE["name"],
    )


@app.route("/logout")
def logout():
    session.clear()
    reset_producer_flag(False)
    flash("您已退出登录。", "success")
    return redirect(url_for("login"))


@app.route("/api/dashboard/state")
@login_required
def dashboard_state():
    state = get_dashboard_state()
    payload = build_dashboard_payload(state)
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
    state["running"] = False
    state["visible_points"] = app.config["DEFAULT_VISIBLE_POINTS"]
    save_dashboard_state(state)
    reset_producer_flag(False)
    clear_engine_alert(engine_id)

    payload = build_dashboard_payload(state, f"已切换到发动机 #{engine_id}。")
    trigger_low_rul_alert_if_needed(payload)
    return jsonify(payload)


@app.route("/api/dashboard/toggle-run", methods=["POST"])
@login_required
def dashboard_toggle_run():
    state = get_dashboard_state()
    state["running"] = not state["running"]
    save_dashboard_state(state)
    reset_producer_flag(state["running"])
    message = "已启动回放 / 推流联动。" if state["running"] else "已暂停回放 / 推流联动。"
    return jsonify(build_dashboard_payload(state, message))


@app.route("/api/dashboard/set-speed", methods=["POST"])
@login_required
def dashboard_set_speed():
    data = request.get_json(silent=True) or {}
    speed_ms = safe_int(data.get("speed_ms"), app.config["DEFAULT_PRINT_SPEED_MS"])
    if not app.config["MIN_PRINT_SPEED_MS"] <= speed_ms <= app.config["MAX_PRINT_SPEED_MS"]:
        return (
            jsonify(
                {
                    "ok": False,
                    "message": f"速度必须在 {app.config['MIN_PRINT_SPEED_MS']} 到 {app.config['MAX_PRINT_SPEED_MS']} 毫秒之间。",
                }
            ),
            400,
        )

    state = get_dashboard_state()
    state["speed_ms"] = speed_ms
    save_dashboard_state(state)
    return jsonify(build_dashboard_payload(state, f"打印周期速度已设置为 {speed_ms} ms。"))


@app.route("/api/dashboard/reset", methods=["POST"])
@login_required
def dashboard_reset():
    state = get_dashboard_state()
    current_engine_id = state["engine_id"]

    state["running"] = False
    state["visible_points"] = app.config["DEFAULT_VISIBLE_POINTS"]
    save_dashboard_state(state)
    reset_producer_flag(False)
    clear_engine_alert(current_engine_id)

    return jsonify(build_dashboard_payload(state, "当前发动机已重置到起始观察窗口。"))


@app.route("/api/dashboard/tick", methods=["POST"])
@login_required
def dashboard_tick():
    state = get_dashboard_state()
    source = get_engine_source(state["engine_id"])
    available_points = len(source["frame"])

    if available_points == 0:
        state["running"] = False
        save_dashboard_state(state)
        reset_producer_flag(False)
        return jsonify(build_dashboard_payload(state, "当前还没有可显示的数据点。"))

    if state["running"]:
        if state["visible_points"] < available_points:
            state["visible_points"] += 1
        elif not source["from_stream"]:
            state["running"] = False
            reset_producer_flag(False)
            save_dashboard_state(state)
            return jsonify(build_dashboard_payload(state, "该发动机离线回放已结束。"))

    save_dashboard_state(state)
    payload = build_dashboard_payload(state)
    trigger_low_rul_alert_if_needed(payload)
    return jsonify(payload)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
