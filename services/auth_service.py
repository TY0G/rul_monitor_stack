"""认证与验证码相关服务。

将原 app.py 中的认证逻辑下沉到该模块，便于维护并避免单文件过长。
"""

from __future__ import annotations

import random
import re
import smtplib
import sqlite3
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path

from flask import current_app, flash, g, redirect, session, url_for
from werkzeug.security import generate_password_hash

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


# --------------------------
# 时间与类型安全工具函数
# --------------------------
def now_str() -> str:
    """返回 UTC ISO 时间字符串（秒级精度）。"""
    return datetime.utcnow().isoformat(timespec="seconds")


def parse_dt(value: str) -> datetime:
    """解析 ISO 时间字符串。"""
    return datetime.fromisoformat(value)


# --------------------------
# DB 连接与表初始化
# --------------------------
def get_db(database_path: Path):
    """获取当前请求上下文中的 SQLite 连接。"""
    if "db" not in g:
        g.db = sqlite3.connect(database_path)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(_error=None):
    """在请求结束时关闭连接。"""
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db(database_path: Path):
    """初始化用户与验证码表。"""
    db = get_db(database_path)
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


# --------------------------
# 用户与验证码操作
# --------------------------
def login_required(view_func):
    """要求用户已登录，否则跳转到登录页。"""

    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("请先登录后再访问主页。", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


def is_valid_email(email: str) -> bool:
    return bool(EMAIL_REGEX.match((email or "").strip()))


def get_user_by_email(database_path: Path, email: str):
    db = get_db(database_path)
    return db.execute("SELECT * FROM users WHERE email = ?", (email.strip().lower(),)).fetchone()


def get_user_by_id(database_path: Path, user_id: int):
    db = get_db(database_path)
    return db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def create_user(database_path: Path, email: str, password: str):
    db = get_db(database_path)
    db.execute(
        "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
        (email.strip().lower(), generate_password_hash(password), now_str()),
    )
    db.commit()


def update_password(database_path: Path, email: str, new_password: str):
    db = get_db(database_path)
    db.execute(
        "UPDATE users SET password_hash = ? WHERE email = ?",
        (generate_password_hash(new_password), email.strip().lower()),
    )
    db.commit()


def create_code(database_path: Path, email: str, purpose: str) -> str:
    code = f"{random.randint(100000, 999999)}"
    db = get_db(database_path)
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
            (datetime.utcnow() + timedelta(minutes=current_app.config["EMAIL_CODE_EXPIRE_MINUTES"])).isoformat(
                timespec="seconds"
            ),
            now_str(),
        ),
    )
    db.commit()
    return code


def can_resend_code(database_path: Path, email: str, purpose: str):
    db = get_db(database_path)
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
    wait_seconds = current_app.config["EMAIL_CODE_RESEND_SECONDS"] - seconds
    if wait_seconds > 0:
        return False, wait_seconds
    return True, 0


def verify_code(database_path: Path, email: str, purpose: str, code: str) -> bool:
    db = get_db(database_path)
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
    """发送验证码邮件；在调试模式下回退到终端打印。"""
    purpose_map = {"register": "注册", "login": "登录", "reset": "重置密码"}
    subject = f"[{current_app.config['SITE_NAME']}] {purpose_map.get(purpose, '验证码')}验证码"
    body = f"""
您好：

您正在进行{purpose_map.get(purpose, '')}操作。
本次验证码为：{code}
有效期：{current_app.config['EMAIL_CODE_EXPIRE_MINUTES']} 分钟

如果这不是您的操作，请忽略本邮件。

—— {current_app.config['SITE_NAME']}
""".strip()

    if current_app.config["MAIL_DEBUG_PRINT"] or not all(
        [current_app.config["MAIL_SERVER"], current_app.config["MAIL_USERNAME"], current_app.config["MAIL_PASSWORD"]]
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
    msg["From"] = current_app.config["MAIL_SENDER"]
    msg["To"] = email

    if current_app.config["MAIL_USE_SSL"]:
        with smtplib.SMTP_SSL(current_app.config["MAIL_SERVER"], current_app.config["MAIL_PORT"], timeout=10) as server:
            server.login(current_app.config["MAIL_USERNAME"], current_app.config["MAIL_PASSWORD"])
            server.sendmail(current_app.config["MAIL_SENDER"], [email], msg.as_string())
    else:
        with smtplib.SMTP(current_app.config["MAIL_SERVER"], current_app.config["MAIL_PORT"], timeout=10) as server:
            if current_app.config["MAIL_USE_TLS"]:
                server.starttls()
            server.login(current_app.config["MAIL_USERNAME"], current_app.config["MAIL_PASSWORD"])
            server.sendmail(current_app.config["MAIL_SENDER"], [email], msg.as_string())


def complete_login(user):
    """写入会话并初始化 Dashboard 状态。"""
    from services.rul_service import default_dashboard_state

    session.clear()
    session.permanent = True
    session["user_id"] = user["id"]
    session["user_email"] = user["email"]
    session["dashboard_state"] = default_dashboard_state()
