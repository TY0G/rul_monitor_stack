"""RUL 数据准备、模型推理与 dashboard 载荷构建服务。"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from flask import current_app, session
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler

SENSOR_SUMMARY_COLUMNS = ["T24", "T30", "P30", "Nf"]

FD001_DATA = {}
MODEL_BUNDLE = {
    "loaded": False,
    "name": "未初始化",
    "error": None,
    "model": None,
    "scaler": None,
    "path": "",
    "scaler_path": "",
}
STREAM_CACHE = {"signature": None, "df": None}


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


def prepare_runtime_dirs(models_dir: Path, control_dir: Path, stream_output_dir: Path, stream_checkpoint_dir: Path):
    models_dir.mkdir(parents=True, exist_ok=True)
    control_dir.mkdir(parents=True, exist_ok=True)
    stream_output_dir.mkdir(parents=True, exist_ok=True)
    stream_checkpoint_dir.mkdir(parents=True, exist_ok=True)


def read_fd001_file(path: Path, names):
    return pd.read_csv(path, sep=r"\s+", header=None, names=names, engine="python")


def prepare_fd001_data(train_path: Path, test_path: Path, rul_path: Path, col_names):
    train = read_fd001_file(train_path, col_names)
    test = read_fd001_file(test_path, col_names)
    rul_truth = read_fd001_file(rul_path, ["RUL"])
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


def train_default_model(prepared, model_path: Path, scaler_path: Path):
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
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    return model, scaler, model_name


def load_model_bundle(prepared, model_path: Path, scaler_path: Path):
    MODEL_BUNDLE["error"] = None
    MODEL_BUNDLE["path"] = str(model_path)
    MODEL_BUNDLE["scaler_path"] = str(scaler_path)
    try:
        if model_path.exists() and scaler_path.exists():
            model = joblib.load(model_path)
            scaler = joblib.load(scaler_path)
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

        model, scaler, model_name = train_default_model(prepared, model_path, scaler_path)
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


def stream_files_signature(stream_output_dir: Path):
    files = []
    if stream_output_dir.exists():
        files = [
            p
            for p in stream_output_dir.rglob("*.json")
            if p.is_file() and "_spark_metadata" not in p.parts and not p.name.startswith("._")
        ]
    signature = tuple(sorted((str(p.relative_to(stream_output_dir)), int(p.stat().st_mtime), p.stat().st_size) for p in files))
    return files, signature


def read_stream_dataframe(stream_output_dir: Path) -> pd.DataFrame:
    files, signature = stream_files_signature(stream_output_dir)
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
        numeric_cols = ["unit_number", "time_cycles", "actual_rul", *FD001_DATA["feature_columns"]]
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


def get_engine_source(engine_id: int, stream_output_dir: Path):
    stream_df = read_stream_dataframe(stream_output_dir)
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


def default_dashboard_state():
    return {
        "engine_id": current_app.config["DEFAULT_ENGINE_ID"],
        "speed_ms": current_app.config["DEFAULT_PRINT_SPEED_MS"],
        "running": False,
        "visible_points": current_app.config["DEFAULT_VISIBLE_POINTS"],
    }


def get_dashboard_state():
    state = session.get("dashboard_state") or default_dashboard_state()
    engine_id = max(1, min(current_app.config["ENGINE_COUNT"], safe_int(state.get("engine_id"), 1)))
    speed_ms = safe_int(state.get("speed_ms"), current_app.config["DEFAULT_PRINT_SPEED_MS"])
    speed_ms = max(current_app.config["MIN_PRINT_SPEED_MS"], min(current_app.config["MAX_PRINT_SPEED_MS"], speed_ms))
    visible_points = max(1, safe_int(state.get("visible_points"), current_app.config["DEFAULT_VISIBLE_POINTS"]))
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


def reset_producer_flag(producer_flag: Path, enabled: bool):
    if not current_app.config["STREAM_CONTROL_ENABLED"]:
        return
    if enabled:
        producer_flag.parent.mkdir(parents=True, exist_ok=True)
        producer_flag.write_text("enabled\n", encoding="utf-8")
    else:
        if producer_flag.exists():
            producer_flag.unlink()


def build_sensor_snapshot(row: pd.Series):
    sensors = {}
    for column in SENSOR_SUMMARY_COLUMNS:
        sensors[column] = round(safe_float(row.get(column)), 4)
    return sensors


def build_dashboard_payload(state, producer_flag: Path, stream_output_dir: Path, message=None):
    source = get_engine_source(state["engine_id"], stream_output_dir)
    frame = source["frame"]
    available_points = int(len(frame))

    visible_points = min(state["visible_points"], available_points) if available_points else 0
    if available_points and visible_points <= 0:
        visible_points = min(current_app.config["DEFAULT_VISIBLE_POINTS"], available_points)

    visible_frame = frame.iloc[:visible_points].copy() if available_points else pd.DataFrame()
    latest_row = visible_frame.iloc[-1] if not visible_frame.empty else None
    total_cycle = int(frame["time_cycles"].max()) if available_points else 0

    return {
        "ok": True,
        "message": message,
        "state": {
            "engine_id": state["engine_id"],
            "engine_count": current_app.config["ENGINE_COUNT"],
            "running": state["running"],
            "speed_ms": state["speed_ms"],
            "current_cycle": int(latest_row["time_cycles"]) if latest_row is not None else 0,
            "visible_points": visible_points,
            "available_points": available_points,
            "total_cycles": total_cycle,
            "data_source": source["label"],
            "stream_enabled": source["from_stream"],
            "stream_rows": source["stream_rows"],
            "producer_enabled": producer_flag.exists(),
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


def init_rul_context(train_path: Path, test_path: Path, rul_path: Path, col_names, model_path: Path, scaler_path: Path):
    """初始化 FD001 数据、模型与离线预测缓存。"""
    global FD001_DATA

    FD001_DATA = prepare_fd001_data(train_path, test_path, rul_path, col_names)
    load_model_bundle(FD001_DATA, model_path, scaler_path)
    enrich_offline_engine_predictions()
