from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_PATH = DATA_DIR / "train_FD001.txt"
MODEL_PATH = MODELS_DIR / "best_rul_model.pkl"
SCALER_PATH = MODELS_DIR / "scaler.pkl"

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

train = pd.read_csv(TRAIN_PATH, sep=r"\s+", header=None, names=COL_NAMES, engine="python")
max_cycles = train.groupby("unit_number")["time_cycles"].max().rename("max_cycle")
train = train.merge(max_cycles, on="unit_number")
train["RUL"] = (train["max_cycle"] - train["time_cycles"]).clip(upper=125)
train = train.drop(columns=["max_cycle"])
constant_features = [col for col in train.columns if col != "RUL" and train[col].nunique() <= 1]
train = train.drop(columns=constant_features)
feature_columns = [col for col in train.columns if col not in {"unit_number", "time_cycles", "RUL"}]

scaler = MinMaxScaler()
X_train = scaler.fit_transform(train[feature_columns])
y_train = train["RUL"].astype(float).to_numpy()

model = RandomForestRegressor(n_estimators=120, max_depth=12, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

joblib.dump(model, MODEL_PATH)
joblib.dump(scaler, SCALER_PATH)
print(f"模型已保存到: {MODEL_PATH}")
print(f"Scaler 已保存到: {SCALER_PATH}")
print(f"删除常量特征: {constant_features}")
print(f"最终特征数: {len(feature_columns)}")
