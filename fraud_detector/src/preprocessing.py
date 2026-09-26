import json
import math
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from geopy.distance import great_circle


CATEGORICAL_COLUMNS = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
TIME_COLUMNS = ("hour", "year", "month", "day_of_month", "day_of_week")
CATEGORY_FEATURES = tuple(f"{name}_cat" for name in CATEGORICAL_COLUMNS)
MEAN_FEATURES = tuple(f"{name}_mean_enc" for name in CATEGORY_FEATURES + TIME_COLUMNS)
FEATURE_NAMES = TIME_COLUMNS + CATEGORY_FEATURES + MEAN_FEATURES + (
    "amount_log", "population_city_log", "distance_log",
)
ARTIFACT_PATH = Path(os.getenv(
    "PREPROCESSING_PATH", Path(__file__).resolve().parents[1] / "models" / "preprocessing.json"
))


@lru_cache(maxsize=1)
def load_artifacts():
    with ARTIFACT_PATH.open(encoding="utf-8") as source:
        artifacts = json.load(source)
    if artifacts["feature_names"] != list(FEATURE_NAMES):
        raise ValueError("preprocessing feature schema does not match the service")
    return artifacts


def to_number(value, default=None):
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def add_time_features(row):
    value = row.get("transaction_time")
    if value is None or value == "":
        return {name: "cat_NAN" for name in TIME_COLUMNS}
    try:
        moment = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return {name: "cat_NAN" for name in TIME_COLUMNS}
    return dict(zip(TIME_COLUMNS, map(str, (
        moment.hour, moment.year, moment.month, moment.day, moment.weekday(),
    ))))


def add_distance_features(row):
    values = [to_number(row.get(name)) for name in ("lat", "lon", "merchant_lat", "merchant_lon")]
    if any(value is None for value in values):
        return None
    lat, lon, merchant_lat, merchant_lon = values
    if not (-90 <= lat <= 90 and -90 <= merchant_lat <= 90):
        return None
    if not (-180 <= lon <= 180 and -180 <= merchant_lon <= 180):
        return None
    return great_circle((lat, lon), (merchant_lat, merchant_lon)).km


def run_preproc(row):
    if not isinstance(row, dict):
        raise TypeError("transaction data must be a JSON object")
    artifacts = load_artifacts()
    features = add_time_features(row)
    for name in CATEGORICAL_COLUMNS:
        value = row.get(name)
        features[f"{name}_cat"] = artifacts["categories"][name].get(str(value), "cat_NAN")

    # В исходном пайплайне неизвестные категории после merge дают NaN.
    for name in CATEGORY_FEATURES + TIME_COLUMNS:
        features[f"{name}_mean_enc"] = artifacts["target_means"][name].get(features[name], float("nan"))

    continuous = {
        "amount": to_number(row.get("amount")),
        "population_city": to_number(row.get("population_city")),
        "distance": add_distance_features(row),
    }
    for name, value in continuous.items():
        if value is None:
            value = artifacts["numeric_means"][name]
        if value < 0:
            raise ValueError(f"{name} must not be negative")
        features[f"{name}_log"] = math.log1p(value)
    return {name: features[name] for name in FEATURE_NAMES}
