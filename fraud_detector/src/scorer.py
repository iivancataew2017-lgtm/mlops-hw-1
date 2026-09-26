import logging
import math
import os
from pathlib import Path

from catboost import CatBoostClassifier, Pool


logger = logging.getLogger(__name__)
MODEL_PATH = Path(os.getenv(
    "MODEL_PATH", Path(__file__).resolve().parents[1] / "models" / "my_catboost.cbm"
))
model = CatBoostClassifier()
model.load_model(str(MODEL_PATH))
model_th = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
if not 0 < model_th < 1:
    raise ValueError("fraud threshold must be between 0 and 1")

logger.info("Loaded CatBoost model from %s", MODEL_PATH)


def predict_proba(features):
    missing = set(model.feature_names_) - features.keys()
    if missing:
        raise ValueError(f"missing model features: {sorted(missing)}")
    values = [features[name] for name in model.feature_names_]
    categorical = set(model.get_cat_feature_indices())
    for index, value in enumerate(values):
        if index in categorical:
            if not isinstance(value, str):
                raise ValueError(f"categorical feature must be a string: {model.feature_names_[index]}")
        elif math.isinf(float(value)):
            raise ValueError(f"infinite feature: {model.feature_names_[index]}")
    pool = Pool([values], feature_names=model.feature_names_, cat_features=list(categorical))
    score = float(model.predict_proba(pool, task_type="CPU", thread_count=1)[0, 1])
    if not math.isfinite(score):
        raise ValueError("model returned a non-finite score")
    return score


def make_pred(features, source_info="kafka"):
    score = predict_proba(features)
    return {"score": round(score, 8), "fraud_flag": int(score > model_th)}
