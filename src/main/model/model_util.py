# src/main/model/model_util.py

import pmdarima as pm
from src.main.utility.logging_config import logger


def predict_price(past_values, horizon=30):
    """
    ARIMA-based time series forecast.
    Args:
        past_values: list-like of floats (historical prices)
        horizon: int, number of periods to forecast
    Returns:
        list of floats (length=horizon), or [nan]*horizon on error
    """
    try:
        # Convert to list if it's a numpy array or similar
        values = list(past_values)
        if len(values) < 10:
            # Not enough data for ARIMA, return NaNs
            return [float("nan")] * horizon
        model = pm.auto_arima(values, error_action='ignore', suppress_warnings=True, seasonal=False)
        forecast = model.predict(n_periods=horizon)
        return forecast.tolist()
    except Exception as e:
        logger.error(f"ARIMA error: {e}")
        return [float("nan")] * horizon
