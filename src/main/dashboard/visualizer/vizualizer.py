import random

import plotly.graph_objs as go
import pandas as pd
from datetime import datetime, timedelta


# --- Graphing Helper Functions (from visualizer.py or defined inline) ---
def create_empty_figure(symbol_for_title=None):
    fig = go.Figure()
    title = "No Data"
    if symbol_for_title:
        title = f"{symbol_for_title} - No Data"
    fig.update_layout(
        title_text=title, title_x=0.5,
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False)
    )
    return fig


def create_coin_chart(symbol, ohlcv_df, forecast_data_item=None):
    if ohlcv_df.empty:
        return create_empty_figure(symbol)
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=ohlcv_df["t"], open=ohlcv_df["open"], high=ohlcv_df["high"],
        low=ohlcv_df["low"], close=ohlcv_df["close"], name="Actual",
        increasing_line_color="#10aaff", decreasing_line_color="#ff5a5a",
        increasing_line_width=2, decreasing_line_width=2
    ))
    if forecast_data_item and forecast_data_item.get("forecast_values"):
        forecast_values = forecast_data_item["forecast_values"]
        last_actual_dt = pd.to_datetime(forecast_data_item.get("last_actual_time"))
        bar_duration_seconds = forecast_data_item.get("bar_duration_seconds", 1)
        forecast_times = [last_actual_dt + timedelta(seconds=bar_duration_seconds * (i + 1)) for i in range(len(forecast_values))]
        # In create_coin_chart, make forecast line thinner and lighter
        fig.add_trace(go.Scatter(
            x=forecast_times, y=forecast_values, mode='lines', name='Forecast',
            line=dict(color='#5aff71', dash='dot', shape='spline', smoothing=1.3, width=2),
            opacity=0.7
        ))

    fig.update_layout(
        autosize=True,
        height=300,  # or 500 for taller charts
        title_text=f"{symbol.upper()}", title_x=0.5, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=40, b=30),
        xaxis=dict(showgrid=True, gridcolor="#333", tickfont=dict(size=12)),
        yaxis=dict(showgrid=True, gridcolor="#333", tickfont=dict(size=12)),
        legend=dict(orientation="h", yanchor="bottom", y=.95, xanchor="right", x=1, font=dict(size=12)),
        font=dict(size=11, color="#e0e0e0")

    )
    fig.update_layout(
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )
    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.layout.uirevision = symbol

    return fig


def generate_metrics_text(symbol, ohlcv_df, forecast_data_item=None):
    if ohlcv_df.empty:
        return f"{symbol.upper()}: No data"
    metric_text = f"{symbol.upper()} - Close: {ohlcv_df['close'].iloc[-1]:.2f}"
    if forecast_data_item and forecast_data_item.get("forecast_values"):
        forecast_values = forecast_data_item["forecast_values"]
        if forecast_values:  # Check if list is not empty
            metric_text += f" | Next Forecast: {forecast_values[0]:.2f}"
    return metric_text


def generate_dummy_ohlcv(symbol: str, periods: int = 6, freq_seconds: int = 1) -> pd.DataFrame:
    now = datetime.utcnow()
    times = [now - timedelta(seconds=(periods - i) * freq_seconds) for i in range(periods)]
    base = random.uniform(120, 180)
    opens = [base + random.uniform(-5, 5) for _ in times]
    highs = [o + random.uniform(2, 8) for o in opens]
    lows = [o - random.uniform(2, 8) for o in opens]
    closes = [random.uniform(l, h) for l, h in zip(lows, highs)]
    return pd.DataFrame({
        "t": times,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes
    })


def generate_dummy_forecast(symbol: str, last_actual_time: datetime, last_close: float, horizon: int = 8, freq_seconds: int = 1):
    forecast_values = []
    price = last_close
    for _ in range(horizon):
        price = price + random.uniform(-2, 2)  # less noise
        forecast_values.append(price)
    return {
        "symbol": symbol,
        "forecast_values": forecast_values,
        "last_actual_time": last_actual_time,
        "bar_duration_seconds": freq_seconds
    }