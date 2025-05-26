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
        increasing_line_color="#10aaff", decreasing_line_color="#ff5a5a"
    ))
    if forecast_data_item and forecast_data_item.get("forecast_values"):
        forecast_values = forecast_data_item["forecast_values"]
        last_actual_dt = pd.to_datetime(forecast_data_item.get("last_actual_time"))
        # Assuming 1s bars from Spark if not specified otherwise
        bar_duration_seconds = forecast_data_item.get("bar_duration_seconds", 1)

        forecast_times = [last_actual_dt + timedelta(seconds=bar_duration_seconds * (i + 1)) for i in
                          range(len(forecast_values))]

        fig.add_trace(go.Scatter(
            x=forecast_times, y=forecast_values, mode='lines', name='Forecast',
            line=dict(color='#5aff71', dash='dot')
        ))
    fig.update_layout(
        title_text=f"{symbol.upper()}", title_x=0.5, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=40, b=30), xaxis=dict(showgrid=False, type='date'),
        yaxis=dict(showgrid=False, side='right'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
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
