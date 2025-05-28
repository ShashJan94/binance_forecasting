import sys
from typing import Union, Any

import dash
from dash import html, dcc, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd

from src.main.dashboard.visualizer.vizualizer import (
    create_coin_chart, create_empty_figure, generate_metrics_text,
    generate_dummy_forecast, generate_dummy_ohlcv
)
from src.main.transfer.spark_to_dash import ohlcv_buffer, forecast_buffer
from dev.config import SYMBOLS


def check_kafka_incoming(ochlv_buffer, forecast_buffer):
    # check if there is any data in the buffers and return true or false
    return any(ochlv_buffer.values()) or any(forecast_buffer.values())


USE_LIVE_DATA = check_kafka_incoming(ohlcv_buffer, forecast_buffer)

CUSTOM_CSS = """
body { background: #090d13; font-family: Calibri, Arial, sans-serif; }
h1 { color: #10aaff; text-shadow: 0 0 15px #10aaff; letter-spacing: 1.5px; }
.glowy-blue { color: #10aaff; text-shadow: 0 0 10px #10aaff; }
.glowy-green { color: #5aff71; text-shadow: 0 0 10px #5aff71; font-size: 1.3em; }
.btn-glow {
    background: transparent; border: 2px solid #10aaff; color: #10aaff;
    border-radius: 8px; padding: 10px 22px; font-weight: 600;
    transition: box-shadow 0.2s, background 0.2s, color 0.2s;
    box-shadow: 0 0 10px #10aaff44;
}
.btn-glow:hover {
    box-shadow: 0 0 22px #10aaff99;
    background: #132c45;
    color: #fff;
}
.graph-card {
    background: #10141b88;
    border-radius: 16px;
    box-shadow: 0 4px 20px rgba(16, 20, 27, 0.5);
    border: 1.5px solid #10aaff44;
    padding: 20px;
    margin: 16px;
    transition: transform 0.2s, box-shadow 0.2s;
}
.graph-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 6px 30px rgba(16, 20, 27, 0.8);
}

/* --- Dash Dropdown (react-select) dark styling --- */
.Select-control, .Select-menu-outer, .Select--multi .Select-value {
    background: #181c24 !important;
    color: #e0e0e0 !important;
    border: 1.5px solid #10aaff !important;
    border-radius: 8px !important;
    font-size: 15px !important;
}
.Select-placeholder, .Select-input > input {
    color: #b0b8c1 !important;
}
.Select-menu-outer {
    border-radius: 0 0 8px 8px !important;
    background: #181c24 !important;
    color: #e0e0e0 !important;
    border: 1.5px solid #10aaff !important;
}
.Select-value-label {
    color: #10aaff !important;
}
.Select-arrow-zone, .Select-clear-zone {
    color: #10aaff !important;
}
.Select--multi .Select-value {
    background: #132c45 !important;
    border-radius: 6px !important;
    margin: 2px 4px 2px 0 !important;
}
.Select-option {
    background: #181c24 !important;
    color: #e0e0e0 !important;
    font-size: 15px !important;
}
.Select-option.is-focused {
    background: #132c45 !important;
    color: #10aaff !important;
}
.Select-option.is-selected {
    background: #10aaff !important;
    color: #181c24 !important;
}
"""

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.index_string = f"""
<!DOCTYPE html>
<html>
<head>
    {{%metas%}}
    <title>Coin Signal Live Analytics</title>
    {{%favicon%}}
    {{%css%}}
    <style>{CUSTOM_CSS}</style>
</head>
<body>
    {{%app_entry%}}
    <footer>
        {{%config%}}
        {{%scripts%}}
        {{%renderer%}}
    </footer>
</body>
</html>
"""

app.layout = html.Div([
    html.H1("Coin Signal Live Analytics", style={"textAlign": "center"}),
    html.Div([
        dbc.Button("Refresh Data", className="btn-glow", id="refresh-btn", n_clicks=0, style={'margin': '8px'}),
        dbc.Button("Pause Stream", className="btn-glow", id="pause-btn", n_clicks=0, style={'margin': '8px'}),
        html.Span("Live Status: ", className="glowy-blue", style={'marginLeft': '18px'}),
        html.Span(id="live-status", className="glowy-green", children="Running")
    ], style={"textAlign": "center", "marginBottom": "18px"}),
    dcc.Dropdown(
        id='coin-selector',
        options=[{'label': sym.upper(), 'value': sym.upper()} for sym in SYMBOLS],
        value=[SYMBOLS[0].upper(), SYMBOLS[1].upper()] if len(SYMBOLS) >= 2 else [s.upper() for s in SYMBOLS],
        multi=True,
        style={'width': '80%', 'margin': '0 auto 20px auto', 'color': '#333'}
    ),
    dcc.Interval(id="interval", interval=10000, n_intervals=0, disabled=False),
    dcc.Loading(
        id="loading-spinner",
        type="circle",
        children=dbc.Container(id='dynamic-graphs-container', fluid=True)
    )
], style={"padding": "18px"})


def get_symbol_data(symbol, use_live_data):
    ohlcv_df = pd.DataFrame()
    forecast_item = None
    if use_live_data:
        if symbol in ohlcv_buffer and ohlcv_buffer[symbol]:
            ohlcv_list = list(ohlcv_buffer[symbol])
            ohlcv_df = pd.DataFrame(ohlcv_list)
            if not ohlcv_df.empty and 't' in ohlcv_df.columns:
                ohlcv_df['t'] = pd.to_datetime(ohlcv_df['t'])
                ohlcv_df = ohlcv_df.sort_values(by='t')
            else:
                ohlcv_df = pd.DataFrame()
        if symbol in forecast_buffer and forecast_buffer[symbol]:
            forecast_raw_item = forecast_buffer[symbol][-1]
            forecast_item = {
                "symbol": forecast_raw_item.get("symbol"),
                "forecast_values": forecast_raw_item.get("forecast"),
                "last_actual_time": pd.to_datetime(forecast_raw_item.get("window_end_time")),
                "bar_duration_seconds": forecast_raw_item.get("bar_duration_seconds", 1)
            }
    else:
        if 'generate_dummy_ohlcv' in globals():
            ohlcv_df = generate_dummy_ohlcv(symbol)
        if not ohlcv_df.empty and 'generate_dummy_forecast' in globals():
            last_actual_dt = ohlcv_df['t'].iloc[-1]
            last_close = ohlcv_df['close'].iloc[-1] if 'close' in ohlcv_df.columns else 0
            forecast_item = generate_dummy_forecast(symbol, last_actual_dt, last_close)
    return ohlcv_df, forecast_item


@app.callback(
    [Output('interval', 'disabled'),
     Output('live-status', 'children')],
    [Input('pause-btn', 'n_clicks')],
    [State('interval', 'disabled')],
    prevent_initial_call=True
)
def toggle_pause_stream(n_clicks_pause, currently_disabled):
    if n_clicks_pause is None or n_clicks_pause == 0:
        return currently_disabled, "Running" if not currently_disabled else "Paused"
    new_disabled_state = not currently_disabled
    live_status_text = "Paused" if new_disabled_state else "Running"
    return new_disabled_state, live_status_text


@app.callback(
    Output('dynamic-graphs-container', 'children'),
    [
        Input("interval", "n_intervals"),
        Input("refresh-btn", "n_clicks"),
        Input('coin-selector', 'value')
    ],
    [State('interval', 'disabled')]
)
def update_dynamic_graphs(n_intervals, n_clicks_refresh, selected_symbols, interval_disabled):
    triggered_id = dash.ctx.triggered_id
    if interval_disabled and triggered_id != "refresh-btn":
        return dash.no_update
    if not selected_symbols:
        return [html.P("Select one or more coins from the dropdown to display graphs.", style={'textAlign': 'center'})]

    all_cols = []
    for symbol_upper in selected_symbols:
        symbol = symbol_upper
        ohlcv_df, forecast_item = get_symbol_data(symbol, USE_LIVE_DATA)
        if ohlcv_df.empty:
            fig = create_empty_figure()
            fig.update_layout(title_text=f"{symbol} - No Data")
            metrics_text = f"{symbol}: Waiting for data..."
        else:
            fig = create_coin_chart(symbol, ohlcv_df, forecast_item)
            metrics_text = generate_metrics_text(symbol, ohlcv_df, forecast_item)
        graph_id = {'type': 'dynamic-graph', 'symbol': symbol}
        metrics_id = {'type': 'dynamic-metrics', 'symbol': symbol}
        card = html.Div([
            dcc.Graph(id=graph_id, figure=fig, style ={"height": "400px"}),
            html.Div(id=metrics_id, children=metrics_text, className="glowy-green")
        ], className="graph-card")
        all_cols.append(dbc.Col(card, width=12, md=6, xl=4))

    cols_per_row = 3
    rows = []
    for i in range(0, len(all_cols), cols_per_row):
        rows.append(dbc.Row(all_cols[i:i + cols_per_row], style={"marginBottom": "1vw"}))
    if not rows and selected_symbols:
        return [html.P("Could not generate graphs for selected symbols.", style={'textAlign': 'center'})]
    return rows


if __name__ == '__main__':
    #run the server
    app.run_server(debug=True, port=8050)
