import dash
from dash import html, dcc, Output, Input
import plotly.graph_objs as go
import dash_bootstrap_components as dbc
import pandas as pd
import random
from src.main.dashboard.visualizer.vizualizer import create_coin_chart, create_empty_figure, generate_metrics_text


# --- Configuration for Live Data ---
USE_LIVE_DATA = True  # Set to False to use dummy data simulation
KAFKA_BROKERS = 'localhost:9092'
OHLCV_TOPIC = 'ohlcv-ticks'
FORECAST_TOPIC = 'feature-ticks'
SYMBOLS_TO_DISPLAY = ["BTCUSDT", "ETHUSDT"]

# ---- Custom CSS for glowy effects ----
CUSTOM_CSS = """
body { background: #090d13; font-family: Calibri, Arial, sans-serif; }
h1 { color: #10aaff; text-shadow: 0 0 15px #10aaff; letter-spacing: 1.5px; }
.glowy-blue { color: #10aaff; text-shadow: 0 0 10px #10aaff; }
.glowy-green { color: #5aff71; text-shadow: 0 0 10px #5aff71; font-size: 1.3em; }
.btn-glow {
    background: transparent; border: 2px solid #10aaff; color: #10aaff;
    border-radius: 8px; padding: 10px 22px; font-weight: 600;
    transition: box-shadow 0.2s;
    box-shadow: 0 0 10px #10aaff44;
}
.btn-glow:hover { box-shadow: 0 0 22px #10aaff99; background: #132c45; }
.graph-card {
    background: #10141b88; border-radius: 16px; box-shadow: 0 0 20px #10aaff11;
    border: 1.5px solid #10aaff44; padding: 16px; margin: 8px;
}
"""

# Inject custom CSS
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

# ---- UI Layout ----
# Assume SYMBOLS is loaded from your config: from config import SYMBOLS

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
        options=[{'label': sym.upper(), 'value': sym.upper()} for sym in SYMBOLS], # Ensure consistent casing if needed
        value=[SYMBOLS[0].upper(), SYMBOLS[1].upper()] if len(SYMBOLS) >= 2 else [s.upper() for s in SYMBOLS], # Default to first two, ensure casing
        multi=True,
        style={'width': '80%', 'margin': '0 auto 20px auto', 'color': '#333'} # Darker text for readability
    ),

    dcc.Interval(id="interval", interval=3000, n_intervals=0, disabled=False), # 3-second updates, added disabled state

    # This Div will be populated by a callback with graph components
    dbc.Container(id='dynamic-graphs-container', fluid=True, children=[
        # You can put a placeholder message here if you like,
        # e.g., html.P("Select coins from the dropdown to see graphs.")
    ])

], style={"padding": "18px"})

@app.callback(
    [Output('interval', 'disabled'),
     Output('live-status', 'children')],
    [Input('pause-btn', 'n_clicks')],
    [State('interval', 'disabled')],
    prevent_initial_call=True  # Don't run on app load
)
def toggle_pause_stream(n_clicks_pause, currently_disabled):
    if n_clicks_pause is None or n_clicks_pause == 0:
        # Should not happen with prevent_initial_call=True but as a safeguard
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
    [State('interval', 'disabled')]  # To check if paused
)
def update_dynamic_graphs(n_intervals, n_clicks_refresh, selected_symbols, interval_disabled):
    # Use dash.ctx.triggered_id to see what fired the callback, if needed for complex logic
    # For now, we'll update if not paused, or if refresh is clicked
    triggered_id = dash.ctx.triggered_id

    if interval_disabled and triggered_id != "refresh-btn":
        # If paused and not a manual refresh, don't update graphs.
        # Return dash.no_update to keep current graphs, or return an empty list/message
        # For simplicity, if paused, let's just signal no update to the graph container.
        # This means existing graphs stay as they are.
        return dash.no_update

    if not selected_symbols:
        return [html.P("Select one or more coins from the dropdown to display graphs.", style={'textAlign': 'center'})]

    dynamic_graph_components = []

    # Define how many graphs per row (e.g., 3 for xl, 2 for lg, 1 for smaller)
    # You can adjust this based on your preference.
    # For simplicity, let's aim for a flexible number of columns per row.
    # dbc.Row will handle wrapping if dbc.Col width sums to more than 12.

    current_row_children = []
    MAX_COLS_PER_ROW_XL = 3  # e.g. up to 3 graphs in a row on extra large screens
    # You can define more breakpoints if needed, e.g. MAX_COLS_PER_ROW_LG = 2

    for symbol_upper in selected_symbols:  # Assuming selected_symbols are already uppercase
        symbol = symbol_upper  # Use consistent casing as in your data buffers

        ohlcv_df = pd.DataFrame()
        forecast_item = None

        if USE_LIVE_DATA:  # Make sure USE_LIVE_DATA is defined in your app
            # Fetch from Live Data Buffers (Logic from your previous update_all_graphs)
            if symbol in ohlcv_buffer and ohlcv_buffer[symbol]:
                ohlcv_list = list(ohlcv_buffer[symbol])
                ohlcv_df = pd.DataFrame(ohlcv_list)
                if not ohlcv_df.empty and 't' in ohlcv_df.columns:
                    ohlcv_df['t'] = pd.to_datetime(ohlcv_df['t'])
                    ohlcv_df = ohlcv_df.sort_values(by='t')  # Ensure sorted
                else:
                    ohlcv_df = pd.DataFrame()

            if symbol in forecast_buffer and forecast_buffer[symbol]:
                forecast_raw_item = forecast_buffer[symbol][-1]
                forecast_item = {
                    "symbol": forecast_raw_item.get("symbol"),
                    "forecast_values": forecast_raw_item.get("forecast"),
                    "last_actual_time": pd.to_datetime(forecast_raw_item.get("window_end_time")),  # Adapt key
                    "bar_duration_seconds": forecast_raw_item.get("bar_duration_seconds", 1)
                }
        else:
            # --- Use Dummy Data Simulation --- (ensure these functions exist)
            # ohlcv_df = generate_dummy_ohlcv(symbol)
            # if not ohlcv_df.empty:
            #     last_actual_dt = ohlcv_df['t'].iloc[-1]
            #     forecast_item = generate_dummy_forecast(symbol, last_actual_dt)
            # For now, let's assume dummy functions if USE_LIVE_DATA is false
            # This part needs your dummy data functions from the previous examples
            if 'generate_dummy_ohlcv' in globals():  # Check if dummy functions are defined
                ohlcv_df = generate_dummy_ohlcv(symbol)
                if not ohlcv_df.empty and 'generate_dummy_forecast' in globals():
                    last_actual_dt = ohlcv_df['t'].iloc[-1]
                    forecast_item = generate_dummy_forecast(symbol, last_actual_dt)
            else:  # Fallback if dummy functions aren't present
                ohlcv_df = pd.DataFrame()
                forecast_item = None

        if ohlcv_df.empty:
            fig = create_empty_figure()
            # Update: create_empty_figure should add a title for clarity
            fig.update_layout(title_text=f"{symbol} - No Data")
            metrics_text_content = f"{symbol}: Waiting for data..."
        else:
            fig = create_coin_chart(symbol, ohlcv_df, forecast_item)
            metrics_text_content = generate_metrics_text(symbol, ohlcv_df, forecast_item)

        # Using dictionary IDs for components for better practice, though not strictly necessary
        # if this callback is the only one writing to 'dynamic-graphs-container'.
        graph_component_id = {'type': 'dynamic-graph', 'symbol': symbol}
        metrics_component_id = {'type': 'dynamic-metrics', 'symbol': symbol}

        coin_card_content = html.Div([
            dcc.Graph(id=graph_component_id, figure=fig),
            html.Div(id=metrics_component_id, children=metrics_text_content, className="glowy-green")
        ], className="graph-card")

        # Add to current row's children, controlling layout with dbc.Col
        # You can customize width, lg, xl based on how many you want per row.
        # For example, to aim for 3 per row on large screens:
        current_row_children.append(dbc.Col(coin_card_content, width=12, md=6, xl=4))

        # If you want to strictly limit to 3 per row and then start a new row:
        # if len(current_row_children) == MAX_COLS_PER_ROW_XL:
        #    dynamic_graph_components.append(dbc.Row(current_row_children, style={"marginBottom": "1vw"}))
        #    current_row_children = []

    # After the loop, if there are any remaining children for the current row, add them.
    # This simplified version just adds all dbc.Col to one dbc.Row, letting them wrap.
    # For more control (e.g., max 3 per row), you'd build dbc.Row components in the loop.
    if current_row_children:
        dynamic_graph_components.append(dbc.Row(current_row_children, style={"marginBottom": "1vw"}))

    # If you prefer to make multiple rows with a fixed number of columns (e.g. 3):
    # final_layout = []
    # for i in range(0, len(all_coin_cards_as_cols), MAX_COLS_PER_ROW_XL):
    #     row_cols = all_coin_cards_as_cols[i:i + MAX_COLS_PER_ROW_XL]
    #     final_layout.append(dbc.Row(row_cols, style={"marginBottom": "1vw"}))
    # return final_layout
    # The current_row_children logic above already puts them in dbc.Col,
    # so dynamic_graph_components will be a list of dbc.Row(s) if you use the MAX_COLS_PER_ROW logic,
    # or a list containing one dbc.Row with many dbc.Col(s) if you use the simpler wrapping.

    # Simpler approach: just return the list of dbc.Row(s) created.
    # The current version with append(dbc.Row(current_row_children...)) will make one row.
    # If you want multiple rows each having up to MAX_COLS_PER_ROW_XL:

    # Let's adjust to create multiple rows if needed:
    final_layout_rows = []
    all_coin_cards_prepared_cols = []  # This list will hold all the dbc.Col(...) items

    # (Re-run the loop above or store the dbc.Col items in a list first)
    # For simplicity, let's assume the previous loop populated `all_coin_cards_prepared_cols`
    # with `dbc.Col(coin_card_content, width=12, md=6, xl=4)` items.

    # Re-doing the card creation part slightly to fit multi-row logic:
    all_coin_cards_prepared_cols = []
    for symbol_upper in selected_symbols:
        symbol = symbol_upper
        # ... (data fetching logic as above for ohlcv_df, forecast_item, fig, metrics_text_content) ...
        # This is a copy of the data fetching & component creation from above
        # In a real app, you'd refactor this into a helper or structure it cleanly
        if USE_LIVE_DATA:
            if symbol in ohlcv_buffer and ohlcv_buffer[symbol]:  # Simplified data fetching part for brevity
                ohlcv_list = list(ohlcv_buffer[symbol]);
                ohlcv_df = pd.DataFrame(ohlcv_list)
                if not ohlcv_df.empty and 't' in ohlcv_df.columns:
                    ohlcv_df['t'] = pd.to_datetime(ohlcv_df['t']); ohlcv_df = ohlcv_df.sort_values(by='t')
                else:
                    ohlcv_df = pd.DataFrame()
            if symbol in forecast_buffer and forecast_buffer[symbol]:
                forecast_raw_item = forecast_buffer[symbol][-1]
                forecast_item = {"symbol": symbol, "forecast_values": forecast_raw_item.get("forecast"),
                                 "last_actual_time": pd.to_datetime(forecast_raw_item.get("window_end_time")),
                                 "bar_duration_seconds": 1}
            else:
                forecast_item = None  # Ensure it's defined
        else:  # Dummy data placeholder
            if 'generate_dummy_ohlcv' in globals():
                ohlcv_df = generate_dummy_ohlcv(symbol)
            else:
                ohlcv_df = pd.DataFrame()
            if not ohlcv_df.empty and 'generate_dummy_forecast' in globals():
                forecast_item = generate_dummy_forecast(symbol, ohlcv_df['t'].iloc[-1])
            else:
                forecast_item = None

        if ohlcv_df.empty:
            fig = create_empty_figure(); fig.update_layout(
                title_text=f"{symbol} - No Data"); metrics_text_content = f"{symbol}: Waiting for data..."
        else:
            fig = create_coin_chart(symbol, ohlcv_df, forecast_item); metrics_text_content = generate_metrics_text(
                symbol, ohlcv_df, forecast_item)

        graph_component_id = {'type': 'dynamic-graph', 'symbol': symbol}
        metrics_component_id = {'type': 'dynamic-metrics', 'symbol': symbol}
        coin_card_content = html.Div([dcc.Graph(id=graph_component_id, figure=fig),
                                      html.Div(id=metrics_component_id, children=metrics_text_content,
                                               className="glowy-green")], className="graph-card")
        all_coin_cards_prepared_cols.append(
            dbc.Col(coin_card_content, width=12, md=6, xl=4))  # md=6 for 2 cols on medium, xl=4 for 3 cols on XL

    # Now create rows from all_coin_cards_prepared_cols
    # This logic will put up to 3 cards (since xl=4, 12/4=3) per row.
    # If you have 5 cards, it will make one row of 3, and one row of 2.
    cols_per_row = 3  # Based on xl=4
    for i in range(0, len(all_coin_cards_prepared_cols), cols_per_row):
        row_children = all_coin_cards_prepared_cols[i: i + cols_per_row]
        final_layout_rows.append(dbc.Row(row_children, style={"marginBottom": "1vw"}))

    if not final_layout_rows and selected_symbols:  # If symbols selected but no cards (e.g. all fail)
        return [html.P("Could not generate graphs for selected symbols.", style={'textAlign': 'center'})]

    return final_layout_rows
