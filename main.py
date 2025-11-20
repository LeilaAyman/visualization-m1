# main.py
# Fully live NYC Collision Dashboard with debounced search + clear buttons

import re
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output

# ==============================
# LOAD & BASIC PREP
# ==============================
df = pd.read_csv(r"C:\Users\maria\Downloads\visulaization\final_cleaned.csv")

# Basic standardization (light, no Nan → 'OTHER' changes, as you asked)
df["borough"] = df["borough"].astype(str).str.upper().str.strip()
df["person_injury"] = df["person_injury"].astype(str).str.upper().str.strip()
df["vehicle_type_code_1"] = df["vehicle_type_code_1"].astype(str).str.upper().str.strip()
df["contributing_factor_vehicle_1"] = df["contributing_factor_vehicle_1"].astype(str).str.upper().str.strip()
df["person_sex"] = df["person_sex"].astype(str).str.upper().str.strip()

df["crash_year"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.year
df["crash_day_of_week"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.dayofweek
df["crash_day_of_week"] = df["crash_day_of_week"].fillna(-1).astype(int)

# Precompute columns that are always needed (save work inside callback)
df["total_injuries"] = (
    df["number_of_persons_injured"]
    + df["number_of_pedestrians_injured"]
    + df["number_of_cyclist_injured"]
    + df["number_of_motorist_injured"]
)

df["severe_injuries"] = (
    df["number_of_persons_killed"]
    + df["number_of_pedestrians_killed"]
    + df["number_of_cyclist_killed"]
    + df["number_of_motorist_killed"]
)

# ==============================
# APP INIT
# ==============================
app = Dash(__name__)

# ==============================
# LAYOUT
# ==============================
app.layout = html.Div(
    [
        html.H1(
            "NYC Motor Vehicle Collisions – Interactive Dashboard",
            style={
                "textAlign": "center",
                "marginBottom": "30px",
                "fontFamily": "Arial",
            },
        ),

        # ---- ACTION BUTTONS ----
        html.Div(
            [
                html.Button(
                    "Clear All Filters",
                    id="clear_filters",
                    n_clicks=0,
                    style={
                        "background": "#c0392b",
                        "color": "white",
                        "padding": "8px 18px",
                        "marginRight": "15px",
                        "border": "none",
                        "borderRadius": "4px",
                        "cursor": "pointer",
                        "fontWeight": "bold",
                    },
                ),
                html.Button(
                    "Clear Search",
                    id="clear_search",
                    n_clicks=0,
                    style={
                        "background": "#2980b9",
                        "color": "white",
                        "padding": "8px 18px",
                        "border": "none",
                        "borderRadius": "4px",
                        "cursor": "pointer",
                        "fontWeight": "bold",
                    },
                ),
            ],
            style={"textAlign": "center", "marginBottom": "20px"},
        ),

        # ---- FILTER DROPDOWNS ----
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Borough"),
                        dcc.Dropdown(
                            id="borough_filter",
                            options=[
                                {"label": b.title(), "value": b}
                                for b in sorted(df["borough"].dropna().unique())
                            ],
                            multi=True,
                            placeholder="Select boroughs",
                        ),
                    ],
                    style={"width": "20%", "display": "inline-block", "padding": "0 5px"},
                ),
                html.Div(
                    [
                        html.Label("Year"),
                        dcc.Dropdown(
                            id="year_filter",
                            options=[
                                {"label": int(y), "value": int(y)}
                                for y in sorted(df["crash_year"].dropna().unique())
                            ],
                            multi=True,
                            placeholder="Select years",
                        ),
                    ],
                    style={"width": "15%", "display": "inline-block", "padding": "0 5px"},
                ),
                html.Div(
                    [
                        html.Label("Vehicle Type"),
                        dcc.Dropdown(
                            id="vehicle_filter",
                            options=[
                                {"label": v, "value": v}
                                for v in sorted(df["vehicle_type_code_1"].dropna().unique())
                            ],
                            multi=True,
                            placeholder="Select vehicle types",
                        ),
                    ],
                    style={"width": "22%", "display": "inline-block", "padding": "0 5px"},
                ),
                html.Div(
                    [
                        html.Label("Contributing Factor"),
                        dcc.Dropdown(
                            id="factor_filter",
                            options=[
                                {"label": f, "value": f}
                                for f in sorted(
                                    df["contributing_factor_vehicle_1"].dropna().unique()
                                )
                            ],
                            multi=True,
                            placeholder="Select factors",
                        ),
                    ],
                    style={"width": "25%", "display": "inline-block", "padding": "0 5px"},
                ),
                html.Div(
                    [
                        html.Label("Injury Type"),
                        dcc.Dropdown(
                            id="injury_filter",
                            options=[
                                {"label": i, "value": i}
                                for i in sorted(df["person_injury"].dropna().unique())
                            ],
                            multi=True,
                            placeholder="Select injury types",
                        ),
                    ],
                    style={"width": "15%", "display": "inline-block", "padding": "0 5px"},
                ),
            ]
        ),

        html.Br(),

        # ---- SEARCH BOX (DEBOUNCED) ----
        html.Div(
            [
                dcc.Input(
                    id="search_box",
                    type="text",
                    debounce=True,  # <-- only fires when Enter or focus-out
                    placeholder="Search (e.g., Brooklyn 2022 pedestrian crashes)...",
                    style={"width": "60%", "height": "40px", "fontSize": "14px"},
                ),
                html.Div(
                    "Tip: Search is applied only when you press Enter.",
                    style={"marginTop": "8px", "fontSize": "12px", "color": "#555"},
                ),
            ],
            style={"textAlign": "center", "marginBottom": "30px"},
        ),

        # ---- GRAPHS ----
        dcc.Graph(id="injury_trend_graph"),
        dcc.Graph(id="factor_bar_graph"),
        dcc.Graph(id="bar_person_graph"),
        dcc.Graph(id="line_day_graph"),
        dcc.Graph(id="vehicle_severeinjuries_graph"),
        dcc.Graph(id="gender_borough_graph"),
    ]
)

# ==============================
# MAIN CALLBACK: FILTERS + SEARCH
# ==============================
@app.callback(
    [
        Output("injury_trend_graph", "figure"),
        Output("factor_bar_graph", "figure"),
        Output("bar_person_graph", "figure"),
        Output("line_day_graph", "figure"),
        Output("vehicle_severeinjuries_graph", "figure"),
        Output("gender_borough_graph", "figure"),
    ],
    [
        Input("borough_filter", "value"),
        Input("year_filter", "value"),
        Input("vehicle_filter", "value"),
        Input("factor_filter", "value"),
        Input("injury_filter", "value"),
        Input("search_box", "value"),  # debounced
    ],
)
def update_all(borough, year, vehicle, factor, injury, query):
    dff = df.copy()
    person_column = "number_of_cyclist_injured"

    # --- APPLY DROPDOWN FILTERS ---
    if borough:
        dff = dff[dff["borough"].isin(borough)]
    if year:
        dff = dff[dff["crash_year"].isin(year)]
    if vehicle:
        dff = dff[dff["vehicle_type_code_1"].isin(vehicle)]
    if factor:
        dff = dff[dff["contributing_factor_vehicle_1"].isin(factor)]
    if injury:
        dff = dff[dff["person_injury"].isin(injury)]

    # --- APPLY SEARCH (ONLY IF LENGTH >= 3) ---
    if query and len(query.strip()) >= 3:
        q = query.lower()

        # Borough in text
        for b in ["bronx", "brooklyn", "manhattan", "queens", "staten island"]:
            if b in q:
                dff = dff[dff["borough"] == b.upper()]

        # Year in text
        years_found = re.findall(r"\b(20\d{2})\b", q)
        if years_found:
            years_int = [int(y) for y in years_found]
            dff = dff[dff["crash_year"].isin(years_int)]

        # Person type to choose metrics
        mapping = {
            "pedestrian": "number_of_pedestrians_injured",
            "cyclist": "number_of_cyclist_injured",
            "motorist": "number_of_motorist_injured",
        }
        for key, col in mapping.items():
            if key in q:
                person_column = col

    # --- If no data → return empty figs instead of crashing ---
    if dff.empty:
        empty_fig = px.scatter(title="No data available for the selected filters/search.")
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig

    # ===================== 1) Injury trends =====================
    trend = (
        dff.groupby(["crash_year", "borough"])["total_injuries"]
        .sum()
        .reset_index()
    )
    fig1 = px.line(
        trend,
        x="crash_year",
        y="total_injuries",
        color="borough",
        markers=True,
        title="Total Injuries per Year Across Boroughs",
    )

    # ===================== 2) Top contributing factors =====================
    fac = (
        dff["contributing_factor_vehicle_1"]
        .value_counts()
        .head(10)
        .reset_index()
    )
    fac.columns = ["factor", "count"]
    fig2 = px.bar(
        fac,
        x="count",
        y="factor",
        orientation="h",
        title="Top 10 Contributing Factors",
    )

    # ===================== 3) Injuries by borough =====================
    bar_df = dff.groupby("borough")[person_column].sum().reset_index()
    fig3 = px.bar(
        bar_df,
        x="borough",
        y=person_column,
        title="Injuries by Borough",
    )

    # ===================== 4) Crashes by day =====================
    day_df = dff.groupby("crash_day_of_week").size().reset_index(name="crashes")
    day_df["day_name"] = day_df["crash_day_of_week"].map(
        {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun", -1: "Unknown"}
    )
    fig4 = px.line(
        day_df,
        x="day_name",
        y="crashes",
        markers=True,
        title="Crashes by Day of Week",
    )

    # ===================== 5) Severe injuries by vehicle type =====================
    vdf = (
        dff.groupby("vehicle_type_code_1")["severe_injuries"]
        .sum()
        .reset_index()
        .sort_values("severe_injuries", ascending=False)
        .head(10)
    )
    fig5 = px.bar(
        vdf,
        x="vehicle_type_code_1",
        y="severe_injuries",
        title="Vehicle Types with Highest Severe Injuries",
    )

    # ===================== 6) Gender vs Borough =====================
    gdf = (
        dff.groupby(["borough", "person_sex"])
        .size()
        .reset_index(name="count")
    )
    fig6 = px.bar(
        gdf,
        x="borough",
        y="count",
        color="person_sex",
        barmode="group",
        title="Crash Frequency by Gender Across Boroughs",
    )

    return fig1, fig2, fig3, fig4, fig5, fig6


# ==============================
# CLEAR BUTTON CALLBACKS
# ==============================

@app.callback(
    [
        Output("borough_filter", "value"),
        Output("year_filter", "value"),
        Output("vehicle_filter", "value"),
        Output("factor_filter", "value"),
        Output("injury_filter", "value"),
    ],
    Input("clear_filters", "n_clicks"),
    prevent_initial_call=True,
)
def clear_all_filters(n_clicks):
    # reset all dropdowns
    return None, None, None, None, None


@app.callback(
    Output("search_box", "value"),
    Input("clear_search", "n_clicks"),
    prevent_initial_call=True,
)
def clear_search_value(n_clicks):
    # reset search text
    return ""


# ==============================
# RUN APP
# ==============================
if __name__ == "__main__":
    app.run_server(debug=True)
