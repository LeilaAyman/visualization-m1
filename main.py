# ============================
# main.py — FINAL FIXED VERSION
# ============================

import os
import requests
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State
import re

# ============================================================
# SETTINGS
# ============================================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"


# ============================================================
# DOWNLOAD PARQUET ONE TIME
# ============================================================
def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset...")
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(2 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("✅ Download complete")
    else:
        print("👍 Using cached dataset:", LOCAL_PATH)

ensure_dataset()


# ============================================================
# CONNECT DUCKDB (NO FULL COPY INTO MEMORY)
# ============================================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE VIEW collisions AS
    SELECT
        *,

        -- Extract hour
        CASE 
            WHEN crash_time IS NULL THEN NULL
            ELSE TRY_CAST(SPLIT_PART(crash_time, ':', 1) AS INTEGER)
        END AS hour,

        -- Age groups
        CASE
            WHEN person_age IS NULL THEN 'Unknown'
            WHEN TRY_CAST(person_age AS INTEGER) < 18 THEN '0–17'
            WHEN TRY_CAST(person_age AS INTEGER) < 30 THEN '18–29'
            WHEN TRY_CAST(person_age AS INTEGER) < 45 THEN '30–44'
            WHEN TRY_CAST(person_age AS INTEGER) < 60 THEN '45–59'
            ELSE '60+'
        END AS person_age_group

    FROM read_parquet('{LOCAL_PATH}');
""")

def q(sql):
    return con.execute(sql).df()


# ============================================================
# LOAD FILTER VALUES
# ============================================================
boroughs = q("SELECT DISTINCT borough FROM collisions ORDER BY borough")["borough"]
years = q("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year")["crash_year"]
vehicles = q("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category")["vehicle_category"]
factors = q("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined")["contributing_factor_combined"]
injuries = q("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury")["person_injury"]


# ============================================================
# DASH APP
# ============================================================
app = Dash(__name__)
server = app.server


# ============================================================
# LAYOUT
# ============================================================
app.layout = html.Div(style={"minHeight": "6000px"}, children=[

    html.H1("NYC Vehicle Collisions Dashboard",
            style={"textAlign": "center"}),

    html.Div([
        html.Button("Clear All Filters", id="clear_filters",
                    style={"background": "#c0392b", "color": "white", "padding": "8px 18px"}),

        html.Button("Clear Search", id="clear_search",
                    style={"background": "#2980b9", "color": "white", "padding": "8px 18px", "marginLeft": "10px"})
    ], style={"textAlign": "center", "marginBottom": "15px"}),

    html.Div([
        html.Button("Generate Report", id="generate_report",
                    style={"background": "#27ae60", "color": "white",
                           "padding": "10px 20px", "fontWeight": "bold"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    html.Div([

        html.Div([
            html.Label("Borough"),
            dcc.Dropdown(id="borough_filter",
                         options=[{"label": b, "value": b} for b in boroughs],
                         multi=True)
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(id="year_filter",
                         options=[{"label": int(y), "value": int(y)} for y in years],
                         multi=True)
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(id="vehicle_filter",
                         options=[{"label": v, "value": v} for v in vehicles],
                         multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(id="factor_filter",
                         options=[{"label": f, "value": f} for f in factors],
                         multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(id="injury_filter",
                         options=[{"label": i, "value": i} for i in injuries],
                         multi=True)
        ], style={"width": "15%", "display": "inline-block"}),

    ]),

    html.Br(),

    html.Div([
        dcc.Input(id="search_box", type="text", debounce=True,
                  placeholder="Search (e.g., Manhattan 2022 pedestrian crashes)...",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    dcc.Graph(id="injury_trend_graph"),
    dcc.Graph(id="factor_bar_graph"),
    dcc.Graph(id="bar_person_graph"),
    dcc.Graph(id="line_day_graph"),
    dcc.Graph(id="vehicle_severeinjuries_graph"),
    dcc.Graph(id="gender_borough_graph"),
    dcc.Graph(id="user_hourly_injuries_graph"),
    dcc.Graph(id="user_severity_heatmap"),
    dcc.Graph(id="street_severity_graph"),
    dcc.Graph(id="age_group_graph"),
])


# ============================================================
# WHERE BUILDER — SEARCH FIX
# ============================================================
def build_where(borough, year, vehicle, factor, injury, query):
    filters = []

    if borough:
        filters.append(f"borough IN ({','.join([repr(b) for b in borough])})")

    if year:
        filters.append(f"crash_year IN ({','.join(map(str, year))})")

    if vehicle:
        filters.append(f"vehicle_category IN ({','.join([repr(v) for v in vehicle])})")

    if factor:
        filters.append(f"contributing_factor_combined IN ({','.join([repr(f) for f in factor])})")

    if injury:
        filters.append(f"person_injury IN ({','.join([repr(i) for i in injury])})")

    # ------------------------------
    # FIXED SEARCH
    # ------------------------------
    if query:
        q = query.lower().replace("'", "''")

        filters.append(f"""
            (
                LOWER(borough) LIKE '%{q}%'
                OR LOWER(on_street_name) LIKE '%{q}%'
                OR LOWER(vehicle_category) LIKE '%{q}%'
                OR LOWER(person_type) LIKE '%{q}%'
                OR LOWER(person_injury) LIKE '%{q}%'
                OR LOWER(contributing_factor_combined) LIKE '%{q}%'
                OR LOWER(person_age_group) LIKE '%{q}%'
            )
        """)

        # Detect year inside search
        year_matches = re.findall(r"\b(20\\d{2})\b", q)
        if year_matches:
            filters.append("crash_year IN (" + ",".join(year_matches) + ")")

    return ("WHERE " + " AND ".join(filters)) if filters else ""


# ============================================================
# CALLBACK
# ============================================================
@app.callback(
    [
        Output("injury_trend_graph", "figure"),
        Output("factor_bar_graph", "figure"),
        Output("bar_person_graph", "figure"),
        Output("line_day_graph", "figure"),
        Output("vehicle_severeinjuries_graph", "figure"),
        Output("gender_borough_graph", "figure"),
        Output("user_hourly_injuries_graph", "figure"),
        Output("user_severity_heatmap", "figure"),
        Output("street_severity_graph", "figure"),
        Output("age_group_graph", "figure"),
    ],
    Input("generate_report", "n_clicks"),
    [
        State("borough_filter", "value"),
        State("year_filter", "value"),
        State("vehicle_filter", "value"),
        State("factor_filter", "value"),
        State("injury_filter", "value"),
        State("search_box", "value"),
    ],
)
def update(_, borough, year, vehicle, factor, injury, query):

    where = build_where(borough, year, vehicle, factor, injury, query)

    # --------------------------------------------------------
    # GRAPH 1
    # --------------------------------------------------------
    df1 = q(f"""
        SELECT crash_year, borough,
               SUM(number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured) AS total_injuries
        FROM collisions {where}
        GROUP BY crash_year, borough
        ORDER BY crash_year
    """)
    fig1 = px.line(df1, x="crash_year", y="total_injuries", color="borough",
                   title="Total Injuries Trend by Year and Borough")
    fig1.update_traces(mode="lines+markers", marker=dict(size=10))
    fig1.update_xaxes(type="category")

    # --------------------------------------------------------
    # GRAPH 2
    # --------------------------------------------------------
    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor
        ORDER BY count DESC
        LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h",
                  title="Top 10 Contributing Factors")

    # --------------------------------------------------------
    # GRAPH 3
    # --------------------------------------------------------
    person_col = "total_injuries"

    if query:
        q_lower = query.lower()
        if "pedestrian" in q_lower:
            person_col = "number_of_pedestrians_injured"
        elif "cyclist" in q_lower:
            person_col = "number_of_cyclist_injured"
        elif "motorist" in q_lower:
            person_col = "number_of_motorist_injured"

    df3 = q(f"""
        SELECT borough,
               SUM(
                   CASE
                       WHEN '{person_col}' = 'total_injuries' THEN
                           number_of_pedestrians_injured +
                           number_of_cyclist_injured +
                           number_of_motorist_injured
                       WHEN '{person_col}' = 'number_of_pedestrians_injured' THEN
                           number_of_pedestrians_injured
                       WHEN '{person_col}' = 'number_of_cyclist_injured' THEN
                           number_of_cyclist_injured
                       WHEN '{person_col}' = 'number_of_motorist_injured' THEN
                           number_of_motorist_injured
                   END
               ) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)

    fig3 = px.bar(df3, x="borough", y="injuries",
                  title=f"Injuries by Borough ({person_col})")

    # --------------------------------------------------------
    # GRAPH 4
    # --------------------------------------------------------
    df4 = q(f"""
        SELECT crash_day_of_week, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week
        ORDER BY crash_day_of_week
    """)
    df4["day_name"] = df4["crash_day_of_week"].map({
        0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun",-1:"Unknown"})
    fig4 = px.line(df4, x="day_name", y="crashes", title="Crashes by Day of Week")

    # --------------------------------------------------------
    # GRAPH 5 — FIXED (correct summation!)
    # --------------------------------------------------------
    df5 = q(f"""
        SELECT vehicle_category,
               SUM(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5 * (
                       number_of_pedestrians_killed +
                       number_of_cyclist_killed +
                       number_of_motorist_killed
                   )
               ) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity",
                  title="Severity Score by Vehicle Category")

    # --------------------------------------------------------
    # GRAPH 6
    # --------------------------------------------------------
    df6 = q(f"""
        SELECT borough, person_sex, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY borough, person_sex
    """)
    fig6 = px.bar(df6, x="borough", y="count", color="person_sex",
                  barmode="group", title="Injuries by Borough and Gender")

    # --------------------------------------------------------
    # GRAPH 7
    # --------------------------------------------------------
    df7 = q(f"""
        SELECT hour,
            AVG(number_of_pedestrians_injured) AS ped,
            AVG(number_of_cyclist_injured) AS cyc,
            AVG(number_of_motorist_injured) AS mot
        FROM collisions {where}
        GROUP BY hour
        ORDER BY hour
    """)
    melted = df7.melt(id_vars="hour", var_name="type", value_name="avg")
    melted["type"] = melted["type"].map({
        "ped":"Pedestrian", "cyc":"Cyclist", "mot":"Motorist"})
    fig7 = px.line(melted, x="hour", y="avg", color="type",
                   title="Average Hourly Injuries by User Type")

    # --------------------------------------------------------
    # GRAPH 8
    # --------------------------------------------------------
    df8 = q(f"""
        SELECT vehicle_category, hour,
               AVG(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5 * (
                       number_of_pedestrians_killed +
                       number_of_cyclist_killed +
                       number_of_motorist_killed
                   )
               ) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category, hour
    """)
    fig8 = px.imshow(
        df8.pivot(index="vehicle_category", columns="hour", values="severity").fillna(0),
        title="Heatmap of Crash Severity by Vehicle Category and Hour"
    )

    # --------------------------------------------------------
    # GRAPH 9
    # --------------------------------------------------------
    df9 = q(f"""
        SELECT on_street_name,
               SUM(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5 * (
                       number_of_pedestrians_killed +
                       number_of_cyclist_killed +
                       number_of_motorist_killed
                   )
               ) AS severity
        FROM collisions {where}
        GROUP BY on_street_name
        ORDER BY severity DESC
        LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="severity",
                  title="Top 15 Streets by Severity")

    # --------------------------------------------------------
    # GRAPH 10 — AGE GROUP FIXED
    # --------------------------------------------------------
    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type",
                   title="Age Group vs Injury Severity by Person Type")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


# ============================================================
# CLEAR BUTTONS
# ============================================================
@app.callback(
    [
        Output("borough_filter", "value"),
        Output("year_filter", "value"),
        Output("vehicle_filter", "value"),
        Output("factor_filter", "value"),
        Output("injury_filter", "value"),
    ],
    Input("clear_filters", "n_clicks"),
    prevent_initial_call=True
)
def reset_filters(_):
    return None, None, None, None, None


@app.callback(
    Output("search_box", "value"),
    Input("clear_search", "n_clicks"),
    prevent_initial_call=True
)
def reset_search(_):
    return ""


# ============================================================
# RUN SERVER — RENDER / FLY.IO
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
