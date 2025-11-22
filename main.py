# ================================
# main.py — FINAL RENDER VERSION
# FULL SEARCH + MEMORY SAFE + 10 VISUALS
# ================================

import os
import re
import requests
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State

# =========================================================
# DATASET SETTINGS
# =========================================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"

# =========================================================
# DOWNLOAD DATASET ONCE
# =========================================================
def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset…")
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("✅ Download complete")
    else:
        print("👍 Using cached dataset")

ensure_dataset()

# =========================================================
# DUCKDB CONNECTION + VIEW
# =========================================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE TABLE collisions_raw AS
    SELECT * FROM read_parquet('{LOCAL_PATH}');
""")

con.execute("""
CREATE VIEW collisions AS
SELECT
    *,
    CASE
        WHEN crash_time IS NULL THEN NULL
        ELSE TRY_CAST(SPLIT_PART(crash_time, ':', 1) AS INTEGER)
    END AS hour,
    (
        COALESCE(number_of_persons_injured, 0) +
        COALESCE(number_of_pedestrians_injured, 0) +
        COALESCE(number_of_cyclist_injured, 0) +
        COALESCE(number_of_motorist_injured, 0)
    ) AS total_injuries,
    (
        COALESCE(number_of_persons_injured, 0) +
        COALESCE(number_of_pedestrians_injured, 0) +
        COALESCE(number_of_cyclist_injured, 0) +
        COALESCE(number_of_motorist_injured, 0)
        +
        5 * (
            COALESCE(number_of_persons_killed, 0) +
            COALESCE(number_of_pedestrians_killed, 0) +
            COALESCE(number_of_cyclist_killed, 0) +
            COALESCE(number_of_motorist_killed, 0)
        )
    ) AS severity,
    CASE
        WHEN person_age IS NULL THEN 'Unknown'
        WHEN TRY_CAST(person_age AS INTEGER) < 18 THEN '0–17'
        WHEN TRY_CAST(person_age AS INTEGER) < 30 THEN '18–29'
        WHEN TRY_CAST(person_age AS INTEGER) < 45 THEN '30–44'
        WHEN TRY_CAST(person_age AS INTEGER) < 60 THEN '45–59'
        ELSE '60+'
    END AS person_age_group
FROM collisions_raw;
""")

def q(sql):
    return con.execute(sql).df()

# =========================================================
# PRELOAD DROPDOWN VALUES
# =========================================================
boroughs = q("SELECT DISTINCT borough FROM collisions ORDER BY borough")["borough"].fillna("UNKNOWN").tolist()
years = q("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year")["crash_year"].tolist()
vehicles = q("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category")["vehicle_category"].tolist()
factors = q("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined")["contributing_factor_combined"].tolist()
injuries = q("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury")["person_injury"].tolist()

# =========================================================
# DASH APP
# =========================================================
app = Dash(__name__)
server = app.server

app.layout = html.Div(style={"minHeight": "6000px"}, children=[

    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    html.Div([
        html.Button("Clear All Filters", id="clear_filters",
                    style={"background": "#c0392b", "color": "white", "padding": "8px 18px"}),
        html.Button("Clear Search", id="clear_search",
                    style={"background": "#2980b9", "color": "white", "padding": "8px 18px",
                           "marginLeft": "10px"})
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
                options=[{"label": b, "value": b} for b in boroughs], multi=True)
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(id="year_filter",
                options=[{"label": int(y), "value": int(y)} for y in years], multi=True)
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(id="vehicle_filter",
                options=[{"label": v, "value": v} for v in vehicles], multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(id="factor_filter",
                options=[{"label": f, "value": f} for f in factors], multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(id="injury_filter",
                options=[{"label": i, "value": i} for i in injuries], multi=True)
        ], style={"width": "15%", "display": "inline-block"}),
    ]),

    html.Br(),

    html.Div([
        dcc.Input(id="search_box", type="text", debounce=True,
                  placeholder="Search (e.g., Brooklyn 2020 cyclist)…",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # 10 GRAPHS
    *[dcc.Graph(id=f"g{i}") for i in range(1, 11)]
])

# =========================================================
# WHERE BUILDER (FULL SEARCH)
# =========================================================
def build_where(borough, year, vehicle, factor, injury, search):
    filters = []

    if borough:
        filters.append("borough IN (" + ",".join(repr(b) for b in borough) + ")")
    if year:
        filters.append("crash_year IN (" + ",".join(str(y) for y in year) + ")")
    if vehicle:
        filters.append("vehicle_category IN (" + ",".join(repr(v) for v in vehicle) + ")")
    if factor:
        filters.append("contributing_factor_combined IN (" + ",".join(repr(f) for f in factor) + ")")
    if injury:
        filters.append("person_injury IN (" + ",".join(repr(i) for i in injury) + ")")

    if search:
        s = search.lower().replace("'", "''")

        # Try year tokens
        year_tokens = re.findall(r"\b(20\d{2})\b", s)
        if year_tokens:
            filters.append("crash_year IN (" + ",".join(year_tokens) + ")")

        # Try borough tokens
        borough_tokens = [b for b in boroughs if b and b.lower() in s]
        if borough_tokens:
            filters.append("borough IN (" + ",".join(repr(b) for b in borough_tokens) + ")")

        # Generic search match
        filters.append(
            f"""
            (
                LOWER(borough) LIKE '%{s}%'
                OR LOWER(on_street_name) LIKE '%{s}%'
                OR LOWER(vehicle_category) LIKE '%{s}%'
                OR LOWER(person_type) LIKE '%{s}%'
                OR LOWER(person_injury) LIKE '%{s}%'
                OR LOWER(contributing_factor_combined) LIKE '%{s}%'
            )
            """
        )

    return "WHERE " + " AND ".join(filters) if filters else ""

# =========================================================
# LIMIT HEAVY QUERIES (memory safe)
# =========================================================
def where_light(where_clause, year_filter):
    if year_filter:
        return where_clause
    if not where_clause.strip():
        return "WHERE crash_year >= 2018"
    return where_clause + " AND crash_year >= 2018"

# =========================================================
# MAIN CALLBACK — 10 VISUALS
# =========================================================
@app.callback(
    [Output(f"g{i}", "figure") for i in range(1, 11)],
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
def update(n, borough, year, vehicle, factor, injury, search):

    where = build_where(borough, year, vehicle, factor, injury, search)
    light = where_light(where, year)

    # ============= VIS 1 =============
    df1 = q(f"""
        SELECT crash_year, borough, SUM(total_injuries) AS total_injuries
        FROM collisions {where}
        GROUP BY crash_year, borough
        ORDER BY crash_year
    """)
    fig1 = px.line(df1, x="crash_year", y="total_injuries", color="borough",
                   title="Total Injuries Trend by Year & Borough")
    fig1.update_traces(mode="lines+markers", marker=dict(size=9))

    # ============= VIS 2 =============
    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor
        ORDER BY count DESC
        LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h",
                  title="Top 10 Contributing Factors")

    # ============= VIS 3 =============
    df3 = q(f"""
        SELECT borough, SUM(total_injuries) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)
    fig3 = px.bar(df3, x="borough", y="injuries",
                  title="Total Injuries by Borough")

    # ============= VIS 4 =============
    df4 = q(f"""
        SELECT crash_day_of_week, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week
        ORDER BY crash_day_of_week
    """)
    df4["day"] = df4["crash_day_of_week"].map({
        0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun",-1:"Unknown"
    })
    fig4 = px.line(df4, x="day", y="crashes",
                   title="Crashes by Day of Week")

    # ============= VIS 5 =============
    df5 = q(f"""
        SELECT vehicle_category, SUM(severity) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity",
                  title="Severity Score by Vehicle Category")

    # ============= VIS 6 =============
    df6 = q(f"""
        SELECT borough, person_sex, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY borough, person_sex
    """)
    fig6 = px.bar(df6, x="borough", y="crashes", color="person_sex",
                  barmode="group",
                  title="Crashes by Borough & Gender")

    # ============= VIS 7 =============
    df7 = q(f"""
        SELECT hour,
            AVG(number_of_pedestrians_injured) AS ped,
            AVG(number_of_cyclist_injured) AS cyc,
            AVG(number_of_motorist_injured) AS mot
        FROM collisions {light}
        GROUP BY hour
        ORDER BY hour
    """)
    melted = df7.melt(id_vars="hour", var_name="type", value_name="avg_injuries")
    melted["type"] = melted["type"].map({
        "ped":"Pedestrian","cyc":"Cyclist","mot":"Motorist"
    })
    fig7 = px.line(melted, x="hour", y="avg_injuries", color="type",
                   title="Average Hourly Injuries by User Type")

    # ============= VIS 8 =============
    df8 = q(f"""
        SELECT vehicle_category, hour, AVG(severity) AS avg_sev
        FROM collisions {light}
        GROUP BY vehicle_category, hour
    """)
    if df8.empty:
        fig8 = px.imshow([[0]], title="Severity Heatmap")
    else:
        pivot = df8.pivot(index="vehicle_category", columns="hour", values="avg_sev").fillna(0)
        fig8 = px.imshow(pivot, title="Crash Severity Heatmap")

    # ============= VIS 9 =============
    df9 = q(f"""
        WITH base AS (
            SELECT on_street_name, severity
            FROM collisions {light}
            WHERE on_street_name IS NOT NULL
            LIMIT 200000
        )
        SELECT on_street_name, SUM(severity) AS total_severity
        FROM base
        GROUP BY on_street_name
        ORDER BY total_severity DESC
        LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="total_severity",
                  title="Top 15 Streets by Severity")

    # ============= VIS 10 =============
    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {light}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type",
                   title="Age Group vs Injury Severity")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10

# =========================================================
# CLEAR FILTER BUTTONS
# =========================================================
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
def reset_filters(n):
    return None, None, None, None, None

@app.callback(
    Output("search_box", "value"),
    Input("clear_search", "n_clicks"),
    prevent_initial_call=True
)
def reset_search(n):
    return ""

# =========================================================
# RUN SERVER
# =========================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
