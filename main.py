# ============================================================
# main.py — MULTI-KEYWORD SMART SEARCH (FINAL VERSION)
# ============================================================

import os
import requests
import re
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State


PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"


# ============================================================
# DOWNLOAD DATASET ONCE
# ============================================================
def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset...")
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("✅ Download complete")
    else:
        print("👍 Using cached dataset:", LOCAL_PATH)


ensure_dataset()


# ============================================================
# DUCKDB VIEW
# ============================================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE VIEW collisions AS
    SELECT *,
        CASE 
            WHEN crash_time IS NULL THEN NULL
            ELSE TRY_CAST(SPLIT_PART(crash_time, ':', 1) AS INTEGER)
        END AS hour,
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
# DROPDOWNS
# ============================================================
boroughs = q("SELECT DISTINCT borough FROM collisions ORDER BY borough")["borough"]
years = q("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year")["crash_year"]
vehicles = q("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category")["vehicle_category"]
factors = q("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined")["contributing_factor_combined"]
injuries = q("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury")["person_injury"]


# ============================================================
# INTELLIGENT MULTI-KEYWORD SEARCH ENGINE
# ============================================================
def parse_keywords(text):
    """
    Extracts boroughs, years, vehicle types, injury keywords, person type,
    weekday names, factors, and leftover free-text.
    """

    if not text or text.strip() == "":
        return {}

    text = text.lower()

    tokens = text.split()
    results = {
        "boroughs": [],
        "years": [],
        "vehicles": [],
        "injuries": [],
        "person_types": [],
        "days": [],
        "factors": [],
        "free": []
    }

    borough_map = [b.lower() for b in boroughs]
    vehicle_map = [v.lower() for v in vehicles]
    injury_map = [i.lower() for i in injuries]
    factor_map = [f.lower() for f in factors]

    weekdays = {
        "monday": 0, "mon": 0,
        "tuesday": 1, "tue": 1,
        "wednesday": 2, "wed": 2,
        "thursday": 3, "thu": 3,
        "friday": 4, "fri": 4,
        "saturday": 5, "sat": 5,
        "sunday": 6, "sun": 6
    }

    person_types = ["pedestrian", "cyclist", "motorist"]

    for t in tokens:

        # Year (regex)
        if re.match(r"20\d{2}", t):
            results["years"].append(int(t))
            continue

        # Borough
        if t in borough_map:
            results["boroughs"].append(t.upper())
            continue

        # Vehicle type
        if t in vehicle_map:
            results["vehicles"].append(t.upper())
            continue

        # Injury terms
        if t in injury_map:
            results["injuries"].append(t.upper())
            continue

        # Person types
        if t in person_types:
            results["person_types"].append(t.title())
            continue

        # Day of week
        if t in weekdays:
            results["days"].append(weekdays[t])
            continue

        # Factor keywords
        if t in factor_map:
            results["factors"].append(t.upper())
            continue

        # LEFTOVER free-text
        results["free"].append(t)

    return results


# ============================================================
# SQL BUILDER
# ============================================================
def build_where(filters, dropdowns):
    where = []

    # -------------------------------------------
    # 1. DROPDOWN FILTERS
    # -------------------------------------------
    borough, year, vehicle, factor, injury = dropdowns

    if borough:
        where.append(f"borough IN ({','.join([repr(b) for b in borough])})")

    if year:
        where.append(f"crash_year IN ({','.join(map(str, year))})")

    if vehicle:
        where.append(f"vehicle_category IN ({','.join([repr(v) for v in vehicle])})")

    if factor:
        where.append(f"contributing_factor_combined IN ({','.join([repr(f) for f in factor])})")

    if injury:
        where.append(f"person_injury IN ({','.join([repr(i) for i in injury])})")

    # -------------------------------------------
    # 2. KEYWORD FILTERS
    # -------------------------------------------
    # Boroughs
    if filters["boroughs"]:
        b_list = [repr(b.upper()) for b in filters["boroughs"]]
        where.append(f"borough IN ({','.join(b_list)})")

    # Years
    if filters["years"]:
        where.append(f"crash_year IN ({','.join(map(str, filters['years']))})")

    # Vehicle types
    if filters["vehicles"]:
        v_list = [repr(v.upper()) for v in filters["vehicles"]]
        where.append(f"vehicle_category IN ({','.join(v_list)})")

    # Injury keywords
    if filters["injuries"]:
        i_list = [repr(i.upper()) for i in filters["injuries"]]
        where.append(f"person_injury IN ({','.join(i_list)})")

    # Person types
    if filters["person_types"]:
        p_list = [repr(p.title()) for p in filters["person_types"]]
        where.append(f"person_type IN ({','.join(p_list)})")

    # Days
    if filters["days"]:
        where.append(f"crash_day_of_week IN ({','.join(map(str, filters['days']))})")

    # Factors
    if filters["factors"]:
        f_list = [repr(f.upper()) for f in filters["factors"]]
        where.append(f"contributing_factor_combined IN ({','.join(f_list)})")

    # Free-text fallback
    if filters["free"]:
        free = "%".join(filters["free"])
        clause = f"""
            (
                LOWER(on_street_name) LIKE '%{free}%'
                OR LOWER(vehicle_category) LIKE '%{free}%'
                OR LOWER(contributing_factor_combined) LIKE '%{free}%'
                OR LOWER(person_type) LIKE '%{free}%'
            )
        """
        where.append(clause)

    return ("WHERE " + " AND ".join(where)) if where else ""


# ============================================================
# DASH APP LAYOUT
# ============================================================
app = Dash(__name__)
server = app.server

app.layout = html.Div([

    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    html.Div([
        html.Button("Clear All Filters", id="clear_filters",
                    style={"background": "#c0392b", "color": "white"}),
        html.Button("Clear Search", id="clear_search",
                    style={"background": "#2980b9", "color": "white", "marginLeft": "10px"}),
    ], style={"textAlign": "center", "marginBottom": "15px"}),

    html.Div([
        dcc.Input(id="search_box", type="text", debounce=True,
                  placeholder="Search anything (Brooklyn 2022 cyclist night factor alcohol)...",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    dcc.Graph(id="injury_trend_graph"),
    dcc.Graph(id="factor_bar_graph"),
    dcc.Graph(id="bar_person_graph"),
    dcc.Graph(id="line_day_graph"),
    dcc.Graph(id="vehicle_severeinjuries_graph"),
    dcc.Graph(id="gender_counts_graph"),
    dcc.Graph(id="user_hourly_injuries_graph"),
    dcc.Graph(id="user_severity_heatmap"),
    dcc.Graph(id="street_severity_graph"),
    dcc.Graph(id="age_group_graph"),

])


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
        Output("gender_counts_graph", "figure"),
        Output("user_hourly_injuries_graph", "figure"),
        Output("user_severity_heatmap", "figure"),
        Output("street_severity_graph", "figure"),
        Output("age_group_graph", "figure"),
    ],
    Input("search_box", "value"),
    [
        State("borough_filter", "value"),
        State("year_filter", "value"),
        State("vehicle_filter", "value"),
        State("factor_filter", "value"),
        State("injury_filter", "value"),
    ]
)
def update(search, borough, year, vehicle, factor, injury):

    parsed = parse_keywords(search)
    where = build_where(parsed, (borough, year, vehicle, factor, injury))

    # =======================================================
    # GENERATE ALL 10 GRAPHS (same as previous version)
    # =======================================================

    df1 = q(f"""
        SELECT crash_year, borough,
               SUM(number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured) AS total_injuries
        FROM collisions {where}
        GROUP BY crash_year, borough
    """)
    fig1 = px.line(df1, x="crash_year", y="total_injuries", color="borough",
                   title="Injuries Trend")

    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor ORDER BY count DESC LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h",
                  title="Top Contributing Factors")

    df3 = q(f"""
        SELECT borough,
               SUM(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured
               ) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)
    fig3 = px.bar(df3, x="borough", y="injuries", title="Injuries by Borough")

    df4 = q(f"""
        SELECT crash_day_of_week, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week
    """)
    fig4 = px.line(df4, x="crash_day_of_week", y="crashes",
                   title="Crashes by Day of Week")

    df5 = q(f"""
        SELECT vehicle_category,
               SUM(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5*(
                       number_of_pedestrians_killed +
                       number_of_cyclist_killed +
                       number_of_motorist_killed
                   )
               ) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity",
                  title="Severity by Vehicle Category")

    df_gender = q(f"""
        SELECT person_sex AS gender, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY person_sex
    """)
    fig6 = px.bar(df_gender, x="gender", y="crashes",
                  title="Crashes by Gender", color="gender")

    df7 = q(f"""
        SELECT hour,
               AVG(number_of_pedestrians_injured) AS ped,
               AVG(number_of_cyclist_injured) AS cyc,
               AVG(number_of_motorist_injured) AS mot
        FROM collisions {where}
        AND hour BETWEEN 0 AND 23
        GROUP BY hour
    """)
    melted = df7.melt(id_vars="hour", value_name="avg", var_name="type")
    fig7 = px.line(melted, x="hour", y="avg", color="type",
                   title="Hourly Injuries")

    df8 = q(f"""
        SELECT vehicle_category, hour,
               AVG(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5*(number_of_pedestrians_killed +
                      number_of_cyclist_killed +
                      number_of_motorist_killed)
               ) AS severity
        FROM collisions {where}
        AND hour BETWEEN 0 AND 23
        GROUP BY vehicle_category, hour
    """)
    heat = df8.pivot(index="vehicle_category", columns="hour", values="severity").fillna(0)
    fig8 = px.imshow(heat, title="Severity Heatmap")

    df9 = q(f"""
        SELECT on_street_name,
               SUM(
                   number_of_pedestrians_injured +
                   number_of_cyclist_injured +
                   number_of_motorist_injured +
                   5*(number_of_pedestrians_killed +
                      number_of_cyclist_killed +
                      number_of_motorist_killed)
               ) AS severity
        FROM collisions {where}
        GROUP BY on_street_name
        ORDER BY severity DESC LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="severity",
                  title="Top 15 Streets by Severity")

    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type",
                   title="Age Group Analysis")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


# ============================================================
# CLEAR FILTERS
# ============================================================
@app.callback(
    [Output("borough_filter", "value"),
     Output("year_filter", "value"),
     Output("vehicle_filter", "value"),
     Output("factor_filter", "value"),
     Output("injury_filter", "value")],
    Input("clear_filters", "n_clicks"),
    prevent_initial_call=True
)
def clear_all(_):
    return None, None, None, None, None


@app.callback(
    Output("search_box", "value"),
    Input("clear_search", "n_clicks"),
    prevent_initial_call=True
)
def clear_search(_):
    return ""


# ============================================================
# RUN SERVER
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
