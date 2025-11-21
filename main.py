# ============================================
# main.py — FINAL RENDER VERSION (DuckDB + Local Cache)
# ============================================

import os
import requests
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, State

# ================================
# SETTINGS
# ================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"

# ================================
# DOWNLOAD ONCE INTO /tmp
# ================================
def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset (first-time only)...")
        try:
            r = requests.get(PARQUET_URL, stream=True)
            r.raise_for_status()
            with open(LOCAL_PATH, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print("✅ Dataset downloaded")
        except Exception as e:
            print("❌ FAILED TO DOWNLOAD DATASET:", e)
            raise

ensure_dataset()

# ================================
# CONNECT TO DUCKDB + READ PARQUET
# ================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE VIEW collisions AS
    SELECT * FROM read_parquet('{LOCAL_PATH}');
""")

def q(sql):
    return con.execute(sql).df()

# ================================
# DASH APP
# ================================
app = Dash(__name__)
server = app.server

# Preload filters
boroughs = q("SELECT DISTINCT borough FROM collisions ORDER BY borough")["borough"].fillna("UNKNOWN")
years = q("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year")["crash_year"]
vehicles = q("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category")["vehicle_category"]
factors = q("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined")["contributing_factor_combined"]
injuries = q("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury")["person_injury"]

# ================================
# LAYOUT
# ================================
app.layout = html.Div([
    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    html.Div([
        html.Button(
            "Generate Report",
            id="generate",
            style={"padding": "10px 20px", "background": "#27ae60", "color": "white"}
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    html.Div([
        dcc.Dropdown(id="borough_f", options=[{"label": b, "value": b} for b in boroughs], multi=True),
        dcc.Dropdown(id="year_f", options=[{"label": int(y), "value": int(y)} for y in years], multi=True),
        dcc.Dropdown(id="vehicle_f", options=[{"label": v, "value": v} for v in vehicles], multi=True),
        dcc.Dropdown(id="factor_f", options=[{"label": f, "value": f} for f in factors], multi=True),
        dcc.Dropdown(id="injury_f", options=[{"label": i, "value": i} for i in injuries], multi=True),
        dcc.Input(id="search", type="text", placeholder="Search...", debounce=True)
    ], style={"margin": "20px"}),

    *[dcc.Graph(id=f"g{i}") for i in range(1, 11)]
])

# ================================
# FILTERS
# ================================
def build_where(borough, year, vehicle, factor, injury, query):
    filters = []

    if borough:
        filters.append(f"borough IN ({','.join([repr(b) for b in borough])})")
    if year:
        filters.append(f"crash_year IN ({','.join([str(y) for y in year])})")
    if vehicle:
        filters.append(f"vehicle_category IN ({','.join([repr(v) for v in vehicle])})")
    if factor:
        filters.append(f"contributing_factor_combined IN ({','.join([repr(f) for f in factor])})")
    if injury:
        filters.append(f"person_injury IN ({','.join([repr(i) for i in injury])})")
    if query:
        filters.append(
            f"(lower(borough) LIKE '%{query.lower()}%' OR lower(on_street_name) LIKE '%{query.lower()}%')"
        )

    return ("WHERE " + " AND ".join(filters)) if filters else ""

# ================================
# CALLBACK
# ================================
@app.callback(
    [Output(f"g{i}", "figure") for i in range(1, 11)],
    Input("generate", "n_clicks"),
    [
        State("borough_f","value"), State("year_f","value"),
        State("vehicle_f","value"), State("factor_f","value"),
        State("injury_f","value"), State("search","value"),
    ]
)
def update(_, borough, year, vehicle, factor, injury, query):

    where = build_where(borough, year, vehicle, factor, injury, query)

    # ================= Graph 1 =================
    df1 = q(f"""
        SELECT crash_year, borough, SUM(number_of_persons_injured) AS injuries
        FROM collisions {where}
        GROUP BY crash_year, borough ORDER BY crash_year
    """)
    fig1 = px.line(df1, x="crash_year", y="injuries", color="borough")

    # ================= Graph 2 =================
    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor ORDER BY count DESC LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h")

    # ================= Graph 3 =================
    df3 = q(f"""
        SELECT borough, SUM(number_of_persons_injured) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)
    fig3 = px.bar(df3, x="borough", y="injuries")

    # ================= Graph 4 =================
    df4 = q(f"""
        SELECT crash_day_of_week, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week ORDER BY crash_day_of_week
    """)
    fig4 = px.line(df4, x="crash_day_of_week", y="crashes")

    # ================= Graph 5 =================
    df5 = q(f"""
        SELECT vehicle_category,
               SUM(number_of_persons_injured + 5*number_of_persons_killed) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity")

    # ================= Graph 6 =================
    df6 = q(f"""
        SELECT borough, person_sex, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY borough, person_sex
    """)
    fig6 = px.bar(df6, x="borough", y="count", color="person_sex")

    # ================= Graph 7 =================
    df7 = q(f"""
        SELECT hour,
            AVG(number_of_pedestrians_injured) AS ped,
            AVG(number_of_cyclist_injured) AS cyc,
            AVG(number_of_motorist_injured) AS mot
        FROM collisions {where}
        GROUP BY hour ORDER BY hour
    """)
    melted = df7.melt(id_vars="hour", var_name="type", value_name="avg")
    fig7 = px.line(melted, x="hour", y="avg", color="type")

    # ================= Graph 8 =================
    df8 = q(f"""
        SELECT vehicle_category, hour,
               AVG(number_of_persons_injured + 5*number_of_persons_killed) AS sev
        FROM collisions {where}
        GROUP BY vehicle_category, hour
    """)
    fig8 = px.density_heatmap(df8, x="hour", y="vehicle_category", z="sev")

    # ================= Graph 9 =================
    df9 = q(f"""
        SELECT on_street_name,
            SUM(number_of_persons_injured + 5*number_of_persons_killed) AS sev
        FROM collisions {where}
        GROUP BY on_street_name ORDER BY sev DESC LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="sev")

    # ================= Graph 10 =================
    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10

# ================================
# RUN ON RENDER
# ================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
