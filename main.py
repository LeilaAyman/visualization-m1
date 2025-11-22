import os
import requests
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, State

# ============================================
# BACKBLAZE PARQUET URL (YOUR EXACT LINK)
# ============================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"

# ============================================
# DOWNLOAD ON RENDER ONLY ONCE
# ============================================
def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset...")
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print("✅ Dataset saved to /tmp")
    else:
        print("👍 Using cached dataset")


ensure_dataset()

# ============================================
# LOAD DATA WITH DUCKDB
# ============================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE VIEW collisions AS
    SELECT * FROM read_parquet('{LOCAL_PATH}');
""")

def q(sql: str):
    return con.execute(sql).df()

# ============================================
# DASH APP
# ============================================
app = Dash(__name__)
server = app.server

# Preload filter options
boroughs = q("SELECT DISTINCT borough FROM collisions ORDER BY borough")["borough"].fillna("UNKNOWN")
years = q("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year")["crash_year"]
vehicles = q("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category")["vehicle_category"]
factors = q("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined")["contributing_factor_combined"]
injuries = q("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury")["person_injury"]

# ============================================
# LAYOUT (IDENTICAL TO LOCAL VERSION)
# ============================================
app.layout = html.Div(style={"minHeight": "6000px"}, children=[
    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    # BUTTONS
    html.Div([
        html.Button("Clear All Filters", id="clear_filters",
                    style={"background": "#c0392b", "color": "white", "padding": "8px 18px"}),
        html.Button("Clear Search", id="clear_search",
                    style={"background": "#2980b9", "color": "white", "padding": "8px 18px",
                           "marginLeft": "10px"})
    ], style={"textAlign": "center", "marginBottom": "15px"}),

    html.Div([
        html.Button(
            "Generate Report",
            id="generate",
            style={"background": "#27ae60", "color": "white",
                   "padding": "10px 20px", "fontWeight": "bold"}
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # FILTERS
    html.Div([

        html.Div([
            html.Label("Borough"),
            dcc.Dropdown(id="borough_f",
                options=[{"label": b, "value": b} for b in boroughs],
                multi=True)
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(id="year_f",
                options=[{"label": int(y), "value": int(y)} for y in years],
                multi=True)
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(id="vehicle_f",
                options=[{"label": v, "value": v} for v in vehicles],
                multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(id="factor_f",
                options=[{"label": f, "value": f} for f in factors],
                multi=True)
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(id="injury_f",
                options=[{"label": i, "value": i} for i in injuries],
                multi=True)
        ], style={"width": "15%", "display": "inline-block"}),
    ]),

    html.Br(),

    # Search Box
    html.Div([
        dcc.Input(id="search", type="text", debounce=True,
                  placeholder="Search (e.g., Manhattan 2022 pedestrian crashes)...",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # GRAPHS (1–10)
    *[dcc.Graph(id=f"g{i}") for i in range(1, 11)]
])

# ============================================
# WHERE FILTER BUILDER
# ============================================
def build_where(b, y, v, f, inj, s):
    filters = []

    if b: filters.append(f"borough IN ({','.join([repr(x) for x in b])})")
    if y: filters.append(f"crash_year IN ({','.join([str(x) for x in y])})")
    if v: filters.append(f"vehicle_category IN ({','.join([repr(x) for x in v])})")
    if f: filters.append(f"contributing_factor_combined IN ({','.join([repr(x) for x in f])})")
    if inj: filters.append(f"person_injury IN ({','.join([repr(x) for x in inj])})")
    if s: filters.append(f"(lower(borough) LIKE '%{s.lower()}%' OR lower(on_street_name) LIKE '%{s.lower()}%')")

    return ("WHERE " + " AND ".join(filters)) if filters else ""


# ============================================
# MAIN CALLBACK — ALL 10 GRAPHS
# ============================================
@app.callback(
    [Output(f"g{i}", "figure") for i in range(1, 10+1)],
    Input("generate", "n_clicks"),
    [
        State("borough_f", "value"),
        State("year_f", "value"),
        State("vehicle_f", "value"),
        State("factor_f", "value"),
        State("injury_f", "value"),
        State("search", "value"),
    ]
)
def update(_, borough, year, vehicle, factor, injury, search):

    where = build_where(borough, year, vehicle, factor, injury, search)

    # ==== Graph 1: Injuries Trend (with dot markers like original) ====
    df1 = q(f"""
        SELECT crash_year, borough,
               SUM(number_of_persons_injured
                   + number_of_pedestrians_injured
                   + number_of_cyclist_injured
                   + number_of_motorist_injured) AS total_injuries
        FROM collisions {where}
        GROUP BY crash_year, borough
        ORDER BY crash_year
    """)
    fig1 = px.line(df1, x="crash_year", y="total_injuries",
                   color="borough", title="Total Injuries Trend by Year & Borough")
    fig1.update_traces(mode="lines+markers", marker=dict(size=10))
    fig1.update_xaxes(type="category")

    # ==== Graph 2: Top Contributing Factors ====
    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor ORDER BY count DESC LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h",
                  title="Top 10 Contributing Factors")

    # ==== Graph 3: Injuries by Borough ====
    df3 = q(f"""
        SELECT borough,
               SUM(number_of_cyclist_injured) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)
    fig3 = px.bar(df3, x="borough", y="injuries",
                  title="Injuries by Borough (Filtered Person Type)")

    # ==== Graph 4: Crashes by Day ====
    df4 = q(f"""
        SELECT crash_day_of_week, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week ORDER BY crash_day_of_week
    """)
    fig4 = px.line(df4, x="crash_day_of_week", y="crashes",
                   title="Crashes by Day of Week")

    # ==== Graph 5: Severity by Vehicle ====
    df5 = q(f"""
        SELECT vehicle_category,
               SUM(number_of_persons_injured + 5*number_of_persons_killed) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity",
                  title="Severity Score by Vehicle Category")

    # ==== Graph 6: Gender Breakdown ====
    df6 = q(f"""
        SELECT borough, person_sex, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY borough, person_sex
    """)
    fig6 = px.bar(df6, x="borough", y="count",
                  color="person_sex", barmode="group",
                  title="Injuries by Borough and Gender")

    # ==== Graph 7: Hourly Injury Risk ====
    df7 = q(f"""
        SELECT hour,
            AVG(number_of_pedestrians_injured) AS ped,
            AVG(number_of_cyclist_injured) AS cyc,
            AVG(number_of_motorist_injured) AS mot
        FROM collisions {where}
        GROUP BY hour ORDER BY hour
    """)
    melted = df7.melt(id_vars="hour", var_name="type", value_name="avg")
    fig7 = px.line(melted, x="hour", y="avg", color="type",
                   title="Average Hourly Injuries by User Type")

    # ==== Graph 8: Severity Heatmap ====
    df8 = q(f"""
        SELECT vehicle_category, hour,
               AVG(number_of_persons_injured + 5*number_of_persons_killed) AS sev
        FROM collisions {where}
        GROUP BY vehicle_category, hour
    """)
    fig8 = px.density_heatmap(df8, x="hour", y="vehicle_category", z="sev",
                              title="Severity Heatmap (Vehicle Category × Hour)")

    # ==== Graph 9: Top Streets ====
    df9 = q(f"""
        SELECT on_street_name,
               SUM(number_of_persons_injured + 5*number_of_persons_killed) AS sev
        FROM collisions {where}
        GROUP BY on_street_name
        ORDER BY sev DESC LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="sev",
                  title="Top 15 Streets by Severity")

    # ==== Graph 10: Age Group ====
    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type",
                   title="Age Group vs Injury Severity by Person Type")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


# ============================================
# CLEAR FILTERS
# ============================================
@app.callback(
    [Output("borough_f","value"),
     Output("year_f","value"),
     Output("vehicle_f","value"),
     Output("factor_f","value"),
     Output("injury_f","value")],
    Input("clear_filters", "n_clicks"),
    prevent_initial_call=True
)
def reset_filters(_):
    return None, None, None, None, None


@app.callback(
    Output("search","value"),
    Input("clear_search","n_clicks"),
    prevent_initial_call=True
)
def reset_search(_):
    return ""


# ============================================
# RUN
# ============================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
