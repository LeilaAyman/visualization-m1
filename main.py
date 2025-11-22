import os
import requests
import duckdb
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State

# =========================================================
# PARQUET SETTINGS
# =========================================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "dataset.parquet"

def ensure_dataset():
    """Download parquet if missing."""
    if not os.path.exists(LOCAL_PATH):
        print("🔥 Downloading dataset…")
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("✅ Dataset downloaded")
    else:
        print("👍 Using cached dataset")

ensure_dataset()

# =========================================================
# DUCKDB CONNECTION (NO PANDAS)
# =========================================================
con = duckdb.connect(database=":memory:")

con.execute(f"""
    CREATE TABLE collisions AS
    SELECT
        *,
        CASE
            WHEN crash_time RLIKE '^[0-9]?[0-9]:' THEN CAST(split_part(crash_time, ':', 1) AS INT)
            ELSE NULL
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
            WHEN person_age < 18 THEN '0–17'
            WHEN person_age < 30 THEN '18–29'
            WHEN person_age < 45 THEN '30–44'
            WHEN person_age < 60 THEN '45–59'
            ELSE '60+'
        END AS person_age_group
    FROM read_parquet('{LOCAL_PATH}');
""")

def q(sql, params=None):
    return con.execute(sql, params or {}).df()  # tiny dataframe chunks only for plotly


# =========================================================
# DASH APP UI
# =========================================================
app = Dash(__name__)
server = app.server

# Preload dropdown options using tiny queries
boroughs = [row[0] for row in con.execute("SELECT DISTINCT borough FROM collisions ORDER BY borough").fetchall()]
years = [row[0] for row in con.execute("SELECT DISTINCT crash_year FROM collisions ORDER BY crash_year").fetchall()]
vehicles = [row[0] for row in con.execute("SELECT DISTINCT vehicle_category FROM collisions ORDER BY vehicle_category").fetchall()]
factors = [row[0] for row in con.execute("SELECT DISTINCT contributing_factor_combined FROM collisions ORDER BY contributing_factor_combined").fetchall()]
injuries = [row[0] for row in con.execute("SELECT DISTINCT person_injury FROM collisions ORDER BY person_injury").fetchall()]

app.layout = html.Div(style={"minHeight": "6000px"}, children=[
    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

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
                  placeholder="Search anything...",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    *[dcc.Graph(id=f"g{i}") for i in range(1, 11)]
])


# =========================================================
# WHERE CLAUSE BUILDER (SQL ONLY)
# =========================================================
def build_where(borough, year, vehicle, factor, injury, query):
    filters = []

    if borough:
        filters.append(f"borough IN {tuple(borough)}")
    if year:
        filters.append(f"crash_year IN {tuple(year)}")
    if vehicle:
        filters.append(f"vehicle_category IN {tuple(vehicle)}")
    if factor:
        filters.append(f"contributing_factor_combined IN {tuple(factor)}")
    if injury:
        filters.append(f"person_injury IN {tuple(injury)}")

    if query:
        q = f"%{query.lower()}%"
        filters.append(f"""
            LOWER(borough) LIKE '{q}' OR
            LOWER(on_street_name) LIKE '{q}' OR
            LOWER(vehicle_category) LIKE '{q}' OR
            LOWER(person_type) LIKE '{q}' OR
            LOWER(person_injury) LIKE '{q}' OR
            LOWER(contributing_factor_combined) LIKE '{q}'
        """)

    return ("WHERE " + " AND ".join(filters)) if filters else ""


# =========================================================
# CALLBACK — 10 VISUALIZATIONS
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
    ]
)
def update_all(_, borough, year, vehicle, factor, injury, query):

    where = build_where(borough, year, vehicle, factor, injury, query)

    # ========== VIS 1 ==========
    df1 = q(f"""
        SELECT crash_year, borough, SUM(total_injuries) AS total
        FROM collisions {where}
        GROUP BY crash_year, borough
        ORDER BY crash_year
    """)
    fig1 = px.line(df1, x="crash_year", y="total", color="borough",
                   title="Total Injuries Trend")

    # ========== VIS 2 ==========
    df2 = q(f"""
        SELECT contributing_factor_combined AS factor, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY factor
        ORDER BY count DESC
        LIMIT 10
    """)
    fig2 = px.bar(df2, x="count", y="factor", orientation="h",
                  title="Top Factors")

    # ========== VIS 3 ==========
    df3 = q(f"""
        SELECT borough,
               SUM(
                   CASE
                       WHEN LOWER('{query or ""}') LIKE '%pedestrian%' THEN number_of_pedestrians_injured
                       WHEN LOWER('{query or ""}') LIKE '%cyclist%' THEN number_of_cyclist_injured
                       WHEN LOWER('{query or ""}') LIKE '%motorist%' THEN number_of_motorist_injured
                       ELSE number_of_cyclist_injured
                   END
               ) AS injuries
        FROM collisions {where}
        GROUP BY borough
    """)
    fig3 = px.bar(df3, x="borough", y="injuries",
                  title="Injuries by Borough")

    # ========== VIS 4 ==========
    df4 = q(f"""
        SELECT crash_day_of_week,
               COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY crash_day_of_week
        ORDER BY crash_day_of_week
    """)
    df4["day"] = df4["crash_day_of_week"].map({
        0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"
    })
    fig4 = px.line(df4, x="day", y="crashes",
                   title="Crashes by Day")

    # ========== VIS 5 ==========
    df5 = q(f"""
        SELECT vehicle_category, SUM(severity) AS severity
        FROM collisions {where}
        GROUP BY vehicle_category
    """)
    fig5 = px.bar(df5, x="vehicle_category", y="severity",
                  title="Severity by Vehicle Type")

    # ========== VIS 6 ==========
    df6 = q(f"""
        SELECT borough, person_sex, COUNT(*) AS crashes
        FROM collisions {where}
        GROUP BY borough, person_sex
    """)
    fig6 = px.bar(df6, x="borough", y="crashes",
                  color="person_sex", barmode="group",
                  title="Crashes by Borough & Gender")

    # ========== VIS 7 ==========
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
        "ped":"Pedestrian","cyc":"Cyclist","mot":"Motorist"
    })
    fig7 = px.line(melted, x="hour", y="avg", color="type",
                   title="Hourly Injuries")

    # ========== VIS 8 ==========
    df8 = q(f"""
        SELECT vehicle_category, hour, AVG(severity) AS sev
        FROM collisions {where}
        GROUP BY vehicle_category, hour
    """)
    if df8.empty:
        fig8 = px.imshow([[0]], title="Heatmap")
    else:
        pivot = df8.pivot(index="vehicle_category", columns="hour", values="sev").fillna(0)
        fig8 = px.imshow(pivot, title="Severity Heatmap")

    # ========== VIS 9 ==========
    df9 = q(f"""
        SELECT on_street_name, SUM(severity) AS tot
        FROM collisions {where}
        WHERE on_street_name IS NOT NULL
        GROUP BY on_street_name
        ORDER BY tot DESC
        LIMIT 15
    """)
    fig9 = px.bar(df9, x="on_street_name", y="tot",
                  title="Top Streets by Severity")

    # ========== VIS 10 ==========
    df10 = q(f"""
        SELECT person_age_group, person_type, person_injury, COUNT(*) AS count
        FROM collisions {where}
        GROUP BY person_age_group, person_type, person_injury
    """)
    fig10 = px.bar(df10, x="person_age_group", y="count",
                   color="person_injury", facet_col="person_type",
                   title="Age Group vs Injury Severity")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


# =========================================================
# RUN ON FLY.IO
# =========================================================
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8080)
