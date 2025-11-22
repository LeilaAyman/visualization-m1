import os
import requests
import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State

# =======================================================
# PARQUET DOWNLOAD
# =======================================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "dataset.parquet"   # HF Spaces allows local write

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

# =======================================================
# LOAD DATA USING DUCKDB
# =======================================================
con = duckdb.connect(database=":memory:")
df = con.execute(f"SELECT * FROM read_parquet('{LOCAL_PATH}')").df()

# =======================================================
# CLEANING
# =======================================================
df["borough"] = df["borough"].astype(str).str.strip().str.upper()
df["crash_year"] = pd.to_numeric(df["crash_year"], errors="coerce").fillna(0).astype(int)

df["crash_time"] = df["crash_time"].astype(str).str.strip()
df["hour"] = df["crash_time"].str.extract(r"^(\d{1,2})")[0].astype(float)
df.loc[(df["hour"] < 0) | (df["hour"] > 23), "hour"] = None
df["hour"] = df["hour"].fillna(df["hour"].mode()[0]).astype(int)

df["total_injuries"] = (
    df["number_of_persons_injured"]
    + df["number_of_pedestrians_injured"]
    + df["number_of_cyclist_injured"]
    + df["number_of_motorist_injured"]
)

df["severity"] = df["total_injuries"] + 5 * (
    df["number_of_persons_killed"]
    + df["number_of_pedestrians_killed"]
    + df["number_of_cyclist_killed"]
    + df["number_of_motorist_killed"]
)

def categorize_age(age):
    if pd.isna(age): return "Unknown"
    try: age = int(age)
    except: return "Unknown"
    if age < 18: return "0–17"
    if age < 30: return "18–29"
    if age < 45: return "30–44"
    if age < 60: return "45–59"
    return "60+"

df["person_age_group"] = df["person_age"].apply(categorize_age)

# =======================================================
# DASH APP
# =======================================================
app = Dash(__name__)

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
                options=[{"label": b, "value": b} for b in sorted(df["borough"].unique())],
                multi=True),
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(id="year_filter",
                options=[{"label": int(y), "value": int(y)} for y in sorted(df["crash_year"].unique())],
                multi=True),
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(id="vehicle_filter",
                options=[{"label": c, "value": c} for c in sorted(df["vehicle_category"].unique())],
                multi=True),
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(id="factor_filter",
                options=[{"label": f, "value": f} for f in sorted(df["contributing_factor_combined"].unique())],
                multi=True),
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(id="injury_filter",
                options=[{"label": i, "value": i} for i in sorted(df["person_injury"].unique())],
                multi=True),
        ], style={"width": "15%", "display": "inline-block"}),
    ]),

    html.Br(),

    html.Div([
        dcc.Input(id="search_box", type="text", debounce=True,
                  placeholder="Search anything...",
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

# =======================================================
# CALLBACK
# =======================================================
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
    ]
)
def update_all(_, borough, year, vehicle, factor, injury, query):

    dff = df.copy()

    if borough: dff = dff[dff["borough"].isin(borough)]
    if year: dff = dff[dff["crash_year"].isin(year)]
    if vehicle: dff = dff[dff["vehicle_category"].isin(vehicle)]
    if factor: dff = dff[dff["contributing_factor_combined"].isin(factor)]
    if injury: dff = dff[dff["person_injury"].isin(injury)]

    person_column = "number_of_cyclist_injured"
    if query and len(query) >= 3:
        q = query.lower()
        if "pedestrian" in q: person_column = "number_of_pedestrians_injured"
        elif "cyclist" in q: person_column = "number_of_cyclist_injured"
        elif "motorist" in q: person_column = "number_of_motorist_injured"

    if dff.empty:
        empty = px.scatter(title="No data.")
        return [empty] * 10

    fig1 = px.line(
        dff.groupby(["crash_year", "borough"])["total_injuries"].sum().reset_index(),
        x="crash_year", y="total_injuries", color="borough",
        title="Total Injuries Trend"
    )

    fac = dff["contributing_factor_combined"].value_counts().head(10).reset_index()
    fac.columns = ["factor", "count"]
    fig2 = px.bar(fac, x="count", y="factor", orientation="h",
                  title="Top Contributing Factors")

    fig3 = px.bar(
        dff.groupby("borough")[person_column].sum().reset_index(),
        x="borough", y=person_column,
        title="Injuries by Borough"
    )

    day_df = dff.groupby("crash_day_of_week").size().reset_index(name="crashes")
    day_df["day"] = day_df["crash_day_of_week"].map(
        {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
    )
    fig4 = px.line(day_df, x="day", y="crashes",
                   title="Crashes by Day of Week")

    fig5 = px.bar(
        dff.groupby("vehicle_category")["severity"].sum().reset_index(),
        x="vehicle_category", y="severity",
        title="Severity by Vehicle Category"
    )

    gender_df = dff.groupby(["borough", "person_sex"]) \
                   .size().reset_index(name="crashes")
    fig6 = px.bar(
        gender_df, x="borough", y="crashes",
        color="person_sex", barmode="group",
        title="Crashes by Borough and Gender"
    )

    hourly = dff.groupby("hour")[
        ["number_of_pedestrians_injured",
         "number_of_cyclist_injured",
         "number_of_motorist_injured"]
    ].mean().reset_index()

    melted = hourly.melt(id_vars="hour", var_name="type",
                         value_name="avg")
    melted["type"] = melted["type"].map({
        "number_of_pedestrians_injured":"Pedestrian",
        "number_of_cyclist_injured":"Cyclist",
        "number_of_motorist_injured":"Motorist"
    })
    fig7 = px.line(melted, x="hour", y="avg",
                   color="type",
                   title="Average Hourly Injuries")

    heat = dff.groupby(["vehicle_category", "hour"])["severity"].mean().reset_index()
    pivot = heat.pivot(index="vehicle_category", columns="hour",
                       values="severity").fillna(0)
    fig8 = px.imshow(pivot, title="Severity Heatmap")

    fig9 = px.bar(
        dff.groupby("on_street_name")["severity"]
           .sum().nlargest(15).reset_index(),
        x="on_street_name", y="severity",
        title="Top 15 Streets by Severity"
    )

    fig10 = px.bar(
        dff.groupby(["person_age_group",
                     "person_type",
                     "person_injury"]).size().reset_index(name="count"),
        x="person_age_group", y="count",
        color="person_injury", facet_col="person_type",
        title="Age Group vs Injury Severity"
    )

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


server = app.server

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=7860)
