# main.py — FINAL CLEAN FIXED VERSION WITH TITLES
# All filters affect ALL visualizations
# No weird years (2021.5 / 2022.5) — x-axis fully fixed
# Brooklyn + 2022 always returns correct points

import re
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State
import os  # <-- added for Render port

# =======================================================
# LOAD CLEANED CSV
# =======================================================
df = pd.read_csv("https://drive.google.com/uc?export=download&id=1vqL6CP13G0MuB-tIZLem7FFTOy5xBNCz")

# =======================================================
# CLEANING CRITICAL COLUMNS
# =======================================================
df["borough"] = df["borough"].astype(str).str.strip().str.upper()
df["crash_year"] = pd.to_numeric(df["crash_year"], errors="coerce").fillna(0).astype(int)

df["crash_time"] = df["crash_time"].astype(str).str.strip()
df["hour"] = df["crash_time"].str.extract(r"^(\d{1,2})")[0].astype(float)
df.loc[(df["hour"] < 0) | (df["hour"] > 23), "hour"] = None
df["hour"] = df["hour"].fillna(df["hour"].mode()[0]).astype(int)

# =======================================================
# INJURY & SEVERITY METRICS
# =======================================================
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

# =======================================================
# AGE GROUPING
# =======================================================
def categorize_age(age):
    if pd.isna(age):
        return "Unknown"
    try:
        age = int(age)
    except:
        return "Unknown"
    if age < 18: return "0–17"
    if age < 30: return "18–29"
    if age < 45: return "30–44"
    if age < 60: return "45–59"
    return "60+"

df["person_age_group"] = df["person_age"].apply(categorize_age)

# =======================================================
# DASH APP LAYOUT
# =======================================================
app = Dash(__name__)
server = app.server  # <-- REQUIRED FOR RENDER / GUNICORN

app.layout = html.Div(style={"minHeight": "6000px"}, children=[
    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    # ================== BUTTONS ===================
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

    # ================== FILTERS ===================
    html.Div([

        html.Div([
            html.Label("Borough"),
            dcc.Dropdown(
                id="borough_filter",
                options=[{"label": b, "value": b} for b in sorted(df["borough"].unique())],
                multi=True,
                persistence=False
            )
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(
                id="year_filter",
                options=[{"label": int(y), "value": int(y)} for y in sorted(df["crash_year"].unique())],
                multi=True,
                persistence=False
            )
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(
                id="vehicle_filter",
                options=[{"label": c, "value": c} for c in sorted(df["vehicle_category"].unique())],
                multi=True,
                persistence=False
            )
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(
                id="factor_filter",
                options=[{"label": f, "value": f} for f in sorted(df["contributing_factor_combined"].unique())],
                multi=True,
                persistence=False
            )
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(
                id="injury_filter",
                options=[{"label": i, "value": i} for i in sorted(df["person_injury"].unique())],
                multi=True,
                persistence=False
            )
        ], style={"width": "15%", "display": "inline-block"}),

    ]),

    html.Br(),

    # Search box
    html.Div([
        dcc.Input(id="search_box", type="text", debounce=True,
                  placeholder="Search (e.g., Manhattan 2022 pedestrian crashes)...",
                  style={"width": "60%", "height": "40px"})
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # ================== ALL GRAPHS ===================
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
# CALLBACK — APPLY ALL FILTERS TO ALL VISUALS
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
def update_all(n, borough, year, vehicle, factor, injury, query):

    dff = df.copy()

    # ================= CLEAN AGAIN AFTER FILTERING =================
    dff["borough"] = dff["borough"].astype(str).str.strip().str.upper()
    dff["crash_year"] = pd.to_numeric(dff["crash_year"], errors="coerce").fillna(0).astype(int)

    # ================= APPLY FILTERS =================
    if borough:
        borough = [b.strip().upper() for b in borough]
        dff = dff[dff["borough"].isin(borough)]

    if year:
        year = [int(y) for y in year]
        dff = dff[dff["crash_year"].isin(year)]

    if vehicle:
        dff = dff[dff["vehicle_category"].isin(vehicle)]

    if factor:
        dff = dff[dff["contributing_factor_combined"].isin(factor)]

    if injury:
        dff = dff[dff["person_injury"].isin(injury)]

    # ================= SEARCH QUERY =================
    person_column = "number_of_cyclist_injured"

    if query and len(query) >= 3:
        q = query.lower()

        years = re.findall(r"\b(20\d{2})\b", q)
        if years:
            dff = dff[dff["crash_year"].isin(map(int, years))]

        if "pedestrian" in q:
            person_column = "number_of_pedestrians_injured"
        elif "cyclist" in q:
            person_column = "number_of_cyclist_injured"
        elif "motorist" in q:
            person_column = "number_of_motorist_injured"

    if dff.empty:
        empty = px.scatter(title="No data.")
        return [empty] * 10

    # =======================================================
    # VISUALIZATIONS BELOW (unchanged)
    # =======================================================

    vis1_data = (
        dff.groupby(["crash_year", "borough"], as_index=False)["total_injuries"].sum()
    )

    fig1 = px.line(
        vis1_data,
        x="crash_year",
        y="total_injuries",
        color="borough",
        title="Total Injuries Trend by Year and Borough"
    )

    fig1.update_traces(mode="lines+markers", marker=dict(size=10))
    fig1.update_xaxes(type="category")

    fac = dff["contributing_factor_combined"].value_counts().head(10).reset_index()
    fac.columns = ["factor", "count"]

    fig2 = px.bar(
        fac,
        x="count",
        y="factor",
        orientation="h",
        title="Top 10 Contributing Factors"
    )

    fig3 = px.bar(
        dff.groupby("borough")[person_column].sum().reset_index(),
        x="borough",
        y=person_column,
        title="Injuries by Borough (Filtered Person Type)"
    )

    day_df = dff.groupby("crash_day_of_week").size().reset_index(name="crashes")
    day_df["day_name"] = day_df["crash_day_of_week"].map({
        0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun",-1:"Unknown"
    })

    fig4 = px.line(
        day_df,
        x="day_name",
        y="crashes",
        title="Crashes by Day of Week"
    )

    fig5 = px.bar(
        dff.groupby("vehicle_category")["severity"].sum().reset_index(),
        x="vehicle_category",
        y="severity",
        title="Severity Score by Vehicle Category"
    )

    fig6 = px.bar(
        dff.groupby(["borough", "person_sex"]).size().reset_index(name="count"),
        x="borough",
        y="count",
        color="person_sex",
        barmode="group",
        title="Injuries by Borough and Gender"
    )

    hourly = dff.groupby("hour")[[
        "number_of_pedestrians_injured",
        "number_of_cyclist_injured",
        "number_of_motorist_injured"
    ]].mean().reset_index()

    melted = hourly.melt(id_vars="hour", var_name="type", value_name="avg_injuries")
    melted["type"] = melted["type"].map({
        "number_of_pedestrians_injured":"Pedestrian",
        "number_of_cyclist_injured":"Cyclist",
        "number_of_motorist_injured":"Motorist"
    })

    fig7 = px.line(
        melted,
        x="hour",
        y="avg_injuries",
        color="type",
        title="Average Hourly Injuries by User Type"
    )

    heat = dff.groupby(["vehicle_category", "hour"])["severity"].mean().reset_index()
    pivot = heat.pivot(index="vehicle_category", columns="hour", values="severity").fillna(0)

    fig8 = px.imshow(
        pivot,
        title="Heatmap of crash Severity by Vehicle Category and Hour"
    )

    street_df = (
        dff.groupby("on_street_name")["severity"]
        .sum()
        .nlargest(15)
        .reset_index()
    )

    fig9 = px.bar(
        street_df,
        x="on_street_name",
        y="severity",
        title="Top 15 Streets by Severity"
    )

    fig10 = px.bar(
        dff.groupby(["person_age_group", "person_type", "person_injury"])
            .size().reset_index(name="count"),
        x="person_age_group",
        y="count",
        color="person_injury",
        facet_col="person_type",
        title="Age Group vs Injury Severity by Person Type"
    )

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10


# =======================================================
# CLEAR FILTER CALLBACKS
# =======================================================
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

# =======================================================
# RUN SERVER — REQUIRED FOR RENDER
# =======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port, debug=False)
