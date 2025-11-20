# main.py
# NYC Collision Dashboard with:
# - Filters and search only applied when "Generate Report" is pressed
# - Vehicle filter uses vehicle_category groupings
# - Extra visualization: Age Group vs Injury Severity by Person Type

import re
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State

# =======================================================
# LOAD CSV
# =======================================================
df = pd.read_csv(r"C:\Users\maria\Downloads\visulaization\final_cleaned2.csv")

# Basic cleaning
df["borough"] = df["borough"].astype(str).str.upper().str.strip()
df["person_injury"] = df["person_injury"].astype(str).str.upper().str.strip()
df["contributing_factor_vehicle_1"] = df["contributing_factor_vehicle_1"].astype(str).str.upper().str.strip()
df["person_sex"] = df["person_sex"].astype(str).str.upper().str.strip()

# Clean weird characters in vehicle type
df["vehicle_type_code_1"] = (
    df["vehicle_type_code_1"].astype(str)
    .str.upper()
    .str.replace(r"[^A-Z0-9 /-]", "", regex=True)   # remove corrupted chars like �
    .str.strip()
    .replace({"": "UNKNOWN", "NAN": "UNKNOWN"})
)

# =======================================================
# VEHICLE CATEGORY GROUPING
# =======================================================

def categorize_vehicle(v: str) -> str:
    # Passenger cars
    if any(x in v for x in ["SEDAN", "PASSENGER", "COUPE", "HATCHBACK"]):
        return "PASSENGER CAR"

    # SUVs and vans
    if "SPORT UTILITY" in v or "STATION WAGON" in v or "SUV" in v:
        return "SUV / VAN"
    if "VAN" in v:
        return "SUV / VAN"

    # Bicycles / e-bikes / scooters
    if any(x in v for x in ["BIKE", "BICYCLE", "E-BIKE", "EBIKE"]):
        return "BICYCLE / SCOOTER"
    if any(x in v for x in ["SCOOT", "SCOOTER"]):
        return "BICYCLE / SCOOTER"

    # Motorcycles / mopeds
    if any(x in v for x in ["MOTORCYCLE", "MOTORBIKE", "MINIBIKE", "MOPED"]):
        return "MOTORCYCLE / MOPED"

    # Buses
    if "BUS" in v or "OMNIBUS" in v:
        return "BUS"

    # Trucks / commercial
    if any(x in v for x in ["PICKUP", "TRUCK", "TRACTOR", "DUMP", "SEMI", "TANKER", "BOX TRUCK", "FLATBED"]):
        return "TRUCK / COMMERCIAL"

    # Emergency
    if any(x in v for x in ["AMBUL", "EMS", "FDNY", "FIRE", "EMERGENCY"]):
        return "EMERGENCY VEHICLE"

    # Government / service / utility / tow
    if any(x in v for x in ["USPS", "GOV", "NYPD", "NYFD", "SANITATION", "NYC "]):
        return "GOV / SERVICE / UTILITY"
    if any(x in v for x in ["UTILITY", "TOW TRUCK", "TOW-TRUCK", "TOW TRUCK / WRECKER"]):
        return "GOV / SERVICE / UTILITY"

    return "OTHER / UNKNOWN"

df["vehicle_category"] = df["vehicle_type_code_1"].apply(categorize_vehicle)

# =======================================================
# DATE & TIME
# =======================================================
df["crash_year"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.year
df["crash_day_of_week"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.dayofweek
df["crash_day_of_week"] = df["crash_day_of_week"].fillna(-1).astype(int)

# Extract hour from crash_time (robust to messy formats)
df["hour"] = (
    df["crash_time"].astype(str).str.extract(r"^(\d{1,2})")[0].astype(float)
)
df.loc[(df["hour"] < 0) | (df["hour"] > 23), "hour"] = None
df["hour"] = df["hour"].fillna(df["hour"].mode()[0]).astype(int)

# =======================================================
# INJURY & SEVERITY
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
# AGE GROUP CATEGORIZATION (for Vis 9)
# =======================================================
def categorize_age(age):
    if pd.isna(age):
        return "Unknown"
    try:
        age = int(age)
    except (ValueError, TypeError):
        return "Unknown"
    if age < 18:
        return "0–17"
    elif age < 30:
        return "18–29"
    elif age < 45:
        return "30–44"
    elif age < 60:
        return "45–59"
    else:
        return "60+"

if "person_age" in df.columns:
    df["person_age_group"] = df["person_age"].apply(categorize_age)
else:
    df["person_age_group"] = "Unknown"

# =======================================================
# DASH APP LAYOUT
# =======================================================
app = Dash(__name__)

app.layout = html.Div([
    html.H1("NYC Vehicle Collisions Dashboard", style={"textAlign": "center"}),

    # Buttons row
    html.Div([
        html.Button(
            "Clear All Filters", id="clear_filters", n_clicks=0,
            style={"background": "#c0392b", "color": "white", "padding": "8px 18px", "marginRight": "10px"}
        ),
        html.Button(
            "Clear Search", id="clear_search", n_clicks=0,
            style={"background": "#2980b9", "color": "white", "padding": "8px 18px"}
        )
    ], style={"textAlign": "center", "marginBottom": "15px"}),

    html.Div([
        html.Button(
            "Generate Report", id="generate_report", n_clicks=0,
            style={"background": "#27ae60", "color": "white",
                   "padding": "10px 20px", "fontWeight": "bold"}
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # Filters
    html.Div([
        html.Div([
            html.Label("Borough"),
            dcc.Dropdown(
                id="borough_filter",
                options=[{"label": b, "value": b} for b in sorted(df["borough"].unique())],
                multi=True
            )
        ], style={"width": "20%", "display": "inline-block"}),

        html.Div([
            html.Label("Year"),
            dcc.Dropdown(
                id="year_filter",
                options=[{"label": int(y), "value": int(y)} for y in sorted(df["crash_year"].dropna().unique())],
                multi=True
            )
        ], style={"width": "15%", "display": "inline-block"}),

        html.Div([
            html.Label("Vehicle Category"),
            dcc.Dropdown(
                id="vehicle_filter",
                options=[{"label": c, "value": c} for c in sorted(df["vehicle_category"].unique())],
                multi=True
            )
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Contributing Factor"),
            dcc.Dropdown(
                id="factor_filter",
                options=[{"label": f, "value": f} for f in sorted(df["contributing_factor_vehicle_1"].unique())],
                multi=True
            )
        ], style={"width": "25%", "display": "inline-block"}),

        html.Div([
            html.Label("Injury Type"),
            dcc.Dropdown(
                id="injury_filter",
                options=[{"label": i, "value": i} for i in sorted(df["person_injury"].unique())],
                multi=True
            )
        ], style={"width": "15%", "display": "inline-block"}),
    ]),

    html.Br(),

    html.Div([
        dcc.Input(
            id="search_box", type="text", debounce=True,
            placeholder="Search (e.g., Manhattan 2022 pedestrian crashes)...",
            style={"width": "60%", "height": "40px"}
        )
    ], style={"textAlign": "center", "marginBottom": "20px"}),

    # Graphs (9 total)
    dcc.Graph(id="injury_trend_graph"),          # 1
    dcc.Graph(id="factor_bar_graph"),            # 2
    dcc.Graph(id="bar_person_graph"),            # 3
    dcc.Graph(id="line_day_graph"),              # 4
    dcc.Graph(id="vehicle_severeinjuries_graph"),# 5
    dcc.Graph(id="gender_borough_graph"),        # 6
    dcc.Graph(id="user_hourly_injuries_graph"),  # 7
    dcc.Graph(id="user_severity_heatmap"),       # 8

    html.H3("Age Group vs Injury Severity Across Person Types", style={"textAlign": "center", "marginTop": "30px"}),
    dcc.Graph(id="age_group_graph"),             # 9 (new)
])

# =======================================================
# ONLY "GENERATE REPORT" UPDATES GRAPHS
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
    person_column = "number_of_cyclist_injured"

    # Apply filters ONLY when Generate Report is clicked
    if borough:
        dff = dff[dff["borough"].isin(borough)]
    if year:
        dff = dff[dff["crash_year"].isin(year)]
    if vehicle:
        dff = dff[dff["vehicle_category"].isin(vehicle)]
    if factor:
        dff = dff[dff["contributing_factor_vehicle_1"].isin(factor)]
    if injury:
        dff = dff[dff["person_injury"].isin(injury)]

    # Search logic
    if query and len(query) >= 3:
        q = query.lower()

        # Borough detection in free text
        for b in ["bronx", "brooklyn", "manhattan", "queens", "staten island"]:
            if b in q:
                dff = dff[dff["borough"] == b.upper()]

        # Year detection
        years = re.findall(r"\b(20\d{2})\b", q)
        if years:
            dff = dff[dff["crash_year"].isin(map(int, years))]

        # Person type detection
        mapping = {
            "pedestrian": "number_of_pedestrians_injured",
            "cyclist": "number_of_cyclist_injured",
            "motorist": "number_of_motorist_injured",
        }
        for key, col in mapping.items():
            if key in q:
                person_column = col

    if dff.empty:
        empty = px.scatter(title="No data available for selected filters / search.")
        return [empty] * 9

    # ========= FIGURE 1: Injury Trend by Year & Borough =========
    trend_df = (
        dff.groupby(["crash_year", "borough"])["total_injuries"]
        .sum()
        .reset_index()
    )
    fig1 = px.line(
        trend_df,
        x="crash_year",
        y="total_injuries",
        color="borough",
        markers=True,
        title="Total Injuries per Year Across Boroughs",
        labels={"crash_year": "Year", "total_injuries": "Total Injuries"},
    )

    # ========= FIGURE 2: Top Contributing Factors =========
    fac = dff["contributing_factor_vehicle_1"].value_counts().head(10).reset_index()
    fac.columns = ["factor", "count"]
    fig2 = px.bar(
        fac,
        x="count",
        y="factor",
        orientation="h",
        title="Top 10 Contributing Factors",
        labels={"count": "Crash Count", "factor": "Contributing Factor"},
    )

    # ========= FIGURE 3: Injuries by Borough (person type from search) =========
    bar_df = dff.groupby("borough")[person_column].sum().reset_index()
    fig3 = px.bar(
        bar_df,
        x="borough",
        y=person_column,
        title="Injuries by Borough (Selected Person Type)",
        labels={"borough": "Borough", person_column: "Injuries"},
    )

    # ========= FIGURE 4: Crashes by Day of Week =========
    day_df = dff.groupby("crash_day_of_week").size().reset_index(name="crashes")
    day_df["day_name"] = day_df["crash_day_of_week"].map(
        {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun", -1: "Unknown"}
    )
    fig4 = px.line(
        day_df,
        x="day_name",
        y="crashes",
        markers=True,
        title="Crash Frequency by Day of Week",
        labels={"day_name": "Day of Week", "crashes": "Number of Crashes"},
    )

    # ========= FIGURE 5: Severity by Vehicle Category =========
    vdf = (
        dff.groupby("vehicle_category")["severity"]
        .sum()
        .reset_index()
        .sort_values("severity", ascending=False)
    )
    fig5 = px.bar(
        vdf,
        x="vehicle_category",
        y="severity",
        title="Crash Severity by Vehicle Category",
        labels={"vehicle_category": "Vehicle Category", "severity": "Total Severity Score"},
    )

    # ========= FIGURE 6: Gender vs Borough =========
    gdf = dff.groupby(["borough", "person_sex"]).size().reset_index(name="count")
    fig6 = px.bar(
        gdf,
        x="borough",
        y="count",
        color="person_sex",
        barmode="group",
        title="Crash Involvement by Gender Across Boroughs",
        labels={"borough": "Borough", "count": "Number of People", "person_sex": "Sex"},
    )

    # ========= FIGURE 7: Hourly Average Injuries by Road User Type =========
    hourly = dff.groupby("hour")[
        ["number_of_pedestrians_injured", "number_of_cyclist_injured", "number_of_motorist_injured"]
    ].mean().reset_index()

    melted = hourly.melt(
        id_vars="hour",
        var_name="type",
        value_name="avg"
    )
    melted["type"] = melted["type"].map({
        "number_of_pedestrians_injured": "Pedestrian",
        "number_of_cyclist_injured": "Cyclist",
        "number_of_motorist_injured": "Motorist",
    })

    fig7 = px.line(
        melted,
        x="hour",
        y="avg",
        color="type",
        markers=True,
        title="Hourly Average Injuries by Road User Type",
        labels={"hour": "Hour of Day (0–23)", "avg": "Average Injuries", "type": "Person Type"},
    )

    # ========= FIGURE 8: Heatmap – Vehicle Category × Hour (Severity) =========
    heat = dff.groupby(["vehicle_category", "hour"])["severity"].mean().reset_index()
    pivot = heat.pivot(index="vehicle_category", columns="hour", values="severity").fillna(0)

    fig8 = px.imshow(
        pivot,
        labels={"x": "Hour of Day (0–23)", "y": "Vehicle Category", "color": "Average Severity"},
        title="Crash Severity by Vehicle Category and Hour of Day",
        aspect="auto",
    )

    # ========= FIGURE 9: Age Group vs Injury Severity Across Person Types =========
    if "person_age_group" in dff.columns and "person_type" in dff.columns:
        age_df = (
            dff.groupby(["person_age_group", "person_type", "person_injury"])
            .size()
            .reset_index(name="count")
        )

        fig9 = px.bar(
            age_df,
            x="person_age_group",
            y="count",
            color="person_injury",
            facet_col="person_type",
            category_orders={
                "person_age_group": ["0–17", "18–29", "30–44", "45–59", "60+", "Unknown"]
            },
            title="Age Group vs Injury Severity Across Person Types",
            labels={
                "person_age_group": "Age Group",
                "count": "Number of People",
                "person_injury": "Injury Severity",
                "person_type": "Person Type",
            },
            barmode="stack",
        )
    else:
        fig9 = px.scatter(title="Age-group visualization not available (missing age/person_type columns).")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9


# =======================================================
# CLEAR BUTTONS (Reset UI only, no graph update)
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
    # Only reset the dropdowns visually;
    # graphs will update only after pressing "Generate Report".
    return None, None, None, None, None


@app.callback(
    Output("search_box", "value"),
    Input("clear_search", "n_clicks"),
    prevent_initial_call=True
)
def reset_search(n):
    # Only reset text in search box; doesn't touch figures.
    return ""


# =======================================================
# RUN APP
# =======================================================
if __name__ == "__main__":
    app.run_server(
        debug=False,
        dev_tools_hot_reload=False,
        dev_tools_silence_routes_logging=True,
    )
