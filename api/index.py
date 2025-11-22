import os
import requests
import duckdb
import pandas as pd
import plotly.express as px
from flask import Flask
from dash import Dash, html, dcc, Input, Output, State
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# =======================================================
# PARQUET DOWNLOAD
# =======================================================
PARQUET_URL = "https://f005.backblazeb2.com/file/visuadataset4455/final_cleaned_final.parquet"
LOCAL_PATH = "/tmp/dataset.parquet"

def ensure_dataset():
    if not os.path.exists(LOCAL_PATH):
        r = requests.get(PARQUET_URL, stream=True)
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

ensure_dataset()

# =======================================================
# DUCKDB LOAD
# =======================================================
con = duckdb.connect(database=":memory:")
df = con.execute(f"SELECT * FROM read_parquet('{LOCAL_PATH}')").df()

# =======================================================
# CLEANING + PROCESSING
# =======================================================
df["borough"] = df["borough"].astype(str).str.upper()
df["crash_year"] = pd.to_numeric(df["crash_year"], errors="coerce").fillna(0).astype(int)

# =======================================================
# FLASK SERVER
# =======================================================
server = Flask(__name__)

# =======================================================
# DASH APP USING SAME FLASK SERVER
# =======================================================
dash_app = Dash(__name__, server=server, url_base_pathname="/")

dash_app.layout = html.Div([
    html.H1("Hello from Dash on Vercel!")
])

# =======================================================
# EXPORT FOR VERCEL
# =======================================================
def handler(request):
    return server(request)
