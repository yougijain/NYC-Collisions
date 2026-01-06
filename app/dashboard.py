import sqlite3, pandas as pd, streamlit as st
import pydeck as pdk
import os

DB_PATH = "data/clean/data.db"
SQL_DIR = "sql"

# Load Mapbox key if provided via Streamlit secrets or env
mapbox_token = st.secrets.get("MAPBOX_API_KEY") or os.getenv("MAPBOX_API_KEY")

# Choose basemap style
if mapbox_token:
    os.environ["MAPBOX_API_KEY"] = mapbox_token
    BASEMAP = "mapbox://styles/mapbox/dark-v10"
else:
    BASEMAP = None  # we’ll use an OpenStreetMap TileLayer instead

@st.cache_data(ttl=600)
def load(sql_file: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn, open(f"{SQL_DIR}/{sql_file}") as f:
        return pd.read_sql_query(f.read(), conn)

def main():
    st.title("NYC Collisions Dashboard")

    df = load("01_filter.sql")
    df["crash_datetime"] = pd.to_datetime(df["crash_datetime"])
    df["borough"] = df["borough"].fillna("UNKNOWN")

    st.sidebar.header("Filter Options")
    min_date = df["crash_datetime"].dt.date.min()
    max_date = df["crash_datetime"].dt.date.max()
    start_date, end_date = st.sidebar.date_input(
        "Date range", (min_date, max_date), min_value=min_date, max_value=max_date
    )
    boroughs = sorted(df["borough"].unique())
    selected = st.sidebar.multiselect("Boroughs", boroughs, default=boroughs)

    mask = (
        (df["crash_datetime"].dt.date >= start_date) &
        (df["crash_datetime"].dt.date <= end_date) &
        (df["borough"].isin(selected))
    )
    df_filt = df.loc[mask].dropna(subset=["latitude", "longitude"])
    df_filt["crash_datetime_str"] = df_filt["crash_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Summary Statistics
    st.subheader("📊 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Incidents", f"{len(df_filt):,}")
    with col2:
        total_injuries = df_filt["number_of_persons_injured"].sum()
        st.metric("Total Injuries", f"{int(total_injuries):,}")
    with col3:
        total_fatalities = df_filt["number_of_persons_killed"].sum()
        st.metric("Total Fatalities", f"{int(total_fatalities):,}")
    with col4:
        avg_injuries = df_filt[df_filt["number_of_persons_injured"] > 0]["number_of_persons_injured"].mean()
        st.metric("Avg Injuries/Crash", f"{avg_injuries:.2f}" if not pd.isna(avg_injuries) else "0.00")
    
    st.divider()

    df2 = load("02_aggregate.sql")
    df2["borough"] = df2["borough"].fillna("UNKNOWN")
    st.subheader("Total Injuries by Borough")
    st.bar_chart(df2[df2["borough"].isin(selected)].set_index("borough")["total_injuries"])

    df3 = load("03_time_analysis.sql")
    st.subheader("Crashes by Hour (AM/PM)")
    st.line_chart(df3.set_index("hour_label")["crash_count"])
    
    # Monthly Trends
    try:
        df4 = load("04_trends.sql")
        df4["month"] = pd.to_datetime(df4["month"])
        st.subheader("Monthly Crash Trends")
        st.line_chart(df4.set_index("month")[["crash_count", "total_injuries", "total_fatalities"]])
    except FileNotFoundError:
        pass  # Gracefully handle if trends SQL doesn't exist yet



# --------------- Crash Heatmap ---------------

    st.subheader("Crash Heatmap")

    view_state = pdk.ViewState(
        latitude=40.734,
        longitude=-73.9,
        zoom=9.7,                   # zoom level (lower = zoom out, higher = zoom in)
        pitch=0                     # tilt angle (0 = top-down view, >0 for perspective)
    )

    heat = pdk.Layer(
        "HeatmapLayer",
        df_filt,
        
        get_position=["longitude", "latitude"],
        pickable=True,               # enable picking for tooltips
        radius_pixels=30,     # radius of influence per data point in pixels
        intensity=1.4,               # heat strength multiplier (higher = hotter)
        threshold=0.3,               # cutoff for minimum normalized weight to render
        color_range=[                # color gradient stops [R,G,B,A] with half opacity
            [0,   0,   0,   0],     
            [0,   255, 0,   70],    
            [255, 255, 0,   95],    
            [255, 0,   0,   130],   
        ],
    )

    tiles = pdk.Layer(
        "TileLayer",
        data="https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
        tile_size=256,              # size of each map tile (standard = 256)
        opacity=1.0,                # tile layer opacity (0 = transparent, 1 = opaque)
    )
    
    scatter = pdk.Layer(
        "ScatterplotLayer",
        df_filt,
        get_position=["longitude", "latitude"],
        get_radius=100,              # pick radius in meters
        get_fill_color=[0, 0, 0, 0], # fully transparent
        pickable=True
    )
    tooltip = {
        "text": "Date: {crash_datetime_str}\nBorough: {borough}",
        "style": {"backgroundColor": "rgba(0, 0, 0, 0.8)", "color": "white"}
    }
    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/dark-v10",
            initial_view_state=view_state,
            layers=[tiles, heat, scatter],
            tooltip=tooltip
        )
    )
# ---------------------------------------------

    st.subheader("Sample of Filtered Records")
    st.dataframe(df_filt.head(7))

if __name__ == "__main__":
    main()
