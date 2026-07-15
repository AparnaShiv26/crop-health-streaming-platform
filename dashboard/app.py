import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Crop Health Analytics",
    page_icon="🌱",
    layout="wide",
)

st.title("🌱 Crop Health Streaming Platform")
st.caption(
    "Kafka → Spark → Bronze/Silver/Gold Data Lake → PostgreSQL"
)


@st.cache_data
def load_data():
    connection = psycopg2.connect(
        host="localhost",
        port=5432,
        database="crop_health",
        user="postgres",
        password="postgres",
    )

    query = """
        SELECT *
        FROM crop_health_analytics
    """

    df = pd.read_sql(query, connection)
    connection.close()

    return df


df = load_data()


# -----------------------------
# Sidebar filters
# -----------------------------

st.sidebar.header("Filters")

regions = ["All"] + sorted(df["region"].dropna().unique().tolist())

selected_region = st.sidebar.selectbox(
    "Region",
    regions,
)

crops = ["All"] + sorted(df["crop"].dropna().unique().tolist())

selected_crop = st.sidebar.selectbox(
    "Crop",
    crops,
)


filtered_df = df.copy()

if selected_region != "All":
    filtered_df = filtered_df[
        filtered_df["region"] == selected_region
    ]

if selected_crop != "All":
    filtered_df = filtered_df[
        filtered_df["crop"] == selected_crop
    ]


# -----------------------------
# KPI cards
# -----------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Farms",
    filtered_df["farm_id"].nunique(),
)

col2.metric(
    "Average NDVI",
    round(filtered_df["ndvi"].mean(), 2),
)

col3.metric(
    "Average SOC",
    round(filtered_df["soc"].mean(), 2),
)

col4.metric(
    "Average Health Score",
    round(filtered_df["crop_health_score"].mean(), 2),
)


# -----------------------------
# Health risk distribution
# -----------------------------

st.subheader("Health Risk Distribution")

risk_counts = (
    filtered_df["health_risk"]
    .value_counts()
    .reset_index()
)

risk_counts.columns = [
    "health_risk",
    "count",
]

risk_chart = px.pie(
    risk_counts,
    names="health_risk",
    values="count",
)

st.plotly_chart(
    risk_chart,
    use_container_width=True,
)


# -----------------------------
# Average health by crop
# -----------------------------

st.subheader("Average Crop Health Score")

crop_health = (
    filtered_df
    .groupby("crop", as_index=False)
    ["crop_health_score"]
    .mean()
    .sort_values(
        "crop_health_score",
        ascending=False,
    )
)

crop_chart = px.bar(
    crop_health,
    x="crop",
    y="crop_health_score",
    labels={
        "crop": "Crop",
        "crop_health_score": "Average Health Score",
    },
)

st.plotly_chart(
    crop_chart,
    use_container_width=True,
)


# -----------------------------
# NDVI vs SOC
# -----------------------------

st.subheader("NDVI vs Soil Organic Carbon")

scatter_chart = px.scatter(
    filtered_df,
    x="soc",
    y="ndvi",
    color="health_risk",
    hover_data=[
        "farm_id",
        "crop",
        "region",
    ],
)

st.plotly_chart(
    scatter_chart,
    use_container_width=True,
)


# -----------------------------
# Data table
# -----------------------------

st.subheader("Farm Health Records")

st.dataframe(
    filtered_df[
        [
            "farm_id",
            "sensor_id",
            "region",
            "crop",
            "ndvi",
            "soc",
            "vegetation_health",
            "crop_health_score",
            "health_risk",
        ]
    ],
    use_container_width=True,
)