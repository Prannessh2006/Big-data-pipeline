import streamlit as st
import pandas as pd
import time
import os

st.set_page_config(page_title="Real-Time Big Data Dashboard", layout="wide")

st.title("Real-Time Big Data Streaming Dashboard")

data_path = "storage/gold"

placeholder = st.empty()

while True:
    try:
        files = [f"{data_path}/{f}" for f in os.listdir(data_path)]

        if files:
            df = pd.concat([pd.read_parquet(f) for f in files])

            with placeholder.container():

                col1, col2 = st.columns(2)

                col1.metric("Total Events", len(df))

                if "source" in df.columns:
                    col2.metric("Unique Sources", df["source"].nunique())

                st.subheader("Latest Events")
                st.dataframe(df.tail(20))

                if "source" in df.columns:
                    st.subheader("Events by Source")
                    chart = df["source"].value_counts()
                    st.bar_chart(chart)

        else:
            st.write("Waiting for streaming data...")

    except Exception as e:
        st.write("Waiting for Spark output...")

    time.sleep(5)