"""Streamlit dashboard for IceStream's local MVP."""
from pathlib import Path
import pandas as pd
import streamlit as st

VALID_FILE = Path("data/valid_transactions.parquet")
BAD_FILE = Path("data/quarantine.parquet")

st.set_page_config(page_title="IceStream Observability", page_icon="🌊", layout="wide")
st.title("🌊 IceStream – Real-Time Lakehouse Observability")
st.caption("Local MVP: Redpanda/Kafka-compatible streaming + Python quality rules + Parquet + Streamlit")

if st.button("Refresh dashboard"):
    st.rerun()

def read_file(path):
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            st.warning(f"Could not read {path}: {exc}")
    return pd.DataFrame()

valid_df = read_file(VALID_FILE)
bad_df = read_file(BAD_FILE)
valid_count = len(valid_df)
bad_count = len(bad_df)
total = valid_count + bad_count
error_rate = (bad_count / total * 100) if total else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total processed", total)
c2.metric("Valid records", valid_count)
c3.metric("Quarantined records", bad_count)
c4.metric("Bad-data rate", f"{error_rate:.2f}%")

if total == 0:
    st.info("No events yet. Start docker compose, consumer.py, and producer.py in separate terminals.")
elif error_rate > 2:
    st.error(f"Alert: bad-data rate is {error_rate:.2f}% (above the 2% target threshold).")
else:
    st.success(f"Bad-data rate is {error_rate:.2f}% (at or below the 2% target threshold).")

left, right = st.columns(2)
with left:
    st.subheader("Valid transactions")
    st.dataframe(valid_df.tail(20), use_container_width=True)
with right:
    st.subheader("Quarantined transactions")
    st.dataframe(bad_df.tail(20), use_container_width=True)

st.subheader("Quality summary")
if bad_count:
    if "quality_reason" in bad_df.columns:
        summary = bad_df["quality_reason"].value_counts().rename_axis("Issue").reset_index(name="Count")
        st.dataframe(summary, use_container_width=True)
    else:
        st.write("Quarantined records are available, but no reason column was found.")
else:
    st.write("No quarantined records yet.")
st.caption("Note: This dashboard refreshes when you click Refresh. It alerts visually; it does not pause a Flink pipeline.")
