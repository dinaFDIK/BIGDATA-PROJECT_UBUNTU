import streamlit as st
import pandas as pd
import os
import time

# Pengaturan Judul Dashboard [cite: 178]
st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("🛡️ Real-Time Fraud Detection Dashboard")

# Path ke folder hasil pemrosesan Spark [cite: 173, 179]
data_path = "stream_data/realtime_output/"

placeholder = st.empty()

while True:
    with placeholder.container():
        if os.path.exists(data_path) and len(os.listdir(data_path)) > 0:
            try:
                # Membaca data Parquet [cite: 179]
                df = pd.read_parquet(data_path)
                
                # Menampilkan Metrik Utama [cite: 180, 181]
                col1, col2 = st.columns(2)
                col1.metric("Total Transaksi", len(df))
                col2.metric("Total Fraud Terdeteksi", len(df[df["status"]=="FRAUD"]))

                # Tabel Data Terbaru (10 transaksi terakhir) [cite: 182]
                st.subheader("📋 Transaksi Terbaru")
                st.dataframe(df.tail(10))

                # Grafik Batang Fraud vs Normal [cite: 183]
                st.subheader("📊 Statistik Status")
                st.bar_chart(df["status"].value_counts())
                
            except Exception as e:
                st.info("Sedang memproses data terbaru...")
        else:
            st.warning("Menunggu data dari Spark... Pastikan Terminal 4 sudah jalan!")
            
    time.sleep(3) # Refresh otomatis setiap 3 detik
