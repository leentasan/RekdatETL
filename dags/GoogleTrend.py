from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import json

# Fungsi untuk mengambil data dari API Google Trends
def fetch_google_trends():
    url = "https://serpapi.com/search.json?engine=google_trends&q=indomie&geo=ID-YO&date=now+7-d&data_type=TIMESERIES&api_key=3a02df93533b7231d221de3ca294baee492f14268dfa2fe53e3c50140c45d071"
    response = requests.get(url)

    if response.status_code == 200:
        # Simpan data ke file JSON sementara
        with open('/tmp/google_trends_data.json', 'w') as file:
            file.write(response.text)
        print("Data Google Trends berhasil diunduh.")
    else:
        raise Exception(f"Gagal mengunduh data: {response.status_code}")

def transform_data():
    # Load data dari file JSON menggunakan json.loads() untuk parsing yang aman
    with open('/tmp/google_trends_data.json', 'r') as file:
        data = json.load(file)  # Memuat JSON secara aman
        
        # Ekstrak data dari `interest_over_time > timeline_data`
        timeline_data = data.get("interest_over_time", {}).get("timeline_data", [])
        if not timeline_data:
            raise Exception("Tidak ada data `timeline_data` untuk ditransformasi.")
        
        # Proses data menjadi DataFrame
        records = []
        for entry in timeline_data:
            record = {
                "date": entry.get("date"),
                "value": entry.get("values", [{}])[0].get("value", 0)  # Ambil nilai popularity
            }
            records.append(record)
        
        # Buat DataFrame dari data yang diproses
        df_transformed = pd.DataFrame(records)
        
        # Hapus baris dengan nilai kosong atau NaN (jika ada)
        df_transformed.dropna(inplace=True)

        # Simpan data ke file CSV sementara
        df_transformed.to_csv('/tmp/google_trends_transformed.csv', index=False)
        print("Data berhasil ditransformasi dan disimpan ke /tmp/google_trends_transformed.csv")

# Fungsi untuk memuat data ke PostgreSQL
def load_to_postgresql():
    # Koneksi ke PostgreSQL
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

    # Load data dari file CSV sementara
    df_transformed = pd.read_csv('/tmp/google_trends_transformed.csv')

    # Fungsi untuk memetakan tipe data pandas ke tipe data SQL
    def map_dtype(dtype):
        if dtype == 'int64':
            return 'INTEGER'
        elif dtype == 'float64':
            return 'FLOAT'
        elif dtype == 'object':
            return 'TEXT'
        else:
            return 'TEXT'  # Default tipe data

    # Nama tabel di PostgreSQL
    table_name = 'google_trends'

    # Buat tabel baru dengan kolom berdasarkan DataFrame
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f"{col} {map_dtype(df_transformed[col].dtype)}" for col in df_transformed.columns])}
    );
    """

    # Eksekusi query pembuatan tabel
    with engine.connect() as connection:
        connection.execute(create_table_query)
        print(f"Tabel '{table_name}' berhasil dibuat atau sudah ada.")

    # Muat data ke tabel PostgreSQL
    df_transformed.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data berhasil dimuat ke PostgreSQL dalam tabel '{table_name}'.")


# Definisikan DAG
with DAG(
    dag_id='google_trends_etl',
    start_date=datetime(2024, 11, 20),
    schedule_interval='@daily',  # Menjalankan DAG setiap hari
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_google_trends',
        python_callable=fetch_google_trends,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql,
    )

    # Urutan eksekusi task
    fetch_task >> transform_task >> load_task
