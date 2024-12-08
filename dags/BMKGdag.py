from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import json
import os
import logging

# Fungsi untuk mengambil data dari API BMKG
def fetch_bmkg_data():
    url = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=34.71.03.1005"
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        # Simpan data ke file JSON
        with open('/tmp/bmkg_data.json', 'w') as file:
            file.write(response.text)
        print("Data BMKG berhasil diunduh.")
    else:
        print(f"Gagal mengunduh data BMKG: {response.status_code}")

def transform_bmkg_data():
    """
    Transformasikan data cuaca dari BMKG dengan hanya mempertahankan parameter tertentu.
    """
    input_path='/tmp/bmkg_data.json'
    output_path='/tmp/bmkg_data.csv'

    try:
        # Validasi keberadaan file input
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"File input tidak ditemukan: {input_path}")
        
        # Load data dari file JSON
        with open(input_path, 'r') as file:
            data1 = json.load(file)  # Membaca file JSON
        
        # Ekstrak data cuaca dari struktur JSON BMKG
        weather_data = data1.get("data", [])  # Ambil list utama dari key 'data'
        if not isinstance(weather_data, list) or not weather_data:
            raise Exception("Tidak ada data cuaca yang valid untuk ditransformasi.")

        # List untuk menyimpan data yang dipilih
        transformed_data = []

        for location_data in weather_data:
            for weather_entry in location_data.get('cuaca', []):
                for detail in weather_entry:
                    # Ekstrak parameter yang dibutuhkan
                    record = {
                        "local_datetime": detail.get("local_datetime"),
                        "t": detail.get("t"),
                        "weather_desc": detail.get("weather_desc"),
                        "analysis_date": detail.get("analysis_date")
                    }
                    transformed_data.append(record)
        
        # Buat DataFrame dari data yang dipilih
        df_transformed = pd.DataFrame(transformed_data)
        
        # Hapus baris dengan nilai kosong atau NaN
        df_transformed.dropna(inplace=True)
        
        # Simpan hasil transformasi ke file CSV
        df_transformed.to_csv(output_path, index=False)
        print(f"Data berhasil ditransformasi dan disimpan ke {output_path}")
    
    except Exception as e:
        print(f"Kesalahan saat transformasi: {e}")


def load_bmkg_data_to_postgresql():
    # Koneksi ke PostgreSQL
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

    # File CSV
    input_file = '/tmp/bmkg_data.csv'

    try:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"File {input_file} tidak ditemukan.")
        
        if os.stat(input_file).st_size == 0:
            raise ValueError(f"File {input_file} kosong.")
        
        df_transformed = pd.read_csv(input_file)
        
        if df_transformed.empty:
            raise ValueError("DataFrame yang dimuat kosong.")
        
        def map_dtype(dtype):
            """
            Memetakan tipe data pandas ke tipe data SQL sesuai skenario data.
            """
            if dtype == 'int64':
                return 'INTEGER'
            elif dtype == 'float64':
                return 'FLOAT'
            elif dtype == 'datetime64[ns]':
                return 'DATETIME'
            elif dtype == 'object':
                return 'TEXT'
            else:
                return 'TEXT'  # Default tipe data
        
        # Rutin PostgreSQL seperti sebelumnya...
        table_name = 'bmkg_weather'
        # Map dtype and create table query
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f"{col} {map_dtype(df_transformed[col].dtype)}" for col in df_transformed.columns])}
        );
        """
        
        with engine.connect() as connection:
            connection.execute(create_table_query)
        
        df_transformed.to_sql(table_name, engine, if_exists='replace', index=False)
    
    except FileNotFoundError as e:
        print(e)
    except pd.errors.EmptyDataError:
        print(f"File {input_file} kosong atau tidak memiliki data yang valid.")
    except Exception as e:
        print(f"Kesalahan saat memuat data ke PostgreSQL: {e}")
        

# Definisikan DAG
with DAG(
    dag_id='bmkg_etl',
    start_date=datetime(2024, 12, 1),
    schedule_interval='@daily',  # Menjalankan DAG setiap hari
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_bmkg_data',
        python_callable=fetch_bmkg_data,
    )

    transform_task = PythonOperator(
        task_id='transform_bmkg_data',
        python_callable=transform_bmkg_data,
    )

    load_task = PythonOperator(
        task_id='load_bmkg_data_to_postgresql',
        python_callable=load_bmkg_data_to_postgresql,
    )

    # Urutan eksekusi task
    fetch_task >> transform_task >> load_task