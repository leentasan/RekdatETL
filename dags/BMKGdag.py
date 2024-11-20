from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

# Fungsi untuk mengambil data dari API BMKG
def fetch_bmkg_data():
    url = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=34"
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        # Simpan data ke file JSON
        with open('/tmp/bmkg_data.json', 'w') as file:
            file.write(response.text)
        print("Data BMKG berhasil diunduh.")
    else:
        print(f"Gagal mengunduh data BMKG: {response.status_code}")

# Definisikan DAG
with DAG(
    dag_id='bmkg_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',  # Menjalankan DAG setiap hari
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_bmkg_data',
        python_callable=fetch_bmkg_data,
    )