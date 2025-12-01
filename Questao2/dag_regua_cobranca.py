from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

# verificar a chegada do arquivo 

def verificar_arquivo(**context):
    diretorio = '/path/do/diretorio/'  # Substituir pelo caminho real
    arquivo = 'pagamentos_d-1.csv'
    caminho_arquivo = os.path.join(diretorio, arquivo)

    # Verifica se o arquivo está no diretório
    if os.path.exists(caminho_arquivo):
        context['ti'].xcom_push(key='arquivo_disponivel', value=True)
    else:
        raise AirflowFailException(f"Arquivo {arquivo} não encontrado!")