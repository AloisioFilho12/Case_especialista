from datetime import datetime, timedelta
import os
import pandas as pd
import sqlite3
# Módulos específicos do Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException # Exceção correta para falha
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain

# --- Configurações Globais ---
# Definir caminho correto
DIRETORIO_BASE = '' 
ARQUIVO_NOME = 'pagamentos_d-1.csv'
NOME_BANCO_DADOS = 'dados_cobranca.db'
NOME_TABELA = 'tb_pagamentos_cobranca' 

# --- FUNÇÕES DE PRÉ-PROCESSAMENTO ---

def limpar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """ Padronizar nomes das colunas. """
    # Remove espaços extras e padroniza a caixa.
    df.columns = df.columns.str.strip().str.lower()
    return df

# --- FUNÇÕES DAS TASKS ---

def verificar_arquivo_fn(diretorio, arquivo, **context):
    """ Verifica a chegada do arquivo e falha a tarefa se ele não for encontrado. """
    caminho_arquivo = os.path.join(diretorio, arquivo)
    
    # Verifica se o arquivo está no diretório
    if os.path.exists(caminho_arquivo):
        print(f"✅ Arquivo {arquivo} encontrado em: {caminho_arquivo}")
        # Retorna o caminho do arquivo (XCom)
        return caminho_arquivo 
    else:
        # Usa AirflowFailException para sinalizar falha e acionar o retry
        raise AirflowFailException(f"Arquivo {arquivo} não encontrado no diretório: {diretorio}")

def processar_arquivo_fn(caminho_arquivo: str):
    """ Lê, limpa colunas e retorna o DataFrame. O Airflow envia o DF via XCom. """
    
    # O argumento 'caminho_arquivo' é injetado pelo Airflow via XCom (retorno da task anterior)
    if not caminho_arquivo:
        raise AirflowFailException("Caminho do arquivo não fornecido via XCom.")
        
    try:
        # 1. Leitura do arquivo
        df = pd.read_csv(caminho_arquivo)
        
        # 2. Limpeza e padronização das colunas
        df = limpar_nomes_colunas(df)
        
        print(f"✅ Arquivo lido com sucesso. Total de linhas: {len(df)}")
        print("\nPrimeiras linhas do DataFrame processado:")
        print(df.head())
        
        # O DataFrame é retornado e armazenado no XCom
        return df
    
    except Exception as e:
        raise AirflowFailException(f" ERRO ao processar o CSV: {e}")

def carregar_para_banco_fn(nome_tabela: str, nome_banco: str, **context):
    """ Puxa o DataFrame do XCom e o carrega para o SQLite. """
    
    # Recebe o DataFrame via XCom
    df = context['ti'].xcom_pull(task_ids='processar_arquivo_task')
    
    if df is None or df.empty:
        print("ERRO: DataFrame vazio ou inválido. Não há dados para carregar.")
        return

    conn = None
    try:
        # 1. Conexão com o banco de dados
        conn = sqlite3.connect(nome_banco)
        print(f"\nConexão com o SQLite estabelecida: {nome_banco}")

        # 2. Utiliza o método .to_sql() do Pandas para carga eficiente
        df.to_sql(
            name=nome_tabela,
            con=conn,
            if_exists='replace',
            index=False         
        )
        
        conn.commit()
        
        # 3. Verificação Rápida
        total_inserido = pd.read_sql(f"SELECT COUNT(*) FROM {nome_tabela}", conn).iloc[0, 0]
        
        print(f"Carga no BD concluída! Total de linhas inseridas: {total_inserido}")
        
    except sqlite3.Error as e:
        raise AirflowFailException(f" ERRO no SQLite ao carregar dados: {e}")
        
    finally:
        if conn:
            conn.close()

def arquivar_dados_fn(diretorio_base: str, arquivo_nome: str, **context):
    """ Puxa o DataFrame do XCom e o salva em uma pasta de log. """
    
    # Puxa o DataFrame que foi retornado pela task 'processar_arquivo_task'
    df = context['ti'].xcom_pull(task_ids='processar_arquivo_task')

    if df is None or df.empty:
        print(" Arquivamento ignorado: DataFrame vazio.")
        return
        
    diretorio_logs = os.path.join(diretorio_base, 'logs_cobranca')
    if not os.path.exists(diretorio_logs):
        os.makedirs(diretorio_logs)

    # Gera nome de arquivo com timestamp único
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_log = os.path.join(diretorio_logs, f'{arquivo_nome.replace(".csv", "")}_{timestamp}.csv')
    
    # Salva o DataFrame no formato CSV para arquivamento
    df.to_csv(arquivo_log, index=False)
    
    print(f"Dados arquivados com sucesso em: {arquivo_log}")

def escolher_caminho(**context):
    """ Condicional para determinar o caminho (BD ou Arquivamento) baseado no dia da semana. """
    # Verifica se o dia da semana é entre segunda (0) e sexta (4)
    dia_semana = datetime.now().weekday()
    if dia_semana in [0, 1, 2, 3, 4]:  # Segunda a Sexta
        return 'enviar_para_bd_task' # Nome da Task ID para seguir
    else:  # Sábado (5) ou Domingo (6)
        return 'arquivar_dados_task' # Nome da Task ID para seguir

# --- DEFINIÇÃO DA DAG ---

default_args = {
    'owner': 'Time de Cobrança',
    'depends_on_past': False,
    'email': ['suporte@empresa.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,  # Configura o retry de 3 tentativas em caso de falha
    'retry_delay': timedelta(minutes=5),
}