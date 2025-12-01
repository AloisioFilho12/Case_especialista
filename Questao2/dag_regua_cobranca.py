from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import sqlite3
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



def processar_arquivo():
    diretorio = '/path/do/diretorio/'  
    arquivo = 'pagamentos_d-1.csv'
    caminho_arquivo = os.path.join(diretorio, arquivo)

    try:
        # 1. Leitura do arquivo e visualização
        df = pd.read_csv(caminho_arquivo)
        
        print(f"Arquivo '{arquivo}' lido com sucesso.")
        print(f"Total de linhas lidas: {len(df)}")
        
        # ver arquivo
        print(df.head())
        
        return df
    
    except Exception as e:
        print(f"ERRO: Erro ao ler o CSV: {e}")
        return None

def carregar_para_sqlite(df: pd.DataFrame, nome_tabela: str, nome_banco: str):
    """
    Carrega o DataFrame para um banco de dados SQLite usando .to_sql().
    """
    if df is None or df.empty:
        print("❌ ERRO: DataFrame vazio ou inválido. Não há dados para carregar.")
        return

    conn = None
    try:
        # 1. Conexão com o banco de dados
        conn = sqlite3.connect(nome_banco)
        print(f"\nConexão com o SQLite estabelecida: {nome_banco}")

        # 2. Utiliza o método .to_sql() do Pandas (Mais eficiente que loops manuais)
        df.to_sql(
            name=nome_tabela,
            con=conn,
            if_exists='replace', # Substitui a tabela se ela já existir
            index=False          # Não salva o índice do DataFrame
        )
        
        conn.commit()
        
        # 3. Verificação Rápida
        total_inserido = pd.read_sql(f"SELECT COUNT(*) FROM {nome_tabela}", conn).iloc[0, 0]
        
        print(f"✅ Dados carregados com sucesso na tabela '{nome_tabela}'.")
        print(f"Total de linhas inseridas: {total_inserido}")
        
    except sqlite3.Error as e:
        print(f"❌ ERRO no SQLite ao carregar dados: {e}")
        
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

# ----------------- EXECUÇÃO PRINCIPAL CORRIGIDA -----------------

if __name__ == "__main__":
    
    # 1. Processa o arquivo CSV, carrega o DF e limpa as colunas
    dados_completos = processar_arquivo(diretorio, arquivo)

    # 2. CORREÇÃO DO ERRO: Verifica se o DataFrame não está vazio usando .empty
    if dados_completos is not None and not dados_completos.empty:
        # Carrega o DataFrame limpo para o SQLite
        carregar_para_sqlite(dados_completos, NOME_TABELA, NOME_BANCO_DADOS)
    else:
        print("\nProcessamento interrompido. Nenhum dado válido para carregar.")