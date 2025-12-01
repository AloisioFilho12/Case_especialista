import pandas as pd
import numpy as np
import requests
import json
from pprint import pprint
import sqlite3


# --- Seu Código de Busca da API (Mantido para Contexto) ---

def buscar_usuarios(url="https://dummyjson.com/users?limit=300"):
    """
    Consome a API de usuários do dummyjson.com e exibe os dados.
    """
    print(f"Buscando dados em: {url}\n")
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            print("Conexão com a API estabelecida (Status 200).")
            dados_usuarios = response.json()
            
            total_usuarios = dados_usuarios.get('total', 0)
            print(f"Total de usuários encontrados: {total_usuarios}")
            print("-" * 30)
            
            usuarios = dados_usuarios.get('users', [])
            if usuarios:
                primeiro_usuario = usuarios[0]
                print("\nDetalhes do Primeiro Usuário:")
                pprint(primeiro_usuario)
            else:
                print("lista veio vazia.")
            
            return dados_usuarios

        else:
            print(f"Erro requisição. Status Code: {response.status_code}")
            print(f"Mensagem de erro: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão com URL: {e}")
        return None

# --- Nova Função: Inserir Dados no SQLite ---

def inserir_usuarios_no_sqlite(dados_completos, nome_banco="dados_usuarios.db"):
    """
    conexão SQLite e criando tabela de usuários
    """
    if not dados_completos or 'users' not in dados_completos:
        print("\n erro na inserção dos dados.")
        return

    usuarios = dados_completos['users']
    
    # 1. Conexão com o Banco de Dados
    print(f"\nConectando ao banco de dados SQLite: {nome_banco}")
    conn = None
    try:
        # Abre a conexão (se o arquivo não existir, ele será criado)
        conn = sqlite3.connect(nome_banco)
        cursor = conn.cursor()
        print("Conexão estabelecida.")

        # 2. Criação da Tabela
        # O SQLite usa o tipo 'INTEGER' para id e 'TEXT' para strings
        print("Criando ou redefinindo a tabela 'usuarios'...")
        cursor.execute("DROP TABLE IF EXISTS usuarios") # Opcional: Limpa dados anteriores
        cursor.execute("""
            CREATE TABLE usuarios (
                id INTEGER PRIMARY KEY,
                primeiro_nome TEXT,
                sobrenome TEXT,
                idade INTEGER,
                email TEXT,
                username TEXT,
                cidade TEXT,
                estado TEXT,
                UF TEXT,
                pais TEXT,
                universidade TEXT,
                empresa TEXT,
                setor TEXT, 
                cargo TEXT,
                estado_empresa TEXT,
                cartao TEXT,
                moeda TEXT,
                criptomoeda TEXT,
                rede TEXT 
            )
        """)
        print("Tabela 'usuarios' criada com sucesso.")

        # 3. Inserção dos Dados
        print(f"Inserindo {len(usuarios)} usuários na tabela...")
        contador = 0
        for usuario in usuarios:
            # Extrai os campos desejados
            user_data = (
                usuario.get('id'),
                usuario.get('firstName'),
                usuario.get('lastName'),
                usuario.get('age'),
                usuario.get('email'),
                usuario.get('username'),
                usuario.get('address', {}).get('city'),
                usuario.get('address', {}).get('state'),
                usuario.get('address', {}).get('stateCode'),
                usuario.get('address', {}).get('country'),
                usuario.get('university'),
                usuario.get('company', {}).get('name'),
                usuario.get('company', {}).get('department'),
                usuario.get('company', {}).get('title'),
                usuario.get('company', {}).get('address', {}).get('state'),
                usuario.get('bank', {}).get('cardType'),
                usuario.get('bank', {}).get('currency'),
                usuario.get('crypto', {}).get('coin'),
                usuario.get('crypto', {}).get('network')
            )
            
            # Comando SQL para inserção (usando ? para evitar injeção de SQL)
            cursor.execute("""
                INSERT INTO usuarios 
                (id, primeiro_nome, sobrenome, idade, email, username, cidade, estado, UF, pais, universidade, empresa, setor, cargo, estado_empresa, cartao, moeda, criptomoeda, rede)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, user_data)
            contador += 1
            
        # 4. Confirmação (Commit) e Fechamento
        conn.commit()
        print(f"✅ Inserção concluída! Total de {contador} registros inseridos.")
        
        # Opcional: Exibir alguns dados inseridos para verificação
        print("\nVerificação (Primeiros 3 Registros Inseridos):")
        cursor.execute("SELECT * FROM usuarios LIMIT 3")
        for linha in cursor.fetchall():
            print(linha)
            
    except sqlite3.Error as e:
        print(f"❌ Erro no SQLite: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

# --- Execução Principal Atualizada ---

if __name__ == "__main__":
   
    dados_completos = buscar_usuarios()

    if dados_completos:
        inserir_usuarios_no_sqlite(dados_completos)