import pandas as pd
import requests
import json
from pprint import pprint

def buscar_usuarios(url="https://dummyjson.com/users"):
    """
    Consome a API de usuários do dummyjson.com e exibe os dados.
    """
    print(f"Buscando dados em: {url}\n")
    
    try:
        # 1. Faz a requisição HTTP GET
        response = requests.get(url)
        
        # 2. Verifica se a requisição foi bem-sucedida (Status Code 200)
        if response.status_code == 200:
            print("✅ Sucesso! Conexão com a API estabelecida (Status 200).")
            
            # 3. Converte a resposta JSON em um dicionário Python
            dados_usuarios = response.json()
            
            # Exibe um resumo dos dados
            total_usuarios = dados_usuarios.get('total', 0)
            print(f"Total de usuários encontrados: {total_usuarios}")
            print("-" * 30)
            
            # 4. Exibe os detalhes do primeiro usuário
            usuarios = dados_usuarios.get('users', [])
            if usuarios:
                primeiro_usuario = usuarios[0]
                print("\nDetalhes do Primeiro Usuário:")
                # pprint é útil para imprimir dicionários grandes de forma legível
                pprint(primeiro_usuario)
            else:
                print("⚠️ A lista de usuários veio vazia.")
            
            return dados_usuarios

        else:
            # Em caso de erro (4xx, 5xx)
            print(f"❌ Erro na requisição. Status Code: {response.status_code}")
            print(f"Mensagem de erro: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        # Em caso de problemas de rede, DNS, etc.
        print(f"❌ Erro de conexão ao tentar acessar a URL: {e}")
        return None

if __name__ == "__main__":
    # Executa a função
    dados_completos = buscar_usuarios()

    # Você pode usar 'dados_completos' para manipulações futuras (ex: salvar em CSV, banco de dados, etc.)
    # Ex: print(f"\nDados salvos (tipo): {type(dados_completos)}")
