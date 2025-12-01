import requests
import json
import sqlite3
from tqdm import tqdm 

# --- Configuração do Banco de Dados ---
DB_NAME = "dados_usuarios.db"

# --------------------------------------------------
# 1. FUNÇÃO PARA BUSCAR ESTADOS DOS EUA (Primeiro Passo)
# --------------------------------------------------

def buscar_estados_eua():
    """Busca a lista base de estados dos EUA."""
    API_URL = "https://countriesnow.space/api/v0.1/countries/states"
    PAYLOAD = {"country": "United States"}
    print(f"\n🌍 Buscando lista de estados em: {API_URL}")
    
    try:
        response = requests.post(API_URL, json=PAYLOAD)
        response.raise_for_status() 
        dados_api = response.json()
        
        if dados_api.get('error') == False and 'data' in dados_api:
            estados_data = dados_api['data'].get('states', [])
            print(f"✅ Sucesso! {len(estados_data)} estados/regiões encontrados.")
            return estados_data
        else:
            print(f"❌ Erro na resposta da API de estados.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de conexão ao buscar estados: {e}")
        return None

# --------------------------------------------------
# 2. FUNÇÃO PARA BUSCAR E FORMATAR CIDADES (SEGUNDO PASSO)
# --------------------------------------------------

def buscar_e_formatar_localidades(dados_estados):
    """
    Itera sobre os estados, busca as cidades para cada um e 
    retorna uma lista única de (cidade, nome_estado, codigo_estado, pais).
    """
    if not dados_estados:
        print("A lista de estados está vazia. Impossível buscar cidades.")
        return []

    API_URL_CIDADES = "https://countriesnow.space/api/v0.1/countries/state/cities"
    pais = "United States"
    localidades_completas = []
    
    print(f"\n🏙️ Iniciando busca de cidades e formatação dos dados para {len(dados_estados)} estados...")
    
    # tqdm mostra o progresso no console durante as múltiplas chamadas API
    for estado in tqdm(dados_estados, desc="Processando Estados"):
        nome_estado = estado.get('name')
        codigo_estado = estado.get('state_code')
        
        if not nome_estado:
            continue

        payload_cidade = {
            "country": pais,
            "state": nome_estado
        }
        
        try:
            response = requests.post(API_URL_CIDADES, json=payload_cidade)
            response.raise_for_status()
            dados_cidade_api = response.json()
            
            if dados_cidade_api.get('error') == False and 'data' in dados_cidade_api:
                lista_cidades = dados_cidade_api.get('data', [])
                
                # Formata os dados no formato final para a tabela única
                for cidade in lista_cidades:
                    localidades_completas.append((
                        cidade,             # nome_cidade
                        nome_estado,        # nome_estado
                        codigo_estado,      # codigo_estado (UF)
                        pais                # pais
                    ))

        except requests.exceptions.RequestException as e:
            # Em caso de erro, continua para o próximo estado
            continue
            
    print(f"✅ Busca e formatação concluída. Total de {len(localidades_completas)} cidades/registros coletados.")
    return localidades_completas

# --------------------------------------------------
# 3. FUNÇÃO PARA INSERIR LOCALIDADES NO SQLITE (UNIFICADA)
# --------------------------------------------------

def inserir_localidades_no_sqlite(dados_localidades):
    """Cria a tabela 'localidades_eua' e insere todos os dados de cidade e estado."""
    if not dados_localidades:
        print("\nErro: Dados de localidades vazios, não há o que inserir.")
        return

    print(f"\nConectando ao banco de dados SQLite: {DB_NAME}")
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        print("Conexão estabelecida.")

        # Criação da Tabela 'localidades_eua'
        print("Criando ou redefinindo a tabela 'localidades_eua'...")
        cursor.execute("DROP TABLE IF EXISTS localidades_eua") 
        cursor.execute("""
            CREATE TABLE localidades_eua (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_cidade TEXT NOT NULL,
                nome_estado TEXT NOT NULL,
                codigo_estado TEXT NOT NULL,
                pais TEXT NOT NULL,
                UNIQUE(nome_cidade, codigo_estado) 
                -- Garante que não haja cidades repetidas no mesmo estado
            )
        """)
        print("Tabela 'localidades_eua' criada com sucesso.")
        

        # Comando SQL para inserção
        sql_insert = "INSERT INTO localidades_eua (nome_cidade, nome_estado, codigo_estado, pais) VALUES (?, ?, ?, ?)"

        # Inserção eficiente de múltiplos registros
        cursor.executemany(sql_insert, dados_localidades)
        
        conn.commit()
        print(f"✅ Inserção concluída! Total de {cursor.rowcount} localidades inseridas.")
        
        # Verificação
        print("\nVerificação (Primeiros 5 Registros Inseridos):")
        cursor.execute("SELECT * FROM localidades_eua LIMIT 5")
        for linha in cursor.fetchall():
            print(linha)
            
    except sqlite3.Error as e:
        print(f"❌ Erro no SQLite: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

# --------------------------------------------------
# 4. EXECUÇÃO PRINCIPAL
# --------------------------------------------------

if __name__ == "__main__":
    
    print("=" * 60)
    print("INÍCIO DO PROCESSAMENTO DE DADOS GEOGRÁFICOS UNIFICADOS DOS EUA")
    print("=" * 60)
    
    # 1. BUSCA ESTADOS (obtém a lista de estados para iterar)
    dados_estados = buscar_estados_eua()
    
    if dados_estados:
        # 2. BUSCA CIDADES E FORMATA OS DADOS (múltiplas chamadas API)
        dados_localidades = buscar_e_formatar_localidades(dados_estados)
        
        # 3. INSERE TODOS OS DADOS NA TABELA ÚNICA
        if dados_localidades:
            inserir_localidades_no_sqlite(dados_localidades)
    
    print("\n" + "=" * 60)
    print("PROCESSAMENTO CONCLUÍDO.")
    print("=" * 60)