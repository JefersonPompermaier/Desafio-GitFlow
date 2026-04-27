import pandas as pd
import sqlite3
from pathlib import Path

# Localiza a pasta raiz do projeto (uma pasta acima de onde este script está)
ROOT_DIR = Path(__file__).parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "delivery_database.db"

def executar_ingestao():
    # Garante que a pasta processed existe
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Conecta ao banco
    conn = sqlite3.connect(DB_PATH)
    
    # Busca os arquivos
    arquivos = list(RAW_DIR.glob("*.csv"))
    
    if not arquivos:
        print(f"Nenhum arquivo encontrado em: {RAW_DIR}")
        return

    for arquivo in arquivos:
        nome_tabela = arquivo.stem.lower()
        try:
            df = pd.read_csv(arquivo)
            df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
            print(f"Tabela {nome_tabela} carregada com sucesso.")
        except Exception as e:
            print(f"⚠️ Erro ao carregar o ficheiro {arquivo.name}: {e}")
            print(f"A saltar {nome_tabela} e a continuar para o próximo ficheiro...")
            continue # Faz o loop saltar para o próximo ficheiro sem quebrar o programa

    conn.close()
    print("Processo concluído.")

if __name__ == "__main__":
    executar_ingestao()