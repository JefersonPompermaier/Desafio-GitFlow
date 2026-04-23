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
        df = pd.read_csv(arquivo)
        df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
        print(f"Tabela {nome_tabela} carregada.")

    conn.close()
    print("Processo concluído.")

if __name__ == "__main__":
    executar_ingestao()