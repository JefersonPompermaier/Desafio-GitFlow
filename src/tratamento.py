import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

ROOT_DIR = Path(__file__).parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "delivery_database.db"

# correção de encoding nos dados originais

def limpeza_caracteres(texto):
    if not isinstance(texto, str):
        return texto
    mapa = {
        "Ã§Ã£": "çã",
        "Ã´": "ô",
        "Ã¡": "á",
        "Ã©": "é",
        "Ã": "á",
    }
    for erro, correcao in mapa.items():
        texto = texto.replace(erro, correcao)
    return texto

# padronização de datas, tentando múltiplos formatos
def _parse_datas(series):
    datas = pd.to_datetime(series, format="%m/%d/%Y", errors="coerce")
    faltantes = datas.isna()
    if faltantes.any():
        datas.loc[faltantes] = pd.to_datetime(series[faltantes], errors="coerce")
    return datas

# validação de registros órfãos
def _validar_orfaos(conn):
    print("\n=== VALIDAÇÃO DE ÓRFÃOS ===")
    
    # Produtos em itens_pedido que não existem em produtos
    cur = conn.cursor()
    cur.execute("""
    SELECT COUNT(DISTINCT i.id_produto) 
    FROM itens_pedido i 
    LEFT JOIN produtos p ON i.id_produto = p.id_produto
    WHERE p.id_produto IS NULL
    """)
    orfaos_produtos = cur.fetchone()[0]
    print(f"Produtos órfãos (em itens mas não em produtos): {orfaos_produtos}")

    # Pedidos em itens_pedido que não existem em pedidos
    cur.execute("""
    SELECT COUNT(DISTINCT i.id_pedido) 
    FROM itens_pedido i 
    LEFT JOIN pedidos p ON i.id_pedido = p.id_pedido
    WHERE p.id_pedido IS NULL
    """)
    orfaos_pedidos = cur.fetchone()[0]
    print(f"Pedidos órfãos (em itens mas não em pedidos): {orfaos_pedidos}")

    # Clientes em pedidos que não existem em clientes
    cur.execute("""
    SELECT COUNT(DISTINCT p.id_cliente) 
    FROM pedidos p 
    LEFT JOIN clientes c ON p.id_cliente = c.id_cliente
    WHERE c.id_cliente IS NULL
    """)
    orfaos_clientes = cur.fetchone()[0]
    print(f"Clientes órfãos (em pedidos mas não em clientes): {orfaos_clientes}")

# inserção de clientes desconhecidos para pedidos órfãos
def _inserir_clientes_desconhecidos(conn):
    print("\nInserindo clientes desconhecidos...")
    
    cur = conn.cursor()
    cur.execute("""
    SELECT DISTINCT p.id_cliente 
    FROM pedidos p 
    LEFT JOIN clientes c ON p.id_cliente = c.id_cliente
    WHERE c.id_cliente IS NULL
    """)
    orfaos = [row[0] for row in cur.fetchall()]

    if orfaos:
        df_novos = pd.DataFrame({
            "id_cliente": orfaos,
            "nome": ["Desconhecido"] * len(orfaos),
            "estado": ["DC"] * len(orfaos),
            "data_cadastro": [datetime.now().strftime("%m/%d/%Y")] * len(orfaos),
        })
        df_novos.to_sql("clientes", conn, if_exists="append", index=False)
        print(f"  {len(orfaos)} cliente(s) desconhecido(s) inserido(s)")
    else:
        print("  Sem clientes desconhecidos para inserir")

# conversão de tipos de dados para numeric
def _converter_tipos_numericos(conn):
    print("\nConvertendo tipos de dados para numeric...")

    # Produtos: preco
    try:
        df_prod = pd.read_sql("SELECT * FROM produtos", conn)
        if "preco" in df_prod.columns:
            df_prod["preco"] = pd.to_numeric(df_prod["preco"], errors="coerce")
        df_prod.to_sql("produtos", conn, if_exists="replace", index=False)
        print("  produtos.preco convertido")
    except Exception as e:
        print(f"  Erro ao converter produtos.preco: {e}")

    # Pedidos: valor_total
    try:
        df_ped = pd.read_sql("SELECT * FROM pedidos", conn)
        if "valor_total" in df_ped.columns:
            df_ped["valor_total"] = pd.to_numeric(df_ped["valor_total"], errors="coerce")
        df_ped.to_sql("pedidos", conn, if_exists="replace", index=False)
        print("  pedidos.valor_total convertido")
    except Exception as e:
        print(f"  Erro ao converter pedidos.valor_total: {e}")

    # Itens_pedido: preco_unitario
    try:
        df_itens = pd.read_sql("SELECT * FROM itens_pedido", conn)
        if "preco_unitario" in df_itens.columns:
            df_itens["preco_unitario"] = pd.to_numeric(
                df_itens["preco_unitario"], errors="coerce"
            )
        df_itens.to_sql("itens_pedido", conn, if_exists="replace", index=False)
        print("  itens_pedido.preco_unitario convertido")
    except Exception as e:
        print(f"  Erro ao converter itens_pedido.preco_unitario: {e}")

# função principal de processamento dos dados silver
def processar_silver():
    print("Tratamento iniciado")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco nao encontrado em: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        # Validação de órfãos
        _validar_orfaos(conn)

        # Inserção de clientes desconhecidos
        _inserir_clientes_desconhecidos(conn)
        conn.commit()

        # Conversão de tipos numéricos
        _converter_tipos_numericos(conn)
        conn.commit()

        # Limpeza de caracteres em produtos e categorias
        print("\nLimpando tabela de produtos...")
        df_prod = pd.read_sql("SELECT * FROM produtos", conn)
        if "nome_produto" in df_prod.columns:
            df_prod["nome_produto"] = df_prod["nome_produto"].apply(limpeza_caracteres)
        if "categoria" in df_prod.columns:
            df_prod["categoria"] = df_prod["categoria"].apply(limpeza_caracteres)
        df_prod.to_sql("produtos_silver", conn, if_exists="replace", index=False)

        # Padronização de datas e ajuste de data_cadastro com base no primeiro pedido
        print("Padronizando datas de pedidos...")
        df_ped = pd.read_sql("SELECT * FROM pedidos", conn)
        if "data_pedido" in df_ped.columns:
            df_ped["data_pedido"] = _parse_datas(df_ped["data_pedido"])
        df_ped.to_sql("pedidos_silver", conn, if_exists="replace", index=False)

        print("Corrigindo datas de cadastro de clientes...")
        df_cli = pd.read_sql("SELECT * FROM clientes", conn)
        if "data_cadastro" in df_cli.columns:
            df_cli["data_cadastro"] = _parse_datas(df_cli["data_cadastro"])

        if {"id_cliente", "data_pedido"}.issubset(df_ped.columns) and {
            "id_cliente",
            "data_cadastro",
        }.issubset(df_cli.columns):
            primeiro_pedido = (
                df_ped.dropna(subset=["data_pedido"])
                .groupby("id_cliente", as_index=False)["data_pedido"]
                .min()
                .rename(columns={"data_pedido": "primeiro_pedido"})
            )

            df_cli = df_cli.merge(primeiro_pedido, on="id_cliente", how="left")
            mascara_ajuste = df_cli["data_cadastro"].isna() | (
                df_cli["primeiro_pedido"].notna()
                & (df_cli["data_cadastro"] > df_cli["primeiro_pedido"])
            )
            df_cli.loc[mascara_ajuste, "data_cadastro"] = df_cli.loc[
                mascara_ajuste, "primeiro_pedido"
            ]
            df_cli = df_cli.drop(columns=["primeiro_pedido"])

        df_cli.to_sql("clientes_silver", conn, if_exists="replace", index=False)

        # Cópia de itens_pedido para silver
        print("Copiando tabela de itens_pedido...")
        df_itens = pd.read_sql("SELECT * FROM itens_pedido", conn)
        df_itens.to_sql("itens_pedido_silver", conn, if_exists="replace", index=False)

    print("\nProcesso de tratamento concluido.")

if __name__ == "__main__":
    processar_silver()